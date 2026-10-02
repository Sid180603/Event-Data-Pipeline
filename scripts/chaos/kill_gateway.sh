#!/usr/bin/env bash
# Beat 1 of 6: kill the gateway mid-flight and show that the receipt balances.
#
# WHAT IT PROVES
#   A 202 is acceptance into an in-memory buffer, NOT a durability receipt
#   (CONTRACT.md section 3). SIGKILLing the gateway therefore drops whatever was
#   un-acked, and the beat measures exactly how much:
#     * the driver's run does not exit clean, which is what proves the kill
#       landed mid-flight rather than after the run finished -- a kill that
#       arrived late would prove nothing and must not pass;
#     * the shortfall reconciles as an identity, not a tolerance:
#         missing == unaccepted + un-acked-window
#       where `unaccepted` is the driver's own count of events in batches that
#       ended without a 202, and the un-acked window is what is left over;
#     * that window is bounded by the buffer's capacity (PRODUCER_QUEUE_MAXSIZE,
#       read from app/config.py so the bound cannot drift from the code);
#     * reconciliation reports no total loss, no unexpected record, no
#       undecodable message, a drained read, and correct pseudonyms;
#     * after the restart, a fresh run reconciles CLEAN with
#       --expect-duplicates 0 --expect-dlq 0, i.e. exit 0.
#
# SIGKILL, not SIGTERM: SIGTERM would run the lifespan shutdown, the producer
# would drain, and there would be nothing left to measure.
#
# IT NEVER PRINTS "no data lost". A zero window is reported as zero FOR THIS RUN,
# with the bound beside it, because a claim of zero is exactly what the 202
# semantics forbid.
#
# WHAT IT NEEDS
#   The compose stack up, `.env` and driver-signing-key.pem at the repository
#   root. Everything runs inside the compose network.
#
# ENVIRONMENT
#   COMPOSE               docker compose command           (default: docker compose)
#   PYTHON                interpreter for the host-side checks
#   CHAOS_SESSIONS        sessions in the run that gets killed  (default: 6000)
#   CHAOS_RECOVERY_SESSIONS  sessions in the post-restart run    (default: 2000)
#   CHAOS_KILL_AFTER      seconds before the kill          (default: 3)
#   CHAOS_LEDGER          ground truth, killed run        (default: chaos-kill-gateway.jsonl)
#   CHAOS_RECOVERY_LEDGER ground truth, recovery run       (default: chaos-kill-recovery.jsonl)
#   CHAOS_VERIFY_TIMEOUT  per-topic read budget, seconds   (default: 60)
#
# Exit status: 0 only when every assertion above held.

# -e exits on the first failed command, -u on an unset variable, and pipefail on
# a failure anywhere in a pipeline. A chaos beat that cannot fail is worse than
# no beat: the whole point is that a green run means something was checked.
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

COMPOSE="${COMPOSE:-docker compose}"
PYTHON="${PYTHON:-python}"
TOPIC_RAW="career.events.raw"
TOPIC_DLQ="career.events.dlq"
CHAOS_SESSIONS="${CHAOS_SESSIONS:-6000}"
CHAOS_RECOVERY_SESSIONS="${CHAOS_RECOVERY_SESSIONS:-2000}"
CHAOS_KILL_AFTER="${CHAOS_KILL_AFTER:-3}"
CHAOS_LEDGER="${CHAOS_LEDGER:-chaos-kill-gateway.jsonl}"
CHAOS_RECOVERY_LEDGER="${CHAOS_RECOVERY_LEDGER:-chaos-kill-recovery.jsonl}"
CHAOS_VERIFY_TIMEOUT="${CHAOS_VERIFY_TIMEOUT:-60}"
SESSIONS="${CHAOS_SESSIONS}"
RECOVERY_SESSIONS="${CHAOS_RECOVERY_SESSIONS}"
KILL_AFTER="${CHAOS_KILL_AFTER}"
IN_LEDGER="/app/${CHAOS_LEDGER}"
IN_RECOVERY_LEDGER="/app/${CHAOS_RECOVERY_LEDGER}"

WORK="$(mktemp -d)"
DRIVER_PID=""

usage() {
    cat <<'USAGE'
kill_gateway.sh -- SIGKILL the gateway mid-run and measure the loss window.

Proves:
  * the kill landed mid-flight (the driver's run does not exit clean);
  * missing == unaccepted + un-acked window, as an identity;
  * the un-acked window is bounded by the producer buffer's capacity, and the
    number is printed rather than described;
  * reconciliation reports no total loss, no unexpected record, no undecodable
    message and correct pseudonyms;
  * after the restart, a fresh run reconciles clean (tools.verify exit 0 with
    --expect-duplicates 0 --expect-dlq 0).

Never reports "no data lost": a 202 is acceptance into an in-memory buffer, so a
window is dropped by construction and this beat measures its size.

Needs: the compose stack up, .env and driver-signing-key.pem present. Runs
everything inside the compose network; nothing is published to the host.

Environment: COMPOSE, PYTHON, CHAOS_SESSIONS, CHAOS_RECOVERY_SESSIONS,
CHAOS_KILL_AFTER, CHAOS_LEDGER, CHAOS_RECOVERY_LEDGER, CHAOS_VERIFY_TIMEOUT.
USAGE
}

info() { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
fail() { printf '\nBEAT FAILED: %s\n' "$*" >&2; exit 1; }

# Read one field out of a JSON document on disk, and only that document: both
# the driver's summary and tools.verify's --json are printed with indent=2, so
# the top-level `{` is the only one at column 0 and every nested object is
# indented. `docker compose run` prefixes the container's output with whatever
# pip printed, which is why a naive json.load on the whole file is not an option.
json_field() {  # path-to-json-file dotted.path
    "$PYTHON" - "$1" "$2" <<'PY'
import json, sys
lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
starts = [i for i, line in enumerate(lines) if line == "{"]
if not starts:
    sys.exit("no JSON object found in " + sys.argv[1])
document = json.loads("\n".join(lines[starts[-1]:]))
for part in sys.argv[2].split("."):
    document = document[part]
if isinstance(document, (dict, list)):
    print(json.dumps(document, sort_keys=True))
else:
    print(document)
PY
}

# Evaluate an arithmetic assertion over the driver's own summary. Where a claim
# is about the receipt ("sent == accepted + rejected + unaccepted") this is the
# receipt's own arithmetic rather than a second script's, so it cannot disagree
# with the numbers on the screen by construction.
json_assert() {  # path-to-json-file python-expression-indexed-by-'r'
    "$PYTHON" - "$1" "$2" <<'PY'
import json, sys
lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
starts = [i for i, line in enumerate(lines) if line == "{"]
r = json.loads("\n".join(lines[starts[-1]:]))
if not eval(sys.argv[2]):
    sys.exit("receipt arithmetic failed: " + sys.argv[2] + " over " + json.dumps(r, sort_keys=True))
PY
}

run_driver() {  # extra driver.main arguments...
    local args=("$@")
    # `docker compose run` needs the service's command overridden to pass any
    # argument at all, and `exec` so the container's exit code is the driver's.
    $COMPOSE run --rm -T driver bash -c \
        "pip install --quiet --no-input -e . && exec $PYTHON -m driver.main ${args[*]}" \
        > "$WORK/driver.log" 2>&1
}

run_verify() {  # in-container-ledger out-json extra-verify-arguments...
    local ledger="$1" destination="$2"
    shift 2
    $COMPOSE exec -T gateway "$PYTHON" -m tools.verify \
        --ledger "$ledger" --json --timeout "$CHAOS_VERIFY_TIMEOUT" "$@" > "$destination" 2> "$WORK/verify.err"
}

verify_json() {  # out-json dotted.path
    json_field "$1" "$2"
}

reset_topics() {
    info "resetting $TOPIC_RAW and $TOPIC_DLQ: tools.verify reconciles the WHOLE"
    info "topic against ONE ledger, so each beat measures only its own run."
    local listed
    if ! $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:9092 --delete --topic "$TOPIC_RAW" --topic "$TOPIC_DLQ" \
        > "$WORK/delete.log" 2>&1; then
        info "  (delete refused or nothing to delete; the wait below decides)"
    fi
    local waited=0
    while :; do
        listed="$($COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --list 2>/dev/null || true)"
        if topics_absent "$listed"; then
            break
        fi
        waited=$((waited + 2))
        [ "$waited" -le 30 ] || fail "the broker still lists a topic 30s after the delete; refusing to reconcile against the previous beat's records"
        sleep 2
    done
    # Recreated through compose's own kafka-init, so the partition counts and
    # configs have one definition instead of a second copy in this script.
    $COMPOSE up -d --force-recreate kafka-init > "$WORK/kafka-init.log" 2>&1
    waited=0
    while :; do
        listed="$($COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --list 2>/dev/null || true)"
        if topics_present "$listed"; then
            return 0
        fi
        waited=$((waited + 2))
        [ "$waited" -le 60 ] || fail "kafka-init did not recreate both topics within 60s"
        sleep 2
    done
}

topics_present() {
    case "$1" in *"$TOPIC_RAW"*) ;; *) return 1 ;; esac
    case "$1" in *"$TOPIC_DLQ"*) ;; *) return 1 ;; esac
    return 0
}

topics_absent() {
    case "$1" in *"$TOPIC_RAW"*) return 1 ;; esac
    case "$1" in *"$TOPIC_DLQ"*) return 1 ;; esac
    return 0
}

wait_for_gateway() {
    local attempt
    for attempt in $(seq 1 60); do
        if $COMPOSE exec -T gateway "$PYTHON" -c \
            "import sys,urllib.request as u; sys.exit(0 if u.urlopen('http://localhost:8000/healthz', timeout=3).status == 200 else 1)" \
            > /dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    $COMPOSE logs --tail 40 gateway >&2 || true
    fail "the gateway did not answer /healthz within 120s"
}

start_gateway() {
    # One uvicorn worker, so a metrics scrape before and after this beat is
    # answered by the same process and the counters are comparable. The default
    # is four workers, and uvicorn hands a scrape to exactly one of them.
    GATEWAY_WORKERS=1 $COMPOSE up -d gateway > "$WORK/gateway-up.log" 2>&1 \
        || fail "'docker compose up -d gateway' failed; see $WORK/gateway-up.log"
    wait_for_gateway
    rm -f "$WORK/gateway-killed"
}

preflight() {
    command -v docker > /dev/null 2>&1 || fail "docker is not on PATH: this beat runs the compose stack, so it needs Docker (WSL2 + Docker Desktop on Windows)."
    docker compose version > /dev/null 2>&1 || fail "'docker compose' is not available. Install Docker Desktop, or set COMPOSE to a working compose command."
    [ -f .env ] || fail "no .env at the repository root: compose will not invent MASTER_SECRET and a JWT public key. Run the generator in the header of docker-compose.yml."
    [ -f driver-signing-key.pem ] || fail "no driver-signing-key.pem: that is the PRIVATE half of the signing pair, written by the same generator. The driver cannot mint a tenant token without it."
    [ -r driver-signing-key.pem ] || fail "driver-signing-key.pem is not readable: the driver runs as root inside the container but this script checks it on the host."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx kafka \
        || fail "the kafka service is not running: 'make up' first. This beat reconciles against the broker after the restart."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx gateway \
        || fail "the gateway service is not running: 'make up' first."
    wait_for_gateway
    # The bound the loss window is checked against, from the code that sets it.
    BUFFER_CAPACITY="$("$PYTHON" -c 'import app.config; print(app.config.PRODUCER_QUEUE_MAXSIZE)')"
    info "the producer buffer holds $BUFFER_CAPACITY records (app/config.py: PRODUCER_QUEUE_MAXSIZE);"
    info "that capacity is what makes the loss window bounded rather than open-ended."
}

cleanup() {
    if [ -n "$DRIVER_PID" ] && kill -0 "$DRIVER_PID" 2> /dev/null; then
        kill "$DRIVER_PID" 2> /dev/null || true
    fi
    # This beat SIGKILLs the gateway, so leaving it stopped would break the beats
    # after it. Best effort: if compose itself is gone there is nothing to bring
    # back, and the message says so rather than pretending.
    if [ -f "$WORK/gateway-killed" ] && command -v docker > /dev/null 2>&1; then
        if ! $COMPOSE exec -T gateway true > /dev/null 2>&1; then
            info "cleanup: bringing the gateway back up, because this script killed it"
            GATEWAY_WORKERS=1 $COMPOSE up -d gateway > /dev/null 2>&1 \
                || info "cleanup: 'docker compose up -d gateway' failed; run 'make up' before the next beat"
        fi
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

for argument in "$@"; do
    case "$argument" in
        -h | --help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument '$argument'. This script takes no arguments; run it with --help to see what it needs and what it proves."
            ;;
    esac
done

step "beat 1: SIGKILL the gateway mid-flight and measure the loss window"
preflight
reset_topics

step "load run ($SESSIONS sessions), SIGKILL at +${KILL_AFTER}s"
run_driver --sessions "$SESSIONS" --ledger "$IN_LEDGER" &
DRIVER_PID=$!
sleep "$KILL_AFTER"
info "docker compose kill -s KILL gateway"
$COMPOSE kill -s KILL gateway
touch "$WORK/gateway-killed"
DRIVER_STATUS=0
wait "$DRIVER_PID" || DRIVER_STATUS=$?
DRIVER_PID=""
sed -n '/^{/,$p' "$WORK/driver.log"
[ "$DRIVER_STATUS" -ne 0 ] \
    || fail "the driver run exited clean, so the SIGKILL landed after the run had finished: nothing was in flight, nothing was measured, and this beat proves nothing. Raise CHAOS_SESSIONS or lower CHAOS_KILL_AFTER."
info "the run exited $DRIVER_STATUS, which is how we know the kill landed mid-flight."

SENT="$(json_field "$WORK/driver.log" sent)"
ACCEPTED="$(json_field "$WORK/driver.log" accepted)"
REJECTED="$(json_field "$WORK/driver.log" rejected)"
UNACCEPTED="$(json_field "$WORK/driver.log" unaccepted)"
RETRIED="$(json_field "$WORK/driver.log" retried)"
BY_STATUS="$(json_field "$WORK/driver.log" by_status)"
json_assert "$WORK/driver.log" 'r["sent"] == r["accepted"] + r["rejected"] + r["unaccepted"]' \
    || fail "the receipt does not balance: sent is not accepted + rejected + unaccepted, so there is nothing to reconcile against."
info ""
info "the receipt: sent $SENT, accepted $ACCEPTED, rejected $REJECTED, unaccepted $UNACCEPTED"
info "refusals by the gateway's own reason code: $BY_STATUS"
info "retried batches: $RETRIED"

step "bringing the gateway back up"
start_gateway

step "reconciling the killed run: the shortfall is an identity, not a tolerance"
KILLED_JSON="$WORK/verify-killed.json"
VERIFY_STATUS=0
run_verify "$IN_LEDGER" "$KILLED_JSON" || VERIFY_STATUS=$?
if [ "$VERIFY_STATUS" -eq 2 ]; then
    cat "$WORK/verify.err" >&2
    fail "tools.verify exited 2 (UNVERIFIED): it read nothing to reconcile. Exit 2 is neither a pass nor a mismatch, so this beat refuses to report one."
fi
cat "$WORK/verify.err" >&2 || true

EXPECTED="$(verify_json "$KILLED_JSON" counts.expected)"
STORED="$(verify_json "$KILLED_JSON" counts.stored)"
MISSING="$(verify_json "$KILLED_JSON" counts.missing)"
UNEXPECTED="$(verify_json "$KILLED_JSON" counts.unexpected)"
DUPLICATES="$(verify_json "$KILLED_JSON" counts.wire_duplicates)"
DLQ="$(verify_json "$KILLED_JSON" counts.dlq)"
UNDECODABLE="$(verify_json "$KILLED_JSON" counts.undecodable)"
DRAINED="$(verify_json "$KILLED_JSON" counts.drained)"
PSEUDO_BAD="$(verify_json "$KILLED_JSON" counts.pseudonym_mismatches)"
LOSS_WINDOW=$((MISSING - UNACCEPTED))

info "ledger $EXPECTED distinct (source, id)   stored $STORED   dlq $DLQ"
info "missing $MISSING   unexpected $UNEXPECTED   wire duplicates $DUPLICATES   undecodable $UNDECODABLE"
info ""
info "  unaccepted (never delivered to the gateway)  $UNACCEPTED"
info "  un-acked window (202'd into the buffer)      $LOSS_WINDOW"
info "  bound on that window (buffer capacity)       $BUFFER_CAPACITY"
if [ "$LOSS_WINDOW" -eq 0 ]; then
    info "  the un-acked window was empty in THIS run, so nothing was lost from the buffer."
    info "  That is a measurement of this run, not a guarantee: a 202 is acceptance into"
    info "  an in-memory buffer, and the next SIGKILL may land with a non-empty one."
else
    info "  the un-acked window held $LOSS_WINDOW event(s) at the instant of the kill, which is"
    info "  at most the buffer's capacity by construction. It is bounded and reported;"
    info "  it is NOT zero and must not be described as zero."
fi

[ "$LOSS_WINDOW" -ge 0 ] \
    || fail "missing ($MISSING) is below unaccepted ($UNACCEPTED): the shortfall is smaller than the events the driver never got an answer for, so something is being counted twice or the topic was not empty when the run started."
[ "$MISSING" -le $((UNACCEPTED + BUFFER_CAPACITY)) ] \
    || fail "the shortfall ($MISSING) exceeds the batches refused ($UNACCEPTED) plus the whole buffer ($BUFFER_CAPACITY): the loss is not bounded by the buffer, which is the whole claim."
[ "$MISSING" -lt "$EXPECTED" ] \
    || fail "every ledger event is missing: this is total loss, not a bounded window. Either the gateway never produced anything, or the topic was not empty when the run started."
[ "$UNEXPECTED" -eq 0 ] || fail "$UNEXPECTED stored (source, id) pair(s) are absent from the ground truth: the broker holds records this run never sent."
[ "$UNDECODABLE" -eq 0 ] || fail "$UNDECODABLE topic message(s) would not decode, so the reconciliation rests on records it could not read."
[ "$DRAINED" = "True" ] || fail "the topic read stopped short of its end offsets, so this is a truncated view and not a reconciliation."
[ "$PSEUDO_BAD" != "None" ] \
    || fail "the pseudonym check was SKIPPED: no MASTER_SECRET in the gateway container, so nothing proved the stored user_id_pseudo is an HMAC of the ledger's user_id"
[ "$PSEUDO_BAD" = "0" ] || fail "$PSEUDO_BAD stored user_id_pseudo value(s) are not the expected HMAC of the ledger's user_id"
# A retried batch whose 202 was lost to the kill is a wire duplicate by
# construction: the same bytes with the same ids reach the topic twice. The count
# cannot be known before the run, so it is measured and bounded instead of
# asserted against a number nobody could have predicted.
info ""
info "wire duplicates: $DUPLICATES after (source, id) dedup. Expected, not a bug: a batch"
info "resent after the kill carries the same ids, so it produces one record twice."
info "Bounded by the events in the batches that were retried: at most"
info "$((RETRIED * 200)) (${RETRIED} retried batches x 200 events, the driver's per-request cap)."
[ "$DUPLICATES" -le $((RETRIED * 200)) ] \
    || fail "$DUPLICATES wire duplicates exceed $((RETRIED * 200)), the maximum the ${RETRIED} retried batches could account for: some duplicate came from somewhere this scenario did not cause."
if [ "$VERIFY_STATUS" -eq 0 ]; then
    info ""
    info "tools.verify exited 0 on the killed run: every event the driver was told the"
    info "gateway had buffered also reached the broker, and the shortfall for this run is zero."
fi
info "The scorecard's own failure list for this run:"
"$PYTHON" -c 'import json,sys; [print("  - " + f) for f in json.load(open(sys.argv[1]))["failures"]]' "$KILLED_JSON" | sed -n '1,6p'

step "the restart: a fresh run must reconcile clean, duplicates and DLQ included"
reset_topics
if ! run_driver --sessions "$RECOVERY_SESSIONS" --ledger "$IN_RECOVERY_LEDGER"; then
    sed -n '/^{/,$p' "$WORK/driver.log" || true
    fail "the post-restart run was not clean, so the gateway came back but is not serving: this is a recovery failure, not a loss window."
fi
sed -n '/^{/,$p' "$WORK/driver.log"
RECOVERY_JSON="$WORK/verify-recovery.json"
RECOVERY_STATUS=0
run_verify "$IN_RECOVERY_LEDGER" "$RECOVERY_JSON" --expect-duplicates 0 --expect-dlq 0 || RECOVERY_STATUS=$?
if [ "$RECOVERY_STATUS" -eq 2 ]; then
    cat "$WORK/verify.err" >&2
    fail "tools.verify exited 2 (UNVERIFIED) on the recovery run: it read nothing to reconcile."
fi
cat "$WORK/verify.err" >&2 || true
[ "$RECOVERY_STATUS" -eq 0 ] || {
    "$PYTHON" -c 'import json,sys; [print("  - " + f) for f in json.load(open(sys.argv[1]))["failures"]]' "$RECOVERY_JSON" >&2
    fail "tools.verify exited $RECOVERY_STATUS on a clean post-restart run with --expect-duplicates 0 --expect-dlq 0: the gateway came back and does not reconcile."
}
info "tools.verify exit 0: $(verify_json "$RECOVERY_JSON" counts.stored) events stored, 0 wire duplicates,"
info "0 in the DLQ, 0 missing, 0 unexpected, every stored pseudonym the expected HMAC."

printf '\n== PASS: shortfall %s = %s never delivered + %s un-acked in the buffer, against a bound of %s.\n' \
    "$MISSING" "$UNACCEPTED" "$LOSS_WINDOW" "$((UNACCEPTED + BUFFER_CAPACITY))"
printf '   The gateway came back, and the next run reconciled clean (verify exit 0, 0 duplicates).\n'