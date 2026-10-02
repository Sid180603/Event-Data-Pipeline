#!/usr/bin/env bash
# Beat 2 of 6: take the broker away and prove the gateway fails loudly and
# recovers.
#
# WHAT IT PROVES
#   The bounded buffer exists so that "I accepted work" and "I can hold it" stay
#   the same sentence. With the broker gone the producer fills, `sink()` raises,
#   and the gateway answers 503 rather than taking more:
#     * a direct signed probe observes HTTP 503 WITH a Retry-After header, from
#       inside the compose network, while traffic is still flowing;
#     * the driver's own receipt agrees independently: `sink_unavailable` counts
#       the 503s it saw, and `by_status` names the gateway's own reason code
#       (SINK_UNAVAILABLE / PRODUCER_QUEUE_FULL / DLQ_UNAVAILABLE);
#     * gateway_http_responses_total{status_class="5xx"} rises by at least as
#       many as the probe-plus-driver observed;
#     * the broker is brought back (on a trap, so a mid-script failure cannot
#       leave the machine broken), the gateway's producer reconnects, a probe is
#       accepted with 202 again, and a fresh run reconciles clean with
#       --expect-duplicates 0 --expect-dlq 0.
#
# WHY THE OUTAGE RUN IS LARGE, and why this beat is the slowest of the four:
#   the buffer holds PRODUCER_QUEUE_MAXSIZE records (50,000 at app/config.py), and
#   a 503 only happens once it is full. The run therefore has to push more than
#   that many events past the per-tenant token buckets before the gateway can
#   refuse anything, which at one request per 200 events is a few tens of
#   seconds. Budget two minutes for CHAOS_SESSIONS=14000.
#
# WHY THE OUTAGE RUN GETS ONE ATTEMPT PER BATCH (CHAOS_MAX_ATTEMPTS=1):
#   a 503 means "come back later", and once the buffer is full it stays full for
#   as long as the broker is gone. With the driver's default of five attempts and
#   the gateway's Retry-After of one second, every batch after the buffer fills
#   costs four seconds of sleeping, so a run that would take forty seconds takes
#   the better part of an hour. The beat measures that the gateway REFUSES, and
#   one attempt is the honest client for that: it takes the answer and stops. The
#   recovery half keeps the default retry policy, because recovery is the half
#   where a client's patience is the thing under test.
#
# WHAT IT NEEDS
#   The compose stack up, `.env` and driver-signing-key.pem at the repository
#   root. The probe reads contracts/examples/job-viewed.json and mints its token
#   with driver-signing-key.pem, both of which are visible inside the gateway
#   container through the repository bind mount.
#
# WHAT IT DELIBERATELY DOES NOT DO
#   It does not reconcile the outage run. There is no broker to reconcile against,
#   so tools.verify would exit 2 -- "I verified nothing" -- and a scorecard for an
#   unreadable topic is the one outcome worse than having no tool. The outage run's
#   evidence is the probe's status codes, the driver's own receipt and the 5xx
#   counter. The reconciled evidence is the run AFTER the broker returns.
#
# ENVIRONMENT
#   COMPOSE               docker compose command           (default: docker compose)
#   PYTHON                interpreter for the host-side checks
#   CHAOS_SESSIONS        sessions in the outage run       (default: 14000)
#   CHAOS_RECOVERY_SESSIONS  sessions in the recovery run   (default: 2000)
#   CHAOS_MAX_ATTEMPTS    attempts per batch, outage run   (default: 1; see above)
#   CHAOS_PROBE_TENANT    tenant the probe signs for  (default: first GATEWAY_TENANTS)
#   CHAOS_PROBE_AFTER     seconds before the 503 probe     (default: 5)
#   CHAOS_PROBE_BUDGET    seconds the probe waits for a 503 (default: 60)
#   CHAOS_LEDGER          ground truth, outage run         (default: chaos-broker-down.jsonl)
#   CHAOS_RECOVERY_LEDGER ground truth, recovery run        (default: chaos-broker-recovery.jsonl)
#   CHAOS_VERIFY_TIMEOUT  per-topic read budget, seconds    (default: 60)
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
CHAOS_SESSIONS="${CHAOS_SESSIONS:-14000}"
CHAOS_RECOVERY_SESSIONS="${CHAOS_RECOVERY_SESSIONS:-2000}"
CHAOS_MAX_ATTEMPTS="${CHAOS_MAX_ATTEMPTS:-1}"
CHAOS_PROBE_AFTER="${CHAOS_PROBE_AFTER:-5}"
CHAOS_PROBE_BUDGET="${CHAOS_PROBE_BUDGET:-60}"
CHAOS_LEDGER="${CHAOS_LEDGER:-chaos-broker-down.jsonl}"
CHAOS_RECOVERY_LEDGER="${CHAOS_RECOVERY_LEDGER:-chaos-broker-recovery.jsonl}"
CHAOS_VERIFY_TIMEOUT="${CHAOS_VERIFY_TIMEOUT:-60}"
SESSIONS="${CHAOS_SESSIONS}"
RECOVERY_SESSIONS="${CHAOS_RECOVERY_SESSIONS}"
IN_LEDGER="/app/${CHAOS_LEDGER}"
IN_RECOVERY_LEDGER="/app/${CHAOS_RECOVERY_LEDGER}"

WORK="$(mktemp -d)"
DRIVER_PID=""

usage() {
    cat <<'USAGE'
broker_down.sh -- stop the broker, prove the gateway answers 503 with
Retry-After, then bring the broker back and prove it recovers.

Proves:
  * a signed probe gets HTTP 503 AND a Retry-After header while the broker is
    gone and traffic is still flowing. The probe signs for a tenant from
    GATEWAY_TENANTS and repoints the published example's `source` at it, because
    a probe for an unregistered tenant would be refused 403 and read as the
    gateway refusing work;
  * the driver's receipt agrees: sink_unavailable > 0, and by_status names the
    gateway's own 503 reason code;
  * gateway_http_responses_total{status_class="5xx"} rises accordingly;
  * after the broker is back the probe is accepted (202) and a fresh run
    reconciles clean (tools.verify exit 0, --expect-duplicates 0
    --expect-dlq 0).

The outage run is NOT reconciled: there is no broker to reconcile against, and
tools.verify would exit 2, which means it read nothing rather than that it
matched.

The broker is restored on exit, including on Ctrl-C or a failed assertion, so a
broken run cannot leave the machine without a broker.

Slow by design: the producer buffer holds 50,000 records, so a 503 needs more
than 50,000 events to have been enqueued first. Budget about two minutes.

Needs: the compose stack up, .env and driver-signing-key.pem present. Runs
everything inside the compose network; nothing is published to the host.

Environment: COMPOSE, PYTHON, CHAOS_SESSIONS, CHAOS_RECOVERY_SESSIONS,
CHAOS_MAX_ATTEMPTS, CHAOS_PROBE_TENANT, CHAOS_PROBE_AFTER, CHAOS_PROBE_BUDGET,
CHAOS_LEDGER, CHAOS_RECOVERY_LEDGER, CHAOS_VERIFY_TIMEOUT.
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

run_driver() {  # [--no-deps] extra driver.main arguments...
    # `--no-deps` for the outage run only. `docker compose run` resolves the
    # driver's dependencies, sees `kafka` stopped, and would start it -- which
    # would end the outage this beat exists to measure.
    local skip=""
    if [ "${1-}" = "--no-deps" ]; then
        skip="--no-deps"
        shift
    fi
    local args=("$@")
    # `docker compose run` needs the service's command overridden to pass any
    # argument at all, and `exec` so the container's exit code is the driver's.
    $COMPOSE run --rm -T $skip driver bash -c \
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

scrape_metrics() {  # destination
    $COMPOSE exec -T gateway "$PYTHON" -c \
        "import urllib.request as u; print(u.urlopen('http://localhost:8000/metrics', timeout=5).read().decode())" \
        > "$1" || fail "could not scrape /metrics from the gateway: is it running?"
}

# One metric family summed over every series in the scrape, parsed with
# tools.observe's own parser so this cannot drift from the live view. The gateway
# runs one uvicorn worker in this beat, so before/after scrapes are comparable.
metric_total() {  # file metric-name [status_class]
    "$PYTHON" - "$1" "$2" "${3-}" <<'PY'
import sys
from tools.observe import parse_exposition

families = parse_exposition(open(sys.argv[1], encoding="utf-8", errors="replace").read())
name, wanted = sys.argv[2], sys.argv[3]
total = 0.0
for sample in families.get(name, ()):
    if wanted and sample.labels.get("status_class") != wanted:
        continue
    total += sample.value
print(int(total) if total == int(total) else total)
PY
}

# One signed, single-event request from inside the network, printing
# "<status> <Retry-After> <reason>" per attempt until it sees the status asked
# for. It signs with driver.replay.TokenMinter, which is the same signer the
# driver uses, and posts the repository's own published ingress example, so the
# request is a real one rather than a hand-rolled approximation of the contract.
#
# The example is posted with its `source` REPOINTED at a tenant this deployment
# actually provisioned, and the token names that same tenant. Both halves matter:
# `authorize_batch` binds the tenant from the signed claim and refuses a body whose
# `source` is anybody else's, so posting the example verbatim would earn a 403 on
# every attempt -- `acme_8921` is the example's own fixture tenant and is not in the
# `.env` this stack runs on. A 403 from this probe would look exactly like a gateway
# that refuses work, and it would be the probe's fault, not the gateway's.
#
# It reads `driver-signing-key.pem` from inside the gateway container, which the
# bind mount makes visible. That is a property of the demo's mount, not of the
# gateway: `app/auth/jwt.py` holds only the public half and refuses a private key,
# so the gateway process cannot mint a token for anyone. The operator running this
# script can read the file anyway.
probe_ingest() {  # tenant wanted-status budget-seconds
    $COMPOSE exec -T gateway "$PYTHON" - "$1" "$2" "$3" <<'PY'
import json
import sys
import time

import httpx

from app.config import Settings
from driver.replay import BATCH_CONTENT_TYPE, INGEST_PATH, TokenMinter

tenant, wanted, budget = sys.argv[1], int(sys.argv[2]), float(sys.argv[3])
settings = Settings()
sample = json.loads(open("contracts/examples/job-viewed.json", "rb").read())[0]
sample["source"] = f"/careers/{tenant}"
body = json.dumps([sample]).encode()
with open("driver-signing-key.pem", "rb") as handle:
    signer = TokenMinter(handle.read(), audience=settings.jwt_audience)
token = signer.token(tenant, sample["sourcechannel"])
headers = {
    "content-type": BATCH_CONTENT_TYPE,
    "authorization": f"Bearer {token}",
    "x-source-type": sample["sourcechannel"],
}
deadline = time.monotonic() + budget
last = None
with httpx.Client(timeout=10.0) as client:
    while True:
        response = client.post(f"http://localhost:8000{INGEST_PATH}", content=body, headers=headers)
        try:
            reason = response.json().get("reason", "-")
        except ValueError:
            reason = "unreadable body"
        retry_after = response.headers.get("Retry-After", "-")
        print(f"{response.status_code} {retry_after} {reason}", flush=True)
        if response.status_code == wanted:
            raise SystemExit(0)
        last = (response.status_code, retry_after, reason)
        if time.monotonic() >= deadline:
            break
        time.sleep(1)
print(f"never answered {wanted}; the last answer was {last}", file=sys.stderr)
raise SystemExit(1)
PY
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

wait_for_broker() {
    local attempt
    for attempt in $(seq 1 60); do
        if $COMPOSE exec -T kafka /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka:9092 --list > /dev/null 2>&1; then
            return 0
        fi
        sleep 2
    done
    fail "the broker did not answer --list within 120s of being started"
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
    # One uvicorn worker, so the before/after /metrics scrapes below are answered
    # by the same process. The default is four, and uvicorn hands a scrape to
    # exactly one of them, which would make the counter deltas meaningless.
    GATEWAY_WORKERS=1 $COMPOSE up -d gateway > "$WORK/gateway-up.log" 2>&1 \
        || fail "'docker compose up -d gateway' failed; see $WORK/gateway-up.log"
    wait_for_gateway
}

preflight() {
    command -v docker > /dev/null 2>&1 || fail "docker is not on PATH: this beat runs the compose stack, so it needs Docker (WSL2 + Docker Desktop on Windows)."
    docker compose version > /dev/null 2>&1 || fail "'docker compose' is not available. Install Docker Desktop, or set COMPOSE to a working compose command."
    [ -f .env ] || fail "no .env at the repository root: compose will not invent MASTER_SECRET and a JWT public key. Run the generator in the header of docker-compose.yml."
    [ -f driver-signing-key.pem ] || fail "no driver-signing-key.pem: that is the PRIVATE half of the signing pair, written by the same generator. The probe cannot mint a tenant token without it."
    [ -r driver-signing-key.pem ] || fail "driver-signing-key.pem is not readable: the probe runs as root inside the container but this script checks it on the host."
    [ -f contracts/examples/job-viewed.json ] || fail "contracts/examples/job-viewed.json is missing: the probe posts the repository's own published ingress example rather than a hand-rolled one."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx kafka \
        || fail "the kafka service is not running: 'make up' first. This beat stops it and has to bring it back."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx gateway \
        || fail "the gateway service is not running: 'make up' first."
    # The probe signs for a tenant the gateway has actually registered. GATEWAY_TENANTS
    # in .env is that registry's source (app/main.py expands it into one credential
    # per source channel), so an entry from that list is a tenant a probe can be
    # refused for something real about rather than for being unknown.
    local configured
    configured="$(sed -n 's/^GATEWAY_TENANTS=//p' .env | tr -d '"'"'" | tr ',' '\n' | grep -c . || true)"
    [ "${configured:-0}" -gt 0 ] \
        || fail "GATEWAY_TENANTS is empty or missing from .env, so there is no tenant the probe could sign for. Run the generator in the header of docker-compose.yml."
    if [ -n "${CHAOS_PROBE_TENANT-}" ]; then
        info "the probe signs for ${CHAOS_PROBE_TENANT}, from CHAOS_PROBE_TENANT."
    else
        CHAOS_PROBE_TENANT="$(sed -n 's/^GATEWAY_TENANTS=//p' .env | tr -d '"'"'" | tr ',' '\n' | grep . | head -n 1)"
        info "the probe signs for ${CHAOS_PROBE_TENANT}, the first tenant this deployment registered."
    fi
    [ -n "$CHAOS_PROBE_TENANT" ] || fail "could not read a tenant out of GATEWAY_TENANTS in .env."
    case ",$(sed -n 's/^GATEWAY_TENANTS=//p' .env | tr -d '"'"'" | tr ',' '\n' | grep . | tr '\n' ',' | sed 's/,$//')," in
        *",${CHAOS_PROBE_TENANT},"*) ;;
        *) fail "${CHAOS_PROBE_TENANT} is not in GATEWAY_TENANTS in .env, so the probe would be refused with 403 before the gateway ever considered the broker. Set CHAOS_PROBE_TENANT to a tenant from that list." ;;
    esac
    start_gateway
    BUFFER_CAPACITY="$("$PYTHON" -c 'import app.config; print(app.config.PRODUCER_QUEUE_MAXSIZE)')"
    info "the producer buffer holds $BUFFER_CAPACITY records (app/config.py: PRODUCER_QUEUE_MAXSIZE);"
    info "a 503 is only possible once it is full, which is why the outage run is large."
}

cleanup() {
    if [ -n "$DRIVER_PID" ] && kill -0 "$DRIVER_PID" 2> /dev/null; then
        kill "$DRIVER_PID" 2> /dev/null || true
    fi
    # This beat stops the broker, so leaving it stopped would break the beats
    # after it -- and would leave the demo machine with no broker at all.
    if [ -f "$WORK/kafka-stopped" ] && command -v docker > /dev/null 2>&1; then
        info "cleanup: restarting the broker, because this script stopped it"
        $COMPOSE start kafka > /dev/null 2>&1 \
            || info "cleanup: 'docker compose start kafka' failed; run 'make up' before the next beat"
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

step "beat 2: stop the broker; the gateway must fail loudly and then recover"
preflight
reset_topics

step "gateway metrics before the outage"
scrape_metrics "$WORK/metrics-before.txt"
FIVE_XX_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_http_responses_total 5xx)"
BUFFER_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_buffer_items)"
info "5xx responses so far: $FIVE_XX_BEFORE"
info "producer queue: $BUFFER_BEFORE of $BUFFER_CAPACITY records"
info "(gateway_buffer_utilisation can read above 1.0 and 1.0 is NOT the 503 point;"
info " app/metrics.py says so in the metric's own HELP text. The 503 point is a full queue.)"

step "stopping the broker"
$COMPOSE stop kafka > "$WORK/kafka-stop.log" 2>&1 \
    || fail "'docker compose stop kafka' failed; see $WORK/kafka-stop.log"
touch "$WORK/kafka-stopped"
info "kafka is stopped. The gateway is still up: /healthz is liveness only and does"
info "not consult the broker (app/main.py), so it keeps taking work until the buffer fills."

step "traffic into the outage ($SESSIONS sessions), probing for a 503 at +${CHAOS_PROBE_AFTER}s"
run_driver --no-deps --sessions "$SESSIONS" --max-attempts "$CHAOS_MAX_ATTEMPTS" --ledger "$IN_LEDGER" &
DRIVER_PID=$!
sleep "$CHAOS_PROBE_AFTER"
# `tee` so the transition from 202 to 503 is visible while it happens; this is
# read live, not afterwards.
probe_ingest "$CHAOS_PROBE_TENANT" 503 "$CHAOS_PROBE_BUDGET" 2>&1 | tee "$WORK/probe.txt" \
    || fail "the gateway never answered 503 with the broker gone, for up to ${CHAOS_PROBE_BUDGET}s. The probe answers are above: accepting work it cannot hold is the failure this beat exists to catch."
DRIVER_STATUS=0
wait "$DRIVER_PID" || DRIVER_STATUS=$?
DRIVER_PID=""

PROBE_STATUS="$(tail -n 1 "$WORK/probe.txt" | awk '{print $1}')"
PROBE_RETRY="$(tail -n 1 "$WORK/probe.txt" | awk '{print $2}')"
PROBE_REASON="$(tail -n 1 "$WORK/probe.txt" | awk '{print $3}')"
info ""
info "the probe saw: status $PROBE_STATUS, Retry-After: $PROBE_RETRY, reason $PROBE_REASON"
[ "$PROBE_RETRY" != "-" ] || fail "the gateway answered 503 with no Retry-After header: CONTRACT.md section 3 requires one, and without it a client cannot know when to come back."
[ "$PROBE_STATUS" = "503" ] || fail "the probe's last answer was $PROBE_STATUS, not 503"

sed -n '/^{/,$p' "$WORK/driver.log"
SENT="$(json_field "$WORK/driver.log" sent)"
UNACCEPTED="$(json_field "$WORK/driver.log" unaccepted)"
SINK_503="$(json_field "$WORK/driver.log" sink_unavailable)"
LIMITED="$(json_field "$WORK/driver.log" rate_limited)"
BY_STATUS="$(json_field "$WORK/driver.log" by_status)"
info ""
info "the driver's receipt: sent $SENT, 503 responses $SINK_503, 429 responses $LIMITED"
info "unaccepted $UNACCEPTED (sent, never accounted for) -- exit status $DRIVER_STATUS"
info "refusals by the gateway's own reason code: $BY_STATUS"
info "This run is deliberately NOT reconciled: the broker it would be reconciled against is"
info "gone, and tools.verify would exit 2, which means it read nothing rather than that it"
info "matched. The reconciled evidence for this beat is the run after the broker returns."
[ "$SINK_503" -gt 0 ] \
    || fail "the driver saw no 503 at all, though the probe did: the gateway served one request path differently from another, and neither of them refuses work it cannot hold for everyone."
"$PYTHON" - "$WORK/driver.log" <<'PY' \
    || fail "no batch ended with the gateway's own 503 reason code: 202 and 429 were seen but nothing was refused for want of a broker."
import json, sys

lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
starts = [i for i, line in enumerate(lines) if line == "{"]
by_status = json.loads("\n".join(lines[starts[-1]:]))["by_status"]
refusals = {"SINK_UNAVAILABLE", "PRODUCER_QUEUE_FULL", "DLQ_UNAVAILABLE"}
if not refusals & set(by_status):
    sys.exit("by_status was " + json.dumps(by_status, sort_keys=True))
PY
[ "$DRIVER_STATUS" -ne 0 ] \
    || fail "the outage run exited clean even though the broker was gone: it cannot have delivered $SENT events to a broker that was not there, so the receipt is not trustworthy."

step "gateway metrics after the outage"
scrape_metrics "$WORK/metrics-after.txt"
FIVE_XX_AFTER="$(metric_total "$WORK/metrics-after.txt" gateway_http_responses_total 5xx)"
BUFFER_AFTER="$(metric_total "$WORK/metrics-after.txt" gateway_buffer_items)"
DENIALS_AFTER="$(metric_total "$WORK/metrics-after.txt" gateway_rate_limit_denials_total)"
info "5xx responses: $FIVE_XX_BEFORE -> $FIVE_XX_AFTER"
info "producer queue: $BUFFER_BEFORE -> $BUFFER_AFTER of $BUFFER_CAPACITY records"
info "rate-limit denials so far: $DENIALS_AFTER"
[ "$FIVE_XX_AFTER" -gt "$FIVE_XX_BEFORE" ] \
    || fail "gateway_http_responses_total{status_class=\"5xx\"} did not rise during the outage, though the driver and the probe both saw 503s: the counters are not wired to the refusals."

step "bringing the broker back"
$COMPOSE start kafka > "$WORK/kafka-start.log" 2>&1 \
    || fail "'docker compose start kafka' failed; see $WORK/kafka-start.log"
wait_for_broker
rm -f "$WORK/kafka-stopped"
info "the broker is back and answering --list."

step "the gateway must recover without being restarted"
probe_ingest "$CHAOS_PROBE_TENANT" 202 30 2>&1 | tee "$WORK/probe-recovery.txt" \
    || fail "with the broker back, the gateway still refuses work after 30s. librdkafka reconnects on its own, so this is the gateway failing to recover, not a slow reconnect."
info "the probe now sees: $(tail -n 1 "$WORK/probe-recovery.txt")"

step "a fresh run must reconcile clean, duplicates and DLQ included"
reset_topics
if ! run_driver --sessions "$RECOVERY_SESSIONS" --ledger "$IN_RECOVERY_LEDGER"; then
    sed -n '/^{/,$p' "$WORK/driver.log" || true
    fail "the post-recovery run was not clean: the broker is back but the gateway is not serving correctly."
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
    fail "tools.verify exited $RECOVERY_STATUS on a clean post-recovery run with --expect-duplicates 0 --expect-dlq 0."
}
info "tools.verify exit 0: $(verify_json "$RECOVERY_JSON" counts.stored) events stored, 0 wire duplicates,"
info "0 in the DLQ, 0 missing, 0 unexpected, every stored pseudonym the expected HMAC."

printf '\n== PASS: the gateway answered 503 with Retry-After (%s) while the broker was gone,\n' "$PROBE_RETRY"
printf '   the driver saw %s 503(s), and after the broker returned the next run reconciled clean.\n' "$SINK_503"