#!/usr/bin/env bash
# Beat 6 of 6: one tenant floods, and the other 499 are provably unaffected.
#
# WHAT IT PROVES
#   Tenant isolation under load, in five falsifiable halves:
#     1. the flood is a SEPARATE stream, so the baseline corpus is not perturbed:
#        `driver.inject` is run twice with the same seed, once with
#        --flood-tenant and once without, and every other tenant's event count
#        must be identical. TenantFlood exists as a separate object precisely so
#        that this comparison means something (driver/inject.py says so).
#     2. the flooded tenant is SHED with 429: the clients' own receipts count the
#        429s and name RATE_LIMITED in by_status, and gateway_rate_limit_denials_total
#        -- which only the 429 path increments -- rises, as does
#        gateway_http_responses_total{status_class="4xx"}.
#     3. no 429 produces a DLQ entry (CONTRACT.md section 3), so the DLQ depth is
#        IDENTICAL before and after the flood.
#     4. the other tenants' stored counts on career.events.raw are IDENTICAL
#        before and after the flood -- per source, from tools.verify's own
#        breakdown. This is the falsifiable half, and it is measured on the topic
#        rather than on the generator.
#     5. the shortfall is an identity, not a range: the number of events the flood
#        sent and never got an answer for equals the number reconciliation reports
#        missing, and nothing on the topic is absent from the ground truth.
#
# HOW THE FLOOD IS SENT, and why it takes SEVERAL CLIENTS
#   `python -m driver.inject` is an offline generator: it writes a ledger and never
#   talks to the gateway. So the flood is sent by running TenantFlood through
#   driver.replay.ReplayDriver, which is the same signer, the same batching, the
#   same retry policy and the same receipt the load driver uses. That is a
#   program on stdin, not a new file: the four beats share one helper vocabulary
#   and none of them adds a module.
#
#   The clients are CONCURRENT, and that is load-bearing rather than decorative.
#   The budget a flood has to exhaust is per tenant: 2,000 events/sec sustained
#   with a burst of 4,000 (app/main.py: DEFAULT_TENANT_LIMIT). A bucket that
#   refills as fast as one client can fill it can never be exhausted: no volume of
#   traffic produces a 429, at any size, from a single client. Measured on the
#   machine these scripts were written on, one client offers ~1,000-2,800
#   events/sec -- under the budget -- and produced exactly zero 429s over a
#   26,000-event flood. Three clients together offered ~3,600 events/sec and shed
#   2,242 batches. So CHAOS_FLOOD_CLIENTS defaults to 3, and dropping it to 1
#   will make this beat fail.
#
#   The clients are given DISJOINT slices of TenantFlood's users, which is what
#   keeps the ordering guarantee intact. TenantFlood picks a channel per session,
#   so driver.replay's batcher cuts its stream into short single-channel runs, and
#   regrouping those by channel to get bigger requests would deliver a user's
#   WEB_APP events, then their MOBILE_APP ones, then their partner's -- which
#   tools.verify counts as ordering violations (measured: 2,024 of them on a
#   3,489-event flood). Splitting by USER instead costs nothing: each user belongs
#   to exactly one client, so every user's events still go out in ascending
#   `sequence` and each `(source, user)` pair still lands on one Kafka partition.
#   Measured on the real pipeline: 0 ordering violations, 64 distinct users, every
#   event sent exactly once.
#
#   Each client is given ONE attempt per batch. A 429 means "come back later", and
#   this beat measures that the gateway sheds the tenant, not how patient a client
#   is: with the driver's default of five attempts and the limiter's Retry-After,
#   every shed batch costs four seconds of sleeping and the beat takes the better
#   part of an hour.
#
# WHAT IT NEEDS
#   The compose stack up, `.env` and driver-signing-key.pem at the repository
#   root, and a flood tenant that is in the gateway's GATEWAY_TENANTS list.
#
# ENVIRONMENT
#   COMPOSE               docker compose command            (default: docker compose)
#   PYTHON                interpreter for the host-side checks
#   CHAOS_TENANTS         tenants in the catalogue           (default: 500)
#   CHAOS_SESSIONS        sessions in the baseline run       (default: 6000)
#   CHAOS_CORPUS_SESSIONS sessions in the offline comparison (default: 6000)
#   CHAOS_SEED            corpus and injection seed          (default: 7)
#   CHAOS_FLOOD_TENANT    the tenant that floods             (default: tenant_0042)
#   CHAOS_FLOOD_SESSIONS  sessions in the whole flood        (default: 9000)
#   CHAOS_FLOOD_USERS     users the flood spreads over       (default: 64)
#   CHAOS_FLOOD_CLIENTS   concurrent clients sending it      (default: 3)
#   CHAOS_LEDGER          ground truth, baseline run         (default: chaos-flood-baseline.jsonl)
#   CHAOS_VERIFY_TIMEOUT  per-topic read budget, seconds     (default: 60)
#
# Why the baseline is 6,000 sessions and not fewer: "the other 499" is only true
# if all 500 tenants appear. Zipf apportionment gives a 500-tenant catalogue a
# non-zero share to every tenant at 6,000 sessions and to only 223 of them at
# 1,000 (measured with driver.skew.apportion), and the script asserts that it
# compared 499 tenants rather than however many turned up.
#
# Exit status: 0 only when every assertion above held. Reconciliation is
# EXPECTED to exit 1 here: the flooded tenant's shed batches are in the ground
# truth and on no topic, which is the contract working, not the beat failing.

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
CHAOS_TENANTS="${CHAOS_TENANTS:-500}"
CHAOS_SESSIONS="${CHAOS_SESSIONS:-6000}"
CHAOS_CORPUS_SESSIONS="${CHAOS_CORPUS_SESSIONS:-6000}"
CHAOS_SEED="${CHAOS_SEED:-7}"
CHAOS_FLOOD_TENANT="${CHAOS_FLOOD_TENANT:-tenant_0042}"
CHAOS_FLOOD_SESSIONS="${CHAOS_FLOOD_SESSIONS:-9000}"
CHAOS_FLOOD_USERS="${CHAOS_FLOOD_USERS:-64}"
CHAOS_FLOOD_CLIENTS="${CHAOS_FLOOD_CLIENTS:-3}"
CHAOS_LEDGER="${CHAOS_LEDGER:-chaos-flood-baseline.jsonl}"
CHAOS_VERIFY_TIMEOUT="${CHAOS_VERIFY_TIMEOUT:-60}"
TENANTS="${CHAOS_TENANTS}"
SESSIONS="${CHAOS_SESSIONS}"
CORPUS_SESSIONS="${CHAOS_CORPUS_SESSIONS}"
SEED="${CHAOS_SEED}"
FLOOD_TENANT="${CHAOS_FLOOD_TENANT}"
FLOOD_SESSIONS="${CHAOS_FLOOD_SESSIONS}"
FLOOD_USERS="${CHAOS_FLOOD_USERS}"
FLOOD_CLIENTS="${CHAOS_FLOOD_CLIENTS}"
IN_LEDGER="/app/${CHAOS_LEDGER}"
# The ground truth for the whole beat is the baseline plus every client's flood,
# concatenated: JSONL is append-only, and tools.verify reads one ledger against
# the whole topic. Two ledgers would leave the flood's stored records
# "unexpected".
COMBINED_LEDGER="chaos-flood-combined.jsonl"
IN_COMBINED_LEDGER="/app/${COMBINED_LEDGER}"
FLOOD_LEDGERS=()
for _client in $(seq 0 $((FLOOD_CLIENTS - 1))); do
    FLOOD_LEDGERS+=("chaos-flood-client-${_client}.jsonl")
done

WORK="$(mktemp -d)"
DRIVER_PID=""
FLOOD_PIDS=()

usage() {
    cat <<'USAGE'
tenant_flood.sh -- one tenant floods; every other tenant is provably unaffected.

Proves:
  * driver.inject's baseline corpus is identical with and without --flood-tenant
    (per-tenant counts diff), so the flood cannot have perturbed anybody else's
    traffic;
  * the flooded tenant is shed with 429: the clients' receipts count them, they
    name RATE_LIMITED as the gateway's own code, and
    gateway_rate_limit_denials_total rises;
  * the DLQ depth is unchanged by the flood, because a 429 produces no DLQ entry
    (CONTRACT.md section 3);
  * every other tenant's stored count on career.events.raw is IDENTICAL before
    and after the flood, per source, from tools.verify's own breakdown;
  * the flood's unaccepted count equals the shortfall reconciliation reports, and
    the clients together sent each of the flood's users exactly once.

tools.verify is EXPECTED to exit 1 on the combined ledger. The flooded tenant's
shed batches are in the ground truth and on no topic; that is the contract
working, and the script asserts the shortfall equals the number of refused events
rather than waving the mismatch through.

Needs: the compose stack up, .env and driver-signing-key.pem present, and a flood
tenant that is in the gateway's GATEWAY_TENANTS list. Runs everything inside the
compose network; nothing is published to the host.

Environment: COMPOSE, PYTHON, CHAOS_TENANTS, CHAOS_SESSIONS,
CHAOS_CORPUS_SESSIONS, CHAOS_SEED, CHAOS_FLOOD_TENANT, CHAOS_FLOOD_SESSIONS,
CHAOS_FLOOD_USERS, CHAOS_FLOOD_CLIENTS, CHAOS_LEDGER, CHAOS_VERIFY_TIMEOUT.
USAGE
}

info() { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
fail() { printf '\nBEAT FAILED: %s\n' "$*" >&2; exit 1; }

# Read one field out of a JSON document on disk, and only that document. The test
# is an EXACT `{` at column 0, not a stripped one: tools.verify --json prints
# indent=2, so every row of its `by_source` table is a `    {` -- which strips to
# `{` as well. Matching the stripped form picks the last table row instead of the
# document, and json.loads then reports "Extra data" on a file that is perfectly
# good. `docker compose run` prefixes the container's output with whatever pip
# printed, which is why a naive json.load on the whole file is not an option
# either.
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

# The ledger's per-tenant row counts, tenant-sorted, read in the gateway container
# so the host needs nothing from the repository's dependencies. The comparison
# below is between two of these files, and the flooded tenant is the one row
# allowed to differ.
ledger_tenants() {  # in-container-ledger
    $COMPOSE exec -T gateway "$PYTHON" - "$1" <<'PY'
import sys
from collections import Counter
from pathlib import Path

from contracts.ledger import Ledger

counts = Counter(row.tenant for row in Ledger(Path(sys.argv[1])).read_all())
for tenant in sorted(counts):
    print(f"{tenant} {counts[tenant]}")
PY
}

# Per client: rows written and distinct users in the ledger, so the script can tie
# each receipt to the ground truth and prove the clients' user slices were disjoint
# and covered the flood between them.
flood_ledger_census() {  # in-container-ledger...
    $COMPOSE exec -T gateway "$PYTHON" - "$@" <<'PY'
import json
import sys
from pathlib import Path

from contracts.ledger import Ledger

rows = users = 0
detail = []
for path in sys.argv[1:]:
    records = Ledger(Path(path)).read_all()
    detail.append({"ledger": Path(path).name, "rows": len(records),
                   "users": len({r.user_pseudo for r in records})})
    rows += len(records)
    users += len({r.user_pseudo for r in records})
print(json.dumps({"rows": rows, "users": users, "per_client": detail}, indent=2))
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

run_inject() {  # extra driver.inject arguments...
    local args=("$@")
    # driver.inject is an offline generator: it never talks to the gateway, so
    # running it in the already-warm gateway container costs no image start-up.
    $COMPOSE exec -T gateway "$PYTHON" -m driver.inject "${args[@]}"
}

# The flood client. One process per slice of the flood's users; the program is on
# stdin, so it needs no file in the repository and the three clients run the same
# text. `python - <args>` reads the program from stdin and takes the arguments
# from argv, which is why the argv list is positional and quoted.
FLOOD_PROGRAM="$WORK/flood-client.py"
write_flood_program() {
    cat > "$FLOOD_PROGRAM" <<'FLOOD'
"""One slice of a TenantFlood, sent through the driver's own replay client.

argv: tenant sessions users seed clients shard ledger
"""
import os
import sys
from pathlib import Path

import httpx

from driver.inject import TenantFlood
from driver.replay import ReplayDriver, TokenMinter

tenant = sys.argv[1]
sessions, users, seed = int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4])
clients, shard = int(sys.argv[5]), int(sys.argv[6])
ledger = sys.argv[7]

flood = TenantFlood(tenant, sessions=sessions, users=users, seed=seed)
# This client's slice of the flood's users, and nothing else. The wire user id is
# f"usr_{user_pseudo[-8:]}" (driver/fsm._event), so this is the same derivation
# rather than a guess; a wrong one leaves the client with no events at all, which
# the script fails on. TenantFlood.round-robins the sessions across `users`, so
# taking every `clients`-th user splits the stream by user and leaves every user's
# own events contiguous and in ascending `sequence`.
mine = {f"usr_{user[-8:]}" for user in flood.users[shard::clients]}


def stream():
    for events in flood.events():
        kept = [event for event in events if event["data"]["candidate"]["user_id"] in mine]
        if kept:
            yield kept


signer = TokenMinter(
    Path("driver-signing-key.pem").read_bytes(), audience=os.environ.get("JWT_AUD", "career-api")
)
with httpx.Client(timeout=30.0) as client:
    report = ReplayDriver(
        url=os.environ.get("GATEWAY_URL", "http://localhost:8000"),
        signer=signer,
        client=client,
        ledger_path=Path(ledger),
        # One attempt: a 429 is "come back later", and this beat measures that the
        # gateway sheds the tenant rather than how long a client waits to retry.
        max_attempts=1,
    ).run(stream())
print(__import__("json").dumps(report.to_dict(), indent=2))
# Non-zero because a shed flood is not a clean run, and the driver says so in its
# exit code as well as its receipt. The script asserts on the receipt and on this
# status separately, so neither can hide the other.
raise SystemExit(0 if report.clean else 1)
FLOOD
}

send_flood_client() {  # shard
    local ledger="/app/${FLOOD_LEDGERS[$1]}"
    $COMPOSE run --rm -T driver bash -c \
        "pip install --quiet --no-input -e . && exec $PYTHON - \
            '$FLOOD_TENANT' '$FLOOD_SESSIONS' '$FLOOD_USERS' '$SEED' \
            '$FLOOD_CLIENTS' '$1' '$ledger'" \
        < "$FLOOD_PROGRAM" > "$WORK/flood-$1.log" 2>&1
}

# Sum the clients' receipts into one document. A client that printed no receipt is
# a hard failure here rather than a zero, because a silent client would otherwise
# read as a quiet one.
sum_flood_receipts() {
    "$PYTHON" - "$WORK" "$FLOOD_CLIENTS" <<'PY'
import json
import os
import sys

work, clients = sys.argv[1], int(sys.argv[2])
keys = ("batches", "sent", "accepted", "rejected", "rate_limited", "sink_unavailable",
        "unaccepted", "retried")
total = {key: 0 for key in keys}
total["by_status"] = {}
for shard in range(clients):
    path = os.path.join(work, f"flood-{shard}.log")
    lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    starts = [i for i, line in enumerate(lines) if line == "{"]
    if not starts:
        sys.exit(f"flood client {shard} printed no receipt: see {path}")
    receipt = json.loads("\n".join(lines[starts[-1]:]))
    for key in keys:
        total[key] += receipt[key]
    for code, count in receipt["by_status"].items():
        total["by_status"][code] = total["by_status"].get(code, 0) + count
total["clients"] = clients
with open(os.path.join(work, "flood-summary.json"), "w", encoding="utf-8") as handle:
    json.dump(total, handle, indent=2)
print(json.dumps({key: total[key] for key in ("clients",) + keys + ("by_status",)}, sort_keys=True))
PY
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
    [ -f driver-signing-key.pem ] || fail "no driver-signing-key.pem: that is the PRIVATE half of the signing pair, written by the same generator. The flood clients cannot mint a tenant token without it."
    [ -r driver-signing-key.pem ] || fail "driver-signing-key.pem is not readable: the clients run as root inside the container but this script checks it on the host."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx kafka \
        || fail "the kafka service is not running: 'make up' first. This beat reconciles against the broker after the flood."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx gateway \
        || fail "the gateway service is not running: 'make up' first."
    case ",$(sed -n 's/^GATEWAY_TENANTS=//p' .env | tr -d '"'"'" | tr ',' '\n' | grep . | tr '\n' ',' | sed 's/,$//')," in
        *",${FLOOD_TENANT},"*) ;;
        *) fail "${FLOOD_TENANT} is not in GATEWAY_TENANTS in .env, so the gateway would answer 403 for every flooded request and the beat would look like a limiter failure. Pick a tenant from that list, or set CHAOS_FLOOD_TENANT." ;;
    esac
    [ "$FLOOD_CLIENTS" -ge 1 ] || fail "CHAOS_FLOOD_CLIENTS must be at least 1. Note that 1 client cannot outrun a 2,000 events/sec bucket from a single thread; see the header."
    start_gateway
    # The budget the flood has to exhaust, read from the code that sets it and
    # inside the container, because importing app.main needs fastapi and this
    # script's host-side checks deliberately need nothing from the repository.
    info "the per-tenant budget the flood has to exhaust, from app/main.py:"
    $COMPOSE exec -T gateway "$PYTHON" -c \
        "from app.main import DEFAULT_TENANT_LIMIT as t; print(f'  {t.rate:g} events/sec sustained, burst {t.burst:g}; the whole batch is denied on the first event it cannot afford (CONTRACT.md section 3)')" \
        || fail "could not read DEFAULT_TENANT_LIMIT from the gateway container."
}

cleanup() {
    local pid
    for pid in ${DRIVER_PID:-} ${FLOOD_PIDS[@]+"${FLOOD_PIDS[@]}"}; do
        if [ -n "$pid" ] && kill -0 "$pid" 2> /dev/null; then
            kill "$pid" 2> /dev/null || true
        fi
    done
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

step "beat 6: one tenant floods; every other tenant must be provably unaffected"
info "flood tenant: $FLOOD_TENANT, $FLOOD_SESSIONS sessions over $FLOOD_USERS users"
info "sent by $FLOOD_CLIENTS concurrent clients, each owning a disjoint slice of those users"
info "catalogue: $TENANTS tenants; the baseline is $SESSIONS sessions"
preflight
reset_topics

step "offline check: the flood is a SEPARATE stream, so the baseline is not perturbed"
run_inject --sessions "$CORPUS_SESSIONS" --tenants "$TENANTS" --seed "$SEED" \
    --ledger "/app/chaos-flood-corpus-plain.jsonl" > "$WORK/inject-plain.json"
run_inject --sessions "$CORPUS_SESSIONS" --tenants "$TENANTS" --seed "$SEED" \
    --flood-tenant "$FLOOD_TENANT" --flood-sessions "$FLOOD_SESSIONS" \
    --flood-users "$FLOOD_USERS" \
    --ledger "/app/chaos-flood-corpus-flood.jsonl" > "$WORK/inject-flood.json"
info "baseline corpus: $(tr -d ' \n' < "$WORK/inject-plain.json")"
info "with the flood:  $(tr -d ' \n' < "$WORK/inject-flood.json")"
ledger_tenants "/app/chaos-flood-corpus-plain.jsonl" > "$WORK/tenants-plain.txt"
ledger_tenants "/app/chaos-flood-corpus-flood.jsonl" > "$WORK/tenants-flood.txt"
# The flood ledger is the baseline PLUS the flood stream, so it is not identical:
# the flooded tenant has more rows by exactly the flood's event count, and every
# other tenant has the same. A whole-file diff would fail on the one tenant that
# is supposed to differ, so the comparison states the exception.
"$PYTHON" - "$WORK/tenants-plain.txt" "$WORK/tenants-flood.txt" \
    "$WORK/inject-flood.json" "$FLOOD_TENANT" "$TENANTS" "$CORPUS_SESSIONS" <<'PY' > "$WORK/tenants.txt" \
    || fail "the baseline corpus changed when a flood was added. TenantFlood is a separate stream precisely so that this cannot happen; if it did, the other-499 claim below would be unfalsifiable."
import json
import sys


def counts(path):
    out = {}
    for line in open(path, encoding="utf-8"):
        name, count = line.split()
        out[name] = int(count)
    return out


plain, flooded = counts(sys.argv[1]), counts(sys.argv[2])
flood_events = json.load(open(sys.argv[3], encoding="utf-8"))["flood"]["events"]
tenant, expected = sys.argv[4], int(sys.argv[5])

drift = [
    f"{name}: {plain[name]} -> {flooded.get(name, 0)}"
    for name in sorted(plain)
    if name != tenant and flooded.get(name, 0) != plain[name]
]
if drift:
    for line in drift[:5]:
        print("  " + line)
    sys.exit(1)
appeared = [name for name in sorted(flooded) if name != tenant and name not in plain]
if appeared:
    print(f"  the flood introduced tenants the baseline did not have: {appeared[:5]}")
    sys.exit(1)
extra = flooded.get(tenant, 0) - plain.get(tenant, 0)
if extra != flood_events:
    sys.exit(
        f"{tenant} grew by {extra} events where the flood stream emitted {flood_events}: "
        "the flood is not the only thing that changed the corpus"
    )
if len(plain) != expected:
    sys.exit(
        f"the catalogue produced {len(plain)} tenants, not {expected}: at "
        f"{sys.argv[6]} sessions Zipf apportionment leaves part of the tail with no "
        "sessions at all, and the other-499 claim is only true of tenants that appear"
    )

print(f"tenants_compared {len(plain) - 1}")
print(f"baseline_events {sum(plain.values())}")
print(f"flood_events {flood_events}")
print(f"flood_tenant_rows {plain.get(tenant, 0)} -> {flooded.get(tenant, 0)}")
PY
cat "$WORK/tenants.txt"
info "every other tenant's event count is unchanged, and ${FLOOD_TENANT}'s grew by"
info "exactly the flood stream's event count: nothing else moved."

step "baseline load run over the whole catalogue ($SESSIONS sessions)"
if ! run_driver --sessions "$SESSIONS" --ledger "$IN_LEDGER"; then
    sed -n '/^{/,$p' "$WORK/driver.log" || true
    fail "the baseline run was not clean, so there is no clean 'before' to compare the flood against."
fi
sed -n '/^{/,$p' "$WORK/driver.log"
BEFORE_JSON="$WORK/verify-before.json"
BEFORE_STATUS=0
run_verify "$IN_LEDGER" "$BEFORE_JSON" --expect-dlq 0 --expect-duplicates 0 || BEFORE_STATUS=$?
if [ "$BEFORE_STATUS" -eq 2 ]; then
    cat "$WORK/verify.err" >&2
    fail "tools.verify exited 2 (UNVERIFIED) on the baseline run: it read nothing to reconcile."
fi
cat "$WORK/verify.err" >&2 || true
[ "$BEFORE_STATUS" -eq 0 ] || {
    "$PYTHON" -c 'import json,sys; [print("  - " + f) for f in json.load(open(sys.argv[1]))["failures"]]' "$BEFORE_JSON" >&2
    fail "tools.verify exited $BEFORE_STATUS on the clean baseline run with --expect-dlq 0 --expect-duplicates 0: there is no trustworthy 'before' to compare against."
}
DLQ_BEFORE="$(verify_json "$BEFORE_JSON" counts.dlq)"
STORED_BEFORE="$(verify_json "$BEFORE_JSON" counts.stored)"
info "baseline reconciled: $STORED_BEFORE events stored, $DLQ_BEFORE in the DLQ"
scrape_metrics "$WORK/metrics-before.txt"
DENIALS_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_rate_limit_denials_total)"
FOURXX_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_http_responses_total 4xx)"
LAG_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_consumer_lag)"
BUFFER_BEFORE="$(metric_total "$WORK/metrics-before.txt" gateway_buffer_items)"
BUFFER_MAX="$(metric_total "$WORK/metrics-before.txt" gateway_buffer_items_max)"
info "rate-limit denials $DENIALS_BEFORE, 4xx $FOURXX_BEFORE, consumer lag $LAG_BEFORE"
info "producer queue $BUFFER_BEFORE of $BUFFER_MAX records"

step "the flood: $FLOOD_TENANT alone, from $FLOOD_CLIENTS concurrent clients"
write_flood_program
local_shard=0
while [ "$local_shard" -lt "$FLOOD_CLIENTS" ]; do
    send_flood_client "$local_shard" &
    FLOOD_PIDS+=("$!")
    local_shard=$((local_shard + 1))
done
FLOOD_STATUS=0
for pid in "${FLOOD_PIDS[@]}"; do
    wait "$pid" || FLOOD_STATUS=$?
done
FLOOD_PIDS=()
sum_flood_receipts || fail "could not sum the flood clients' receipts; the per-client logs are in $WORK."
# Per client, from its own receipt, so one client doing all the shedding cannot
# hide behind the total.
for shard in $(seq 0 $((FLOOD_CLIENTS - 1))); do
    "$PYTHON" - "$WORK/flood-$shard.log" <<'PY'
import json
import sys

lines = open(sys.argv[1], encoding="utf-8", errors="replace").read().splitlines()
starts = [i for i, line in enumerate(lines) if line == "{"]
if not starts:
    print("  (this client printed no receipt)")
else:
    receipt = json.loads("\n".join(lines[starts[-1]:]))
    print(
        f"  sent {receipt['sent']}  accepted {receipt['accepted']}"
        f"  429s {receipt['rate_limited']}  unaccepted {receipt['unaccepted']}"
        f"  {json.dumps(receipt['by_status'], sort_keys=True)}"
    )
PY
done

FLOOD_SENT="$(verify_json "$WORK/flood-summary.json" sent)"
FLOOD_ACCEPTED="$(verify_json "$WORK/flood-summary.json" accepted)"
FLOOD_UNACCEPTED="$(verify_json "$WORK/flood-summary.json" unaccepted)"
FLOOD_LIMITED="$(verify_json "$WORK/flood-summary.json" rate_limited)"
FLOOD_BY_STATUS="$(verify_json "$WORK/flood-summary.json" by_status)"

# Scraped AFTER the flood, not during it: the denials this beat asserts on are the
# ones that happened over the whole flood, and a scrape taken two seconds in would
# under-count them by however much the rest of the run shed.
scrape_metrics "$WORK/metrics-after-flood.txt"
DENIALS_FLOOD="$(metric_total "$WORK/metrics-after-flood.txt" gateway_rate_limit_denials_total)"
FOURXX_FLOOD="$(metric_total "$WORK/metrics-after-flood.txt" gateway_http_responses_total 4xx)"
LAG_FLOOD="$(metric_total "$WORK/metrics-after-flood.txt" gateway_consumer_lag)"
BUFFER_FLOOD="$(metric_total "$WORK/metrics-after-flood.txt" gateway_buffer_items)"

info ""
info "the flood's receipts: sent $FLOOD_SENT, accepted $FLOOD_ACCEPTED, 429 responses $FLOOD_LIMITED"
info "unaccepted $FLOOD_UNACCEPTED (sent, never accounted for)"
info "refusals by the gateway's own reason code: $FLOOD_BY_STATUS"
info ""
info "gateway_rate_limit_denials_total: $DENIALS_BEFORE -> $DENIALS_FLOOD"
info "gateway_http_responses_total{4xx}:  $FOURXX_BEFORE -> $FOURXX_FLOOD"
info "gateway_consumer_lag:               $LAG_BEFORE -> $LAG_FLOOD"
info "producer queue:                     $BUFFER_FLOOD of $BUFFER_MAX records"
info "(gateway_buffer_utilisation is printed by 'make observe' and can read above 1.0;"
info " 1.0 is NOT the 503 point -- app/metrics.py says so in the metric's HELP text.)"
info "(gateway_consumer_lag is an INPUT here: the gateway produces and never consumes,"
info " so nothing in this stack sets it. 0 means no consumer group is being fed, not"
info " that lag is bounded. The backlog this beat CAN measure is the shed tenant's"
info " shortfall above, and the producer's own un-acked window.)"

# The clients each exit non-zero when they were refused anything, so a clean exit
# is the claim that must be proven false. Checked per client rather than from the
# total, because one client refusing everything and the rest succeeding would still
# sum to a non-zero count.
FLOOD_CLEAN_CLIENTS=0
for shard in $(seq 0 $((FLOOD_CLIENTS - 1))); do
    if grep -q '^"clean": true' "$WORK/flood-$shard.log"; then
        FLOOD_CLEAN_CLIENTS=$((FLOOD_CLEAN_CLIENTS + 1))
    fi
done
[ "$FLOOD_STATUS" -ne 0 ] \
    || fail "every flood client exited clean: ${FLOOD_SENT} events for one tenant all fitted inside its budget, so nothing was shed and this beat proved nothing. Raise CHAOS_FLOOD_SESSIONS."
[ "$FLOOD_CLEAN_CLIENTS" -eq 0 ] \
    || fail "$FLOOD_CLEAN_CLIENTS of $FLOOD_CLIENTS flood clients were never refused, so the shed rate did not come from the limiter."
[ "$FLOOD_LIMITED" -gt 0 ] \
    || fail "the flood produced no 429 at all: ${FLOOD_SENT} events at one tenant did not exhaust the per-tenant budget. Raise CHAOS_FLOOD_SESSIONS, or raise CHAOS_FLOOD_CLIENTS if the clients are not saturating the gateway."
[ "$DENIALS_FLOOD" -gt "$DENIALS_BEFORE" ] \
    || fail "gateway_rate_limit_denials_total did not rise during the flood, though the clients saw $FLOOD_LIMITED 429s: the counter and the behaviour have come apart."
[ "$FOURXX_FLOOD" -gt "$FOURXX_BEFORE" ] \
    || fail "gateway_http_responses_total{status_class=\"4xx\"} did not rise during the flood."
"$PYTHON" - "$WORK/flood-summary.json" <<'PY' \
    || fail "the flood's only refusal reason is not RATE_LIMITED: the flood tenant was refused for something other than its budget, so this was not a rate-limit shed."
import json
import sys

by_status = json.load(open(sys.argv[1], encoding="utf-8"))["by_status"]
if not by_status.get("RATE_LIMITED"):
    sys.exit("by_status was " + json.dumps(by_status, sort_keys=True))
for code in by_status:
    if code != "RATE_LIMITED":
        sys.exit(
            f"{by_status[code]} batch(es) were refused as {code} rather than RATE_LIMITED: "
            "the flood was refused for something other than its budget"
        )
PY

step "the clients' ledgers: every flood user sent exactly once"
CENSUS_JSON="$WORK/flood-census.json"
flood_ledger_census $(printf '/app/%s ' "${FLOOD_LEDGERS[@]}") > "$CENSUS_JSON" \
    || fail "could not census the flood clients' ledgers in the gateway container."
cat "$CENSUS_JSON"
CENSUS_ROWS="$(json_field "$CENSUS_JSON" rows)"
CENSUS_USERS="$(json_field "$CENSUS_JSON" users)"
[ "$CENSUS_ROWS" -eq "$FLOOD_SENT" ] \
    || fail "the flood ledgers hold $CENSUS_ROWS rows and the clients reported sending $FLOOD_SENT: the receipts and the ground truth disagree, so the shortfall below cannot be trusted."
[ "$CENSUS_USERS" -eq "$FLOOD_USERS" ] \
    || fail "the flood clients covered $CENSUS_USERS distinct users, not the $FLOOD_USERS the flood was built over: the clients' slices were not disjoint, or a slice was empty."

step "reconciling: baseline + flood against one ledger"
cat "$CHAOS_LEDGER" "${FLOOD_LEDGERS[@]}" > "$COMBINED_LEDGER"
info "ground truth is the baseline plus all $FLOOD_CLIENTS client ledgers, concatenated into $COMBINED_LEDGER"
AFTER_JSON="$WORK/verify-after.json"
AFTER_STATUS=0
run_verify "$IN_COMBINED_LEDGER" "$AFTER_JSON" --expect-duplicates 0 || AFTER_STATUS=$?
if [ "$AFTER_STATUS" -eq 2 ]; then
    cat "$WORK/verify.err" >&2
    fail "tools.verify exited 2 (UNVERIFIED): it read nothing to reconcile. Exit 2 is neither a pass nor a mismatch."
fi
cat "$WORK/verify.err" >&2 || true
[ "$AFTER_STATUS" -ne 0 ] \
    || fail "tools.verify reconciled clean after the flood, so the flood tenant was never shed. The per-tenant limiter did not fire and this beat proved nothing."

DLQ_AFTER="$(verify_json "$AFTER_JSON" counts.dlq)"
STORED_AFTER="$(verify_json "$AFTER_JSON" counts.stored)"
MISSING="$(verify_json "$AFTER_JSON" counts.missing)"
UNEXPECTED="$(verify_json "$AFTER_JSON" counts.unexpected)"
DUPLICATES="$(verify_json "$AFTER_JSON" counts.wire_duplicates)"
UNDECODABLE="$(verify_json "$AFTER_JSON" counts.undecodable)"
DRAINED="$(verify_json "$AFTER_JSON" counts.drained)"
PSEUDO_BAD="$(verify_json "$AFTER_JSON" counts.pseudonym_mismatches)"
VIOLATIONS="$(verify_json "$AFTER_JSON" counts.ordering_violations)"
SEQ_MISMATCH="$(verify_json "$AFTER_JSON" counts.seq_mismatches)"
LEDGER_DUPES="$(verify_json "$AFTER_JSON" counts.ledger_duplicates)"

info "stored $STORED_AFTER (was $STORED_BEFORE)   dlq $DLQ_AFTER (was $DLQ_BEFORE)"
info "missing $MISSING   unexpected $UNEXPECTED   wire duplicates $DUPLICATES   undecodable $UNDECODABLE"
info "ordering violations $VIOLATIONS   sequence mismatches $SEQ_MISMATCH   ledger duplicate rows $LEDGER_DUPES"
info "pseudonym mismatches $PSEUDO_BAD"
[ "$DLQ_AFTER" -eq "$DLQ_BEFORE" ] \
    || fail "the DLQ went from $DLQ_BEFORE to $DLQ_AFTER records during the flood: a 429 must not produce a DLQ entry (CONTRACT.md section 3)."
[ "$UNEXPECTED" -eq 0 ] || fail "$UNEXPECTED stored (source, id) pair(s) are absent from the ground truth: the flood produced events the clients never recorded."
[ "$DUPLICATES" -eq 0 ] || fail "$DUPLICATES wire duplicate(s) after (source, id) dedup: a retry is only idempotent because ids are stable, and this scenario causes no resends, so there should be none."
[ "$LEDGER_DUPES" -eq 0 ] || fail "the ground truth holds $LEDGER_DUPES duplicate (source, id) rows, so it cannot describe a run in which each event was sent once."
[ "$UNDECODABLE" -eq 0 ] || fail "$UNDECODABLE topic message(s) would not decode, so the comparison rests on records it could not read."
[ "$DRAINED" = "True" ] || fail "the topic read stopped short of its end offsets, so this is a truncated view and not a reconciliation."
[ "$VIOLATIONS" -eq 0 ] \
    || fail "$VIOLATIONS event(s) arrived with a sequence lower than one already seen for their (source, user): splitting the flood across clients must not have reordered any user's journey, and it did."
[ "$SEQ_MISMATCH" -eq 0 ] || fail "$SEQ_MISMATCH stored sequence value(s) disagree with the ledger."
[ "$PSEUDO_BAD" != "None" ] \
    || fail "the pseudonym check was SKIPPED: no MASTER_SECRET in the gateway container, so nothing proved the stored user_id_pseudo is an HMAC of the ledger's user_id."
[ "$PSEUDO_BAD" = "0" ] || fail "$PSEUDO_BAD stored user_id_pseudo value(s) are not the expected HMAC of the ledger's user_id"
[ "$MISSING" -eq "$FLOOD_UNACCEPTED" ] \
    || fail "reconciliation reports $MISSING missing and the clients' receipts say ${FLOOD_UNACCEPTED} events went unaccounted for. Every shed batch is in the ground truth and on no topic, so the two numbers must be equal; anything else is something lost without being refused."

step "the other tenants: identical with and without the flood"
"$PYTHON" - "$BEFORE_JSON" "$AFTER_JSON" "$FLOOD_TENANT" "$TENANTS" "$FLOOD_UNACCEPTED" <<'PY' > "$WORK/isolation.txt" \
    || fail "the flood changed what other tenants have on career.events.raw. The per-tenant claim is that they did not."
import json
import sys

before, after = json.load(open(sys.argv[1])), json.load(open(sys.argv[2]))
flood, expected_tenants, flood_unaccepted = sys.argv[3], int(sys.argv[4]), int(sys.argv[5])


def rows(document):
    # `key` is the CloudEvents source, "/careers/<tenant>"; the ledger's tenant
    # field is the id, and so is the driver's tenant naming. The per-group field
    # is `rejected`, not `dlq`: that is what tools.verify's _group_dict emits, and
    # reading a name it does not publish is a KeyError on a real run rather than a
    # wrong answer here.
    return {row["key"].rsplit("/", 1)[-1]: row for row in document["by_source"]}


a, b = rows(before), rows(after)
drift = []
for name in sorted(set(a) | set(b)):
    if name == flood:
        continue
    first, second = a.get(name), b.get(name)
    if first is None or second is None:
        drift.append(f"{name}: present before={first is not None} after={second is not None}")
    elif (first["sent"], first["stored"], first["rejected"], first["delta"], first["unresolved"]) != (
        second["sent"], second["stored"], second["rejected"], second["delta"], second["unresolved"]
    ):
        drift.append(f"{name}: {first} -> {second}")

compared = len([name for name in set(a) | set(b) if name != flood])
if drift:
    for line in drift[:5]:
        print("  " + line)
    if len(drift) > 5:
        print(f"  ... and {len(drift) - 5} more")
    sys.exit(1)

flooded = b.get(flood)
if flooded is None:
    sys.exit(f"{flood} has no row in the after-reconciliation at all")
if flooded["stored"] <= 0:
    sys.exit(f"{flood} stored {flooded['stored']} events: the flood was shed from the first batch, which is a different scenario")
if flooded["rejected"] != 0:
    sys.exit(f"{flood} has {flooded['rejected']} DLQ entries: a 429 must not produce one")
if flooded["delta"] >= 0:
    sys.exit(f"{flood} has delta {flooded['delta']}: nothing was shed, so this is not a flood")
if flooded["sent"] - flooded["stored"] != flood_unaccepted:
    sys.exit(
        f"{flood} sent {flooded['sent']}, stored {flooded['stored']}, and the clients reported "
        f"{flood_unaccepted} unaccepted: the shortfall is not accounted for"
    )
if compared != expected_tenants - 1:
    sys.exit(
        f"compared {compared} tenants, expected {expected_tenants - 1}: at this session count "
        "Zipf apportionment leaves part of the tail with no sessions, so 'the other 499' is "
        "not yet true of this run. Raise CHAOS_SESSIONS."
    )

print(f"tenants_compared {compared}")
print(f"tenants_with_zero_drift {compared - len(drift)}")
print(f"flood_sent {flooded['sent']}")
print(f"flood_stored {flooded['stored']}")
print(f"flood_dlq {flooded['rejected']}")
print(f"flood_delta {flooded['delta']}")
print(f"flood_unresolved {flooded['unresolved']}")
PY
cat "$WORK/isolation.txt"
info ""
info "every one of the tenants outside the flood has exactly the same sent and stored count"
info "on career.events.raw before and after. The flood's own tenant kept $FLOOD_ACCEPTED of $FLOOD_SENT."

printf '\n== PASS: %s was shed with 429 (%s denials, %s of %s events kept), its shortfall\n' \
    "$FLOOD_TENANT" "$((DENIALS_FLOOD - DENIALS_BEFORE))" "$FLOOD_ACCEPTED" "$FLOOD_SENT"
printf '   accounted for exactly, the DLQ stayed at %s, and every other tenant is unchanged.\n' \
    "$DLQ_AFTER"
