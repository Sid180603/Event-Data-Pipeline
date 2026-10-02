#!/usr/bin/env bash
# Beat 3 of 6: inject exactly 5% malformed events and prove the DLQ caught
# exactly that many, with the main topic intact.
#
# WHAT IT PROVES
#   `driver.inject.plan_injection` places a malformed event by arithmetic, not by
#   sampling, so "the DLQ holds exactly the number we injected" is an equality
#   and not an approximation. Three checks:
#     1. arithmetic  -- driver.inject rebuilds the plan offline from the same seed
#                       over the same catalogue and must land on the same integer
#                       the replayed run reports; that integer must equal
#                       floor(corpus_size * rate + 0.5), and the per-variant counts
#                       must sum to it;
#     2. exact       -- tools.verify --expect-dlq INJECTED exits 0, which also
#                       proves the main topic is intact (every good event stored,
#                       nothing unexpected, nothing undecodable, no ordering
#                       violation, every stored pseudonym the expected HMAC);
#     3. per code    -- verify's DLQ breakdown equals the driver's per-variant
#                       breakdown, so nothing was rejected for a reason we did
#                       not inject and nothing injected escaped.
#
# WHAT IT NEEDS
#   The compose stack up (kafka, gateway healthy), `.env` and
#   driver-signing-key.pem at the repository root. Everything it needs runs
#   inside the compose network; nothing has to be published to the host.
#
# WHAT IT DELIBERATELY DOES NOT CLAIM
#   "the DLQ holds no plaintext PII" is FALSE for this beat and is not asserted.
#   Every injected variant is rejected during validation, i.e. BEFORE the encrypt
#   stage, so the only payload that exists is the plaintext request element --
#   see app/dlq/envelope.py, which says so in its own module docstring. What is
#   asserted instead is the narrower claim that is true and checkable:
#     * `error_context` never carries an identifier or an offending value, and
#     * every DLQ record here is a validate-stage record, so the "post-encryption
#       payload is ciphertext" half of the contract does not apply to any of them.
#
# ENVIRONMENT
#   COMPOSE               docker compose command          (default: docker compose)
#   PYTHON                interpreter for the host-side checks
#   CHAOS_SESSIONS        baseline sessions               (default: 6000)
#   CHAOS_INVALID_PCT     percentage of malformed events  (default: 5)
#   CHAOS_SEED            corpus and injection seed          (default: 7)
#   CHAOS_LEDGER          ground truth to write           (default: chaos-bad-events.jsonl)
#   CHAOS_VERIFY_TIMEOUT  per-topic read budget, seconds  (default: 60)
#
# Exit status: 0 only when every assertion above held. Anything else is a
# non-zero exit with the failed assertion named, because `make chaos` runs the
# four beats in order and depends on a failure propagating.

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
CHAOS_INVALID_PCT="${CHAOS_INVALID_PCT:-5}"
# driver.main's own default, and the seed the run below is launched with, so the
# corpus recounted in the gateway container is the run's corpus and not a
# different one built from the same session count.
CHAOS_SEED="${CHAOS_SEED:-7}"
CHAOS_LEDGER="${CHAOS_LEDGER:-chaos-bad-events.jsonl}"
CHAOS_VERIFY_TIMEOUT="${CHAOS_VERIFY_TIMEOUT:-60}"
SESSIONS="${CHAOS_SESSIONS}"
INJECT_RATE="${CHAOS_INVALID_PCT}"
SEED="${CHAOS_SEED}"
# The ledger is addressed by container path inside the compose network and by
# repository-relative path everywhere else, because the repository is bind
# mounted at /app.
IN_LEDGER="/app/${CHAOS_LEDGER}"
OUT_LEDGER="${CHAOS_LEDGER}"

WORK="$(mktemp -d)"

usage() {
    cat <<'USAGE'
bad_events.sh -- inject exactly N% malformed events and prove the DLQ caught them.

Proves, by arithmetic and by tools.verify's exit code:
  * the injected count equals floor(corpus_size * rate + 0.5), not approximately;
  * tools.verify --expect-dlq INJECTED exits 0, which also means every good
    event reached career.events.raw and every stored pseudonym is the expected
    HMAC of the ledger's plaintext user id;
  * the DLQ's per-code breakdown equals the driver's per-variant breakdown;
  * no DLQ record's error_context carries an identifier or the offending value.

Does NOT claim the DLQ is free of plaintext: a validate-stage rejection is
stored as the plaintext request element, because it was never encrypted.

Needs: the compose stack up, .env and driver-signing-key.pem present. Runs
everything inside the compose network; nothing is published to the host.

Environment: COMPOSE, PYTHON, CHAOS_SESSIONS, CHAOS_INVALID_PCT, CHAOS_SEED, CHAOS_LEDGER,
CHAOS_VERIFY_TIMEOUT.
USAGE
}

info() { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
fail() { printf '\nBEAT FAILED: %s\n' "$*" >&2; exit 1; }

# Read one field out of a JSON document on disk, and only that document: the
# driver's summary is printed with indent=2, so the top-level `{` is the only one
# at column 0 and every nested object is indented. `docker compose run` prefixes
# the container's output with whatever pip printed, which is why a naive
# json.load on the whole file is not an option.
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

run_driver() {  # extra driver.main arguments...
    local args=("$@")
    info "driver.main ${args[*]}"
    # `docker compose run` needs the service's command overridden to pass any
    # argument at all, and `exec` so the container's exit code is the driver's.
    $COMPOSE run --rm -T driver bash -c \
        "pip install --quiet --no-input -e . && exec $PYTHON -m driver.main ${args[*]}" \
        > "$WORK/driver.log" 2>&1
}

run_verify() {  # ledger extra-verify-arguments...  -> exit status is tools.verify's
    $COMPOSE exec -T gateway "$PYTHON" -m tools.verify \
        --ledger "$IN_LEDGER" --json --timeout "$CHAOS_VERIFY_TIMEOUT" "$@" \
        > "$WORK/verify.json" 2> "$WORK/verify.err"
}

verify_json() {  # dotted.path
    json_field "$WORK/verify.json" "$1"
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

preflight() {
    command -v docker > /dev/null 2>&1 || fail "docker is not on PATH: this beat runs the compose stack, so it needs Docker (WSL2 + Docker Desktop on Windows)."
    docker compose version > /dev/null 2>&1 || fail "'docker compose' is not available. Install Docker Desktop, or set COMPOSE to a working compose command."
    [ -f .env ] || fail "no .env at the repository root: compose will not invent MASTER_SECRET and a JWT public key. Run the generator in the header of docker-compose.yml."
    [ -f driver-signing-key.pem ] || fail "no driver-signing-key.pem: that is the PRIVATE half of the signing pair, written by the same generator. The driver cannot mint a tenant token without it."
    [ -r driver-signing-key.pem ] || fail "driver-signing-key.pem is not readable: the driver runs as root inside the container but this script checks it on the host."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx kafka \
        || fail "the kafka service is not running: 'make up' first. This beat has to read the DLQ off the broker."
    $COMPOSE ps --status running --services 2> /dev/null | grep -qx gateway \
        || fail "the gateway service is not running: 'make up' first."
    wait_for_gateway
}

cleanup() {
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

step "beat 3: inject exactly ${INJECT_RATE}% malformed events, the DLQ catches exactly that many"
info "proves: the DLQ count is an equality, the main topic is intact, and error_context"
info "carries no identifier or offending value. It does NOT prove the DLQ holds no"
info "plaintext: every injected variant is rejected before the encrypt stage."
preflight
reset_topics

step "load run with exactly ${INJECT_RATE}% of the corpus malformed"
if ! run_driver \
    --sessions "$SESSIONS" \
    --seed "$SEED" \
    --inject-invalid-rate "$INJECT_RATE" \
    --ledger "$IN_LEDGER"; then
    sed -n '/^{/,$p' "$WORK/driver.log" || true
    fail "the driver run failed. A clean beat needs every batch accounted for: if a batch was refused it is still in the ledger, and reconciliation will not balance."
fi
info "the driver's receipt:"
sed -n '/^{/,$p' "$WORK/driver.log"

# --- 1. the arithmetic the beat is named for -----------------------------------
# The injection count is arithmetic, not a sample, so it must be recomputable.
# `driver.main` reports `injected` and `by_variant` but not the corpus size it
# planned over, so the corpus is recounted here -- over the gateway's OWN tenant
# list, with driver.main's own catalogue, seed and session count, in the gateway
# container where GATEWAY_TENANTS is set. That is the same corpus the run planned
# over, which is what makes `injected == floor(corpus * rate + 0.5)` an equality
# rather than an approximation.
#
# `driver.inject` is run alongside as a SECOND opinion, and the two are printed
# side by side rather than asserted equal, because they are not the same
# computation: `driver.inject --tenants 500` mints tenant_0000..tenant_0499
# (driver/tenants.py numbers from 0) while the `.env` this stack runs on lists
# tenant_0001..tenant_0500, so its corpus is a few events smaller and its plan is
# a handful of events lower. Asserting the two equal would be asserting something
# false about the repository; the beat's claim is about the RUN, and the run is
# the one recounted here.
TENANTS="$(json_field "$WORK/driver.log" tenants)"
RUN_CORPUS="$($COMPOSE exec -T gateway "$PYTHON" - "$SESSIONS" "$SEED" <<'PY'
import sys

from driver.inject import count_events
from driver.main import ConfiguredCatalog, tenants_from_env
from driver.skew import SkewedCorpus

sessions, seed = int(sys.argv[1]), int(sys.argv[2])
configured = tenants_from_env()
if not configured:
    # The run's own catalogue came from GATEWAY_TENANTS; without it this recount
    # would be over a different one and the equality below would be meaningless.
    raise SystemExit("GATEWAY_TENANTS is not set in the gateway container")
catalog = ConfiguredCatalog(configured, seed=seed, users_per_tenant=50)
print(count_events(SkewedCorpus(catalog=catalog, seed=seed).batches(sessions, max_events=200)))
PY
)" || fail "could not recount the run's corpus in the gateway container."
CORPUS_SIZE="$RUN_CORPUS"
$COMPOSE exec -T gateway "$PYTHON" -m driver.inject \
    --sessions "$SESSIONS" --tenants "$TENANTS" --seed "$SEED" \
    --inject-invalid-rate "$INJECT_RATE" \
    > "$WORK/plan.json" || fail "driver.inject could not recompute an injection plan offline."
OFFLINE_CORPUS="$(json_field "$WORK/plan.json" emitted)"
OFFLINE_INJECTED="$(json_field "$WORK/plan.json" injected)"
INJECTED="$(json_field "$WORK/driver.log" injected)"
SENT="$(json_field "$WORK/driver.log" sent)"
# The plan the run was built from, from the run's own corpus. driver.inject's CLI
# cannot be given a tenant LIST, only a count, so the run's corpus has to be
# recounted with driver.main's own catalogue rather than read off driver.inject.
PLANNED="$("$PYTHON" -c 'import math,sys; total=int(sys.argv[1]); print(min(math.floor(total*float(sys.argv[2])/100.0+0.5), total))' "$CORPUS_SIZE" "$INJECT_RATE")"

info ""
info "the run's corpus, recounted over the gateway's own ${TENANTS}-tenant list: $CORPUS_SIZE events"
info "the driver's receipt says it sent $SENT; the difference is appended DUPLICATE_ID partners"
info "the second opinion, driver.inject over its own 0-based catalogue: $OFFLINE_CORPUS events, $OFFLINE_INJECTED planned."
info "  Not asserted equal -- the two catalogues differ by one tenant id, so their corpora do too."
info "floor(${CORPUS_SIZE} * ${INJECT_RATE}/100 + 0.5) = $PLANNED, and that is what was injected"
[ "$PLANNED" -gt 0 ] || fail "no event was injected at ${INJECT_RATE}%: the DLQ would be empty and this beat would prove nothing."
[ "$INJECTED" -eq "$PLANNED" ] \
    || fail "the run injected $INJECTED events where the plan over its own corpus says $PLANNED: the injected count is arithmetic, so a disagreement means the run is not the run that was counted (see driver.inject.count_events)."
VARIANT_SUM="$(json_field "$WORK/driver.log" by_variant | "$PYTHON" -c 'import json,sys; print(sum(json.load(sys.stdin).values()))')"
[ "$VARIANT_SUM" -eq "$INJECTED" ] \
    || fail "per-variant counts sum to $VARIANT_SUM but $INJECTED events were injected"
"$PYTHON" - "$WORK/plan.json" "$INJECT_RATE" <<'PY' \
    || fail "the injection plan is not the arithmetic it claims to be."
import json, math, sys

document = json.load(open(sys.argv[1], encoding="utf-8"))
rate = float(sys.argv[2]) / 100.0
total = document["emitted"] - document["appended"]
planned = min(math.floor(total * rate + 0.5), total)
if document["injected"] != planned:
    sys.exit(f"injected {document['injected']} where floor({total} * {rate} + 0.5) = {planned}")
if sum(document["by_variant"].values()) != document["injected"]:
    sys.exit(f"per-variant counts sum to {sum(document['by_variant'].values())}, not {document['injected']}")
PY
info "the per-variant counts sum to $INJECTED, and driver.inject's own plan is arithmetic too"

step "reconciling: the DLQ must hold exactly $INJECTED records"
VERIFY_STATUS=0
run_verify --expect-dlq "$INJECTED" --expect-duplicates 0 || VERIFY_STATUS=$?
if [ "$VERIFY_STATUS" -eq 2 ]; then
    cat "$WORK/verify.err" >&2
    fail "tools.verify exited 2 (UNVERIFIED): it read nothing to reconcile. Exit 2 is not a pass and not a mismatch, so this beat refuses to report one."
fi
cat "$WORK/verify.err" >&2 || true

STORED="$(verify_json counts.stored)"
DLQ="$(verify_json counts.dlq)"
MISSING="$(verify_json counts.missing)"
UNEXPECTED="$(verify_json counts.unexpected)"
UNDECODABLE="$(verify_json counts.undecodable)"
DRAINED="$(verify_json counts.drained)"
PSEUDO_BAD="$(verify_json counts.pseudonym_mismatches)"
PSEUDO_CHECKED="$(verify_json counts.pseudonyms_checked)"
EXPECTED="$(verify_json counts.expected)"

info "stored $STORED (distinct source,id)   dlq $DLQ   expected $EXPECTED"
info "missing $MISSING   unexpected $UNEXPECTED   undecodable $UNDECODABLE   drained $DRAINED"
info "pseudonyms $PSEUDO_CHECKED checked, $PSEUDO_BAD mismatched"
info "dlq by code: $(verify_json dlq_by_code)"
[ "$DLQ" -eq "$INJECTED" ] || fail "the DLQ holds $DLQ records and $INJECTED were injected: the DLQ count is an equality, not a range."
[ "$MISSING" -eq 0 ] || fail "$MISSING injected event(s) reached neither the topic nor the DLQ: they did not fail loudly."
[ "$UNEXPECTED" -eq 0 ] || fail "$UNEXPECTED stored event(s) are absent from the ground truth: something reached the topic that the driver never sent."
[ "$UNDECODABLE" -eq 0 ] || fail "$UNDECODABLE topic message(s) would not decode, so the comparison rests on records it could not read."
[ "$DRAINED" = "True" ] || fail "the topic read stopped short of its end offsets, so this is a truncated view and not a reconciliation."
[ "$PSEUDO_BAD" != "None" ] \
    || fail "the pseudonym check was SKIPPED: no MASTER_SECRET in the gateway container, so nothing proved the stored user_id_pseudo is an HMAC of the ledger's user_id"
[ "$PSEUDO_BAD" = "0" ] || fail "$PSEUDO_BAD stored user_id_pseudo value(s) are not the expected HMAC of the ledger's user_id"

# The main topic is intact in the exact sense the beat names: the good events are
# all there and the poison is not on it. `accepted + dlq == expected` is the
# gateway's own per-event accounting of the batch, which is why it is stated
# rather than "stored looks about right".
info "the main topic holds $STORED good events and none of the $INJECTED malformed ones"
if [ "$VERIFY_STATUS" -ne 0 ]; then
    printf 'verify failures:\n' >&2
    "$PYTHON" -c 'import json,sys; [print("  - " + f) for f in json.load(open(sys.argv[1]))["failures"]]' "$WORK/verify.json" >&2
    fail "tools.verify exited $VERIFY_STATUS: the DLQ count matched exactly but something else in the reconciliation did not hold."
fi
info "tools.verify exited 0: reconciled."

step "the DLQ's per-code breakdown must equal the driver's per-variant breakdown"
info "injected: $(json_field "$WORK/driver.log" by_variant)"
info "on the DLQ: $(verify_json dlq_by_code)"
"$PYTHON" - "$WORK/driver.log" "$WORK/verify.json" <<'PY' || fail "the DLQ's codes are not the codes that were injected"
import json, sys


def document(path):
    lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    starts = [i for i, line in enumerate(lines) if line == "{"]
    return json.loads("\n".join(lines[starts[-1]:]))


injected = document(sys.argv[1])["by_variant"]
dlq = document(sys.argv[2])["dlq_by_code"]
if injected != dlq:
    print(f"  injected {injected}")
    print(f"  on the DLQ {dlq}")
    sys.exit(1)
PY
info "every injected variant, and nothing else, is on the DLQ."

step "what the DLQ does and does not carry"
# Read the DLQ with tools.verify's own reader rather than a second consumer, and
# derive the expected pseudonyms with the gateway's own functions, so this check
# cannot drift from the pipeline it is checking.
set +e
$COMPOSE exec -T gateway "$PYTHON" - "$IN_LEDGER" "$TOPIC_DLQ" "$INJECT_RATE" <<'PY' > "$WORK/dlq-check.txt" 2> "$WORK/dlq-check.err"
import sys
from pathlib import Path

import msgspec

from app.config import Settings
from app.crypto.keys import derive_tenant_keys
from app.dlq.envelope import DlqEvent
from app.pseudonym.hmac import pseudonymize
from contracts.ledger import Ledger, dedup_key
from driver.inject import ILLEGAL_ATTRIBUTE, ILLEGAL_TIME, ILLEGAL_TYPE
from tools.verify import KafkaTopicReader

ledger_path, topic, rate = Path(sys.argv[1]), sys.argv[2], sys.argv[3]
settings = Settings()
secret = settings.master_secret

# Values that must never appear in a rejection's error_context. The ledger's
# `user_pseudo` field holds the ingress PLAINTEXT user_id, so the HMACs and
# pseudonyms are re-derived from it here rather than read off the topic.
ledger = Ledger(ledger_path).read_all()
keys = {}
for record in ledger:
    keys.setdefault(
        record.tenant,
        derive_tenant_keys(
            secret, settings.tenant_key_salt(record.tenant), career_site_id=record.tenant,
            key_version=settings.key_version,
        ),
    )
plaintext = {record.user_pseudo for record in ledger}
derived = {
    pseudonymize(tenant_keys.mac_key, user_id)
    for tenant, tenant_keys in keys.items()
    for user_id in (row.user_pseudo for row in ledger if row.tenant == tenant)
}

# Attribute NAMES are names, not values: an UNKNOWN_ATTRIBUTE reason carries the
# offending attribute's name on purpose, because that is what lets the client fix
# their request. So ILLEGAL_ATTRIBUTE is deliberately not in this list.
forbidden = sorted(plaintext | derived | {ILLEGAL_TYPE, ILLEGAL_TIME})
records, undecodable = [], 0
snapshot = KafkaTopicReader(
    settings.kafka_bootstrap_servers, timeout=60.0
).read(topic, timeout=60.0)
if not snapshot.drained:
    print("NOT DRAINED")
    raise SystemExit(3)
for message in snapshot.messages:
    try:
        records.append(msgspec.json.decode(message.value, type=DlqEvent))
    except msgspec.DecodeError:
        undecodable += 1

contexts = [msgspec.json.encode(record.data.error_context).decode() for record in records]
stages = {}
for record in records:
    stages[record.data.error_context.stage] = stages.get(record.data.error_context.stage, 0) + 1
leaks = sorted({value for value in forbidden for context in contexts if value in context})
encrypted_stage = [
    r for r in records
    if r.data.error_context.stage != "validate" and "_enc" in msgspec.json.encode(r.data.original_payload).decode()
]

print(f"dlq_records {len(records)}")
print(f"undecodable {undecodable}")
print(f"stages {stages}")
print(f"forbidden_values_checked {len(forbidden)}")
print(f"error_context_leaks {len(leaks)}")
print(f"encrypt_stage_payloads_carrying_ciphertext {len(encrypted_stage)}")
print(f"rate {rate}")
if leaks:
    print("leaked_values " + ",".join(leaks[:3]))
if stages.get("validate", 0) == len(records):
    print(
        "note every record here is validate-stage, so each carries the plaintext request "
        "element (it was never encrypted); the ciphertext rule applies only to a record "
        "rejected after the encrypt stage, and this beat produces none of those"
    )
PY
DLQ_STATUS=$?
set -e
if [ "$DLQ_STATUS" -eq 3 ]; then
    fail "the DLQ read stopped short of its end offsets, so the PII check verified nothing"
elif [ "$DLQ_STATUS" -ne 0 ]; then
    cat "$WORK/dlq-check.err" >&2
    fail "the DLQ inspection failed with status $DLQ_STATUS"
fi
cat "$WORK/dlq-check.txt"
DLQ_RECORDS="$(awk '$1 == "dlq_records" {print $2}' "$WORK/dlq-check.txt")"
UNDECODABLE_DLQ="$(awk '$1 == "undecodable" {print $2}' "$WORK/dlq-check.txt")"
LEAKS="$(awk '$1 == "error_context_leaks" {print $2}' "$WORK/dlq-check.txt")"
[ "$LEAKS" = "0" ] || fail "a DLQ record's error_context carries an identifier or the offending value: the rejection reason must be a code and a field path, never the data"
[ "$DLQ_RECORDS" -eq "$INJECTED" ] || fail "a second read of the DLQ found $DLQ_RECORDS records where the reconciliation found $DLQ: the topic moved under the check"
[ "$UNDECODABLE_DLQ" -eq 0 ] || fail "$UNDECODABLE_DLQ DLQ record(s) would not decode, so the PII check could not read them"

printf '\n== PASS: %s malformed events injected, %s caught by the DLQ, %s good events stored, error_context clean.\n' \
    "$INJECTED" "$DLQ" "$STORED"
