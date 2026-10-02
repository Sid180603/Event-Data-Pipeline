# Task List: Event Generator

Plan: `tasks/plan.md` · Spec: `SPEC.txt` · **Revision r5** (replanned from built reality)
**Scope: Event Generator only.** The repo `README.md` is whole-platform synthesis and out of scope.
**P0** = required for the demo · **P1** = high value · **P2** = cut if time-boxed
**Never cut: R1, R2, R3, R4, R5.**

**Demo posture (unchanged from r4):** six falsifiable beats, no throughput claim. Beats: contract slide ·
live session · raw-vs-encrypted · kill-gateway receipt · 5% DLQ · tenant flood.

**Environment:** running system lives in **WSL2/Ubuntu**. Dev on Windows is fine. Gateway and driver in
**separate containers with CPU limits**.

**Status legend:** ✅ done · 🔄 in flight · ⬜ not started

---

## Progress (ground truth from git log)

| # | Task | Status | Commit |
|---|---|---|---|
| T0 | scaffold | ✅ | `4ce45c7` |
| T2 | CloudEvents contract + ledger + spec amendment | ✅ | `4ce45c7` |
| T4 | asymmetric auth, JWT-only tenant binding | ✅ | `096298f` |
| T5a | validation + post-encryption DLQ | ✅ | `bd49eca` |
| T6 | crypto: key separation, registry, AAD | ✅ | `6559943` |
| T7 | per-tenant token bucket | ✅ | `365add2` |
| T8a | driver funnel FSM + corpus | ✅ | `26e2d49` |
| — | integration fixes from worker review | ✅ | `a40bf0a` |
| T5b | ingest pipeline, handler, limits, decrypt | ✅ | `8f80b2a` |
| T8b | 500-tenant skew, sharding, webhook source | ✅ | `1dcb2c5` |
| T10a | metrics registry | ✅ | `67d1d24` |
| T8c | fault injection (exact bad-event count + flood) | ✅ | `a157f63` |
| — | driver invariant pins (LSP + two-pass docs) | ✅ | `f987cff` |
| T3 | application factory, Kafka sink, compose stack | ✅ | `bc2fc83` |
| R1 | producer DLQ-count fix + exit guard | ✅ | `6e749f9` |
| R2 | driver replay + main entry | ✅ | `67c0678` |
| R3 | verification oracle | ✅ | `6ef3002` |
| R5 | live view + inspector + `make demo` | ✅ | `7d297bf` |
| R6 | load client + producer tuning doc | ✅ | `efd3bd1` |
| R4 | chaos scripts + demo runbook | 🔄 | in flight |
| R7 | contract and handoff close-out | ✅ | `d8c0758`, `df01029` |

### Defects found while building, and fixed

Every row here was found by running something, not by reading it. None was in the plan.

| Defect | Consequence had it shipped | Fix |
|---|---|---|
| `app/config.py` published `compression.zstd.level`, which is not a librdkafka property | `Producer()` raises `_INVALID_ARG`; **the gateway could not start at all** | `6f0320e` |
| `tools/observe.py` had no `__main__` guard | `make observe` and `make demo`'s live screen printed **nothing and exited 0** — a blank demo beat reporting success | `003ca4a` |
| `contracts/event.schema.json` shipped a bare `$ref` with no `$defs` | the published egress schema **validated nothing** for the Queue and DB teams | `167a1a9` |
| `app/dlq/envelope.py`, `app/ingest/` and `CONTRACT.md` claimed the DLQ is never plaintext | false: a **validation-stage** rejection is DLQ'd as plaintext, deliberately. A PII claim three teams build retention policy on | `108410b`, `4f9b9ab`, `d8c0758` |
| the driver cut batches by cap only, so the credential's channel was stamped on every event in a request | the demo's per-event `sourcechannel` was fiction | `a4a1789` |
| `httpx` was a dev-only dependency | `import httpx` fails in the compose driver container, which installs only `[project].dependencies` | `a4a1789` |
| no `.gitattributes` | CRLF checkout; every `.sh` and the `Makefile` fail in WSL2 with `bad interpreter` | `14b26df` |
| the hot-path cost assertion fired on contention, not regression | flaky red builds | `6f02d27`, `dac133a` |
| `app/config.py` and `app/ingest/` still described `INGRESS_EVENT_BYTES` as provisional and `MAX_EVENT_BYTES` as unreachable | both false once measured | `d8c0758`, `4f9b9ab` |

### Open items for the team — not for an agent

1. **The top 297 B of the 48 KiB ingress cap is a dead zone.** Measured: an event whose
   bulk sits in the five encrypted PII fields inflates past the 64 KiB ceiling (49,152 B
   ingress → 65,923 B) and is refused per-event to the DLQ. The largest that fits is
   48,855 B. `INGRESS_EVENT_BYTES` was deliberately **not** changed — it is a published
   contract value and lowering it changes what three teams may send. Practical rule
   meanwhile: keep the encrypted fields small.
2. **`STICKY_ROUTING` is read by nothing.** `Settings.sticky_routing` parses the env var
   and no code path consults it; there is no affinity layer in front of uvicorn's
   `--workers`. A tenant's effective rate budget is therefore up to N× the configured
   rate. Partitioning is unaffected (the Kafka key is derived) and the flood beat's
   "the other 499 are unaffected" still holds, but no claim about the *exact* enforced
   rate is supportable. Closing this needs an L7 balancer and is outside this slice.
   Corrected in `923abd1` and in `CONTRACT.md` §6.
3. **Unverified on real infrastructure.** No Docker, broker or WSL distro on the build
   machine. `make demo`, `make chaos` and every reconciliation have never been executed
   end to end. The chaos scripts were exercised only with a stubbed `docker`.
4. **The demo has never been run in front of anyone.** Everything above is a component
   being correct, not a demo being reliable.

---

## Phase A — Close the running system

### Task R1: Commit the producer DLQ-count fix + process-exit guard

**Description:** Land the one uncommitted producer change as its own fix and pin the teardown behavior
the T3 verification caught: the process must actually exit.

**Acceptance criteria:**
- [ ] `record_dlq_published` is counted exactly once per DLQ record, at the `InstrumentedSink` wrapper —
  not in `KafkaSink._succeeded`
- [ ] Producer thread is daemon **and** joined on lifespan shutdown
- [ ] A test spawns the app and asserts the process exits

**Verification:**
- [ ] Tests pass: `python -m pytest app/kafka app/test_app.py -q`
- [ ] Full suite green serially: `python -m pytest -q`
- [ ] Manual check: test process terminates; no hang on exit

**Dependencies:** T3 (committed as `bc2fc83`)

**Files likely touched:**
- `app/kafka/producer.py`
- `app/kafka/test_producer.py`
- `app/test_app.py`

**Estimated scope:** Small: 2-3 files

### Task R2: Driver replay + main entry point

**Description:** Give the driver the entry point the demo drives load through: read the pre-generated
corpus, POST single-tenant batches, keep-alive on, ledger every emission.

**Acceptance criteria:**
- [ ] `driver/replay.py` + `driver/main.py` replay corpus batches within the 500-event / 4 MiB caps
- [ ] HTTP keep-alive on; stable `id` reused on retry
- [ ] Every emitted event appended to the ledger in the T2 schema
- [ ] No `application-abandoned` emitted (Queue's Flink owns it)
- [ ] A full driver run is accepted by `POST /v1/ingest` end to end (FakeSink first, then compose stack)

**Verification:**
- [ ] Tests pass: `python -m pytest driver -q`
- [ ] Full suite green serially
- [ ] Manual check: `docker compose up` → driver run completes; ledger `sent == accepted` on a clean run

**Dependencies:** R1, T3, T8a–T8c

**Files likely touched:**
- `driver/replay.py`
- `driver/main.py`
- `driver/test_replay.py`

**Estimated scope:** Small: 2-3 files

## Checkpoint: System runs
- [ ] `docker compose up` → gateway healthy, topics created, driver run completes
- [ ] Ledger `sent == accepted` on a clean run; full suite green
- [ ] Review with human before building proof tooling

---

## Phase B — Proof (the demo beats)

### Task R3: Verification oracle

**Description:** Turn ground truth into one command: reconcile ledger vs Kafka offsets (and Cassandra
counts where reachable) and print the demo scorecard.

**Acceptance criteria:**
- [ ] `tools/verify.py` reports per-`source` / per-`type` deltas, duplicate `(source, id)` count, DLQ
  count vs injected count
- [ ] Duplicate assertion scoped to "after `(source,id)` dedup" (Chaos 1 produces wire duplicates by construction)
- [ ] Exit non-zero on mismatch; one-screen scorecard: sent / accepted / stored / duplicates / DLQ

**Verification:**
- [ ] Tests pass: `python -m pytest tools -q`
- [ ] Full suite green serially
- [ ] Manual check: clean run reconciles to zero; a seeded-duplicate run reports exactly

**Dependencies:** R2

**Files likely touched:**
- `tools/verify.py`
- `tools/test_verify.py`

**Estimated scope:** Small: 2 files

### Task R4: Chaos scripts + demo runbook

**Description:** The four scripted failures plus the timed runbook that performs the six demo beats.

**Acceptance criteria:**
- [ ] `scripts/chaos/`: kill-gateway (bounded, reported loss window — never "zero"), broker-down (`503`,
  fails loudly, recovers), 5%-invalid (DLQ catches exactly 5%, main topic intact, no plaintext PII in
  DLQ), tenant-flood (shed one tenant, 499 unaffected, lag reported)
- [ ] `docs/demo-runbook.md`: six beats in order, timed, runnable from a fresh clone
- [ ] Each script is runnable standalone and from the runbook

**Verification:**
- [ ] Tests pass where unit-testable; otherwise each scenario run end to end with `verify` output inspected
- [ ] Full suite green serially
- [ ] Manual check: full demo script runs inside the time limit

**Dependencies:** R2, R3

**Files likely touched:**
- `scripts/chaos/kill_gateway.sh`
- `scripts/chaos/broker_down.sh`
- `scripts/chaos/bad_events.sh`
- `scripts/chaos/tenant_flood.sh`
- `docs/demo-runbook.md`

**Estimated scope:** Medium: 4-5 files

## Checkpoint: Proof works
- [ ] Each chaos scenario run end to end; `verify` output inspected and correct
- [ ] Full demo script runs inside the time limit
- [ ] Review with human before polish

---

## Phase C — Show and hand off

### Task R5: Live view + inspector + `make demo`

**Description:** The screen the demo is watched on, the side-by-side PII proof, and the single entry point.

**Acceptance criteria:**
- [ ] `tools/observe.py`: zero-dependency live events/sec + error rate from `/metrics`
- [ ] `app/inspect.py`: auth-gated raw-vs-encrypted side-by-side for a sampled event
- [ ] `Makefile` targets: `up`, `demo`, `observe`, `verify`, `chaos`, `test`; `make demo` works from a
  clean checkout

**Verification:**
- [ ] Tests pass: `python -m pytest tools app/test_app.py -q`
- [ ] Full suite green serially
- [ ] Manual check: `make demo` from a clean checkout; live view matches the load run

**Dependencies:** R2 (needs the running system); T10a metrics already committed

**Files likely touched:**
- `tools/observe.py`
- `tools/test_observe.py`
- `app/inspect.py`
- `Makefile`

**Estimated scope:** Medium: 3-4 files

### Task R6: Honest load client + producer tuning doc

**Description:** A small load client that shows the gateway holding a rate without claiming a benchmark,
plus the documented producer configuration the Queue team builds on.

**Acceptance criteria:**
- [ ] `bench/load.py`: configurable-rate replay, reports achieved events/sec, p50/p99, error breakdown,
  machine spec attached; explicitly **not** a benchmark
- [ ] `docs/kafka-producer-tuning.md`: every D11 value explicit with rationale; effective-config dump
  procedure; `enable.idempotence=false` prohibition; librdkafka-vs-Java-defaults caveat
- [ ] Load client testable without a broker (stub sink arithmetic covered)

**Verification:**
- [ ] Tests pass: `python -m pytest bench -q`
- [ ] Full suite green serially
- [ ] Manual check: short run against the real gateway produces a plausible figure

**Dependencies:** R2

**Files likely touched:**
- `bench/load.py`
- `bench/test_load.py`
- `docs/kafka-producer-tuning.md`

**Estimated scope:** Small: 2-3 files

### Task R7: Contract and handoff close-out

**Description:** Close the contract text and list the handoff bundle. Docs only — no gateway behavior
changes in this task.

**Acceptance criteria:**
- [ ] `CONTRACT.md` §1 ingress/egress table matches both generated schemas; section cross-refs correct
- [ ] UI obligations current (batching, keep-alive, `sequence` maintenance, stable `id`)
- [ ] Handoff bundle listed: schemas + examples, effective producer config, key-derivation doc, ledger schema

**Verification:**
- [ ] Tests pass: `python -m pytest contracts -q` (schema staleness guards)
- [ ] Manual check: Queue/DB/UI leads can build from the bundle alone

**Dependencies:** R6 (producer doc feeds the bundle)

**Files likely touched:**
- `contracts/CONTRACT.md`
- `docs/handoff.md` (only if a new file is needed; prefer editing CONTRACT.md)

**Estimated scope:** Small: 1-2 files

## Checkpoint: Complete (FINAL)
- [ ] All tests pass; no spike code; no uncommitted fixes
- [ ] `make demo` + chaos script run clean from a fresh clone, inside the time limit
- [ ] End-to-end reconciliation: zero loss, zero duplicate `(source, id)`, ordering holds
- [ ] Ready for review

---

## Notes on superseded items (not deleted, just retired)

- Old T1 spike / T1r benchmark framing, 10-minute soak, seven-decision scale argument: retired at r4
  because Q1 closed with no 50k claim. Kept here as a record so nobody re-adds them.
- Old phase/numbering (T5b/T8b/T10a "in flight," T3 "not started") described pre-commit state and is
  superseded by the Progress table above.
- `tasks/plan-siddharth.md`: stale untracked r4 copy. Not a source of truth; delete after human confirms.
- `README.md`: whole-platform synthesis (Queue/DB/UI included). Out of scope for this slice except
  `contracts/CONTRACT.md` precedence.
