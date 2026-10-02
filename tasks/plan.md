# Implementation Plan: Event Generator (Ingestion Gateway + Synthetic Driver)

**Spec of record:** `SPEC.txt`
**Scope:** Team slice #2 of 4 — **Event Generator only**. Excludes UI, Queue/Kafka+Flink, Database/Cassandra.
The repo's `README.md` is a whole-platform synthesis and is **out of scope** for this slice except where it
restates `contracts/CONTRACT.md`, which remains binding.
**Revision:** **r5** — replanned from built reality, not from r4 estimates.
**Status:** Draft for human review. Plan mode — no code changed to produce this document.

**What changed since r4 / r4.1:** the r4 task list still shows T5b, T8b, T10a "in flight" and T3 "not
started." All four are **done and committed** (`8f80b2a`, `1dcb2c5`, `67d1d24`, `bc2fc83`; T8c also done
in `a157f63`). This plan takes the built tree as ground truth and plans only what remains. `tasks/todo.md`
is updated in place for the same work (per the skill's same-work rule — the user explicitly asked for a
replan). `tasks/plan-siddharth.md` is a stale untracked r4 copy and is **not** a source of truth.

---

## Overview

The Event Generator is the pipeline's front door: a FastAPI ingestion gateway that takes CloudEvents
batches over HTTP, binds them to a tenant from an asymmetric JWT, validates, encrypts PII, rate-limits,
and produces to one Kafka topic — plus the synthetic driver that proves it works. The gateway path
`decode → auth → validate → encrypt → produce` is built, tested (~420 tests), and committed. What remains
is entirely **"does the demo run"**: commit one pending producer fix, add the driver's replay entry point,
then build the verification oracle, chaos scripts, live view, load client, and tuning doc that turn a
tested library into a six-beat falsifiable demo. No throughput claim is made or demonstrated (Q1 closed).

---

## Architecture Decisions (standing — not revisited)

- **D1 — CloudEvents 1.0 envelope, two schemas.** Ingress (`contracts/ingress.py`, plaintext, what a
  client POSTs) and egress (`contracts/cloudevent.py`, encrypted, what lands on Kafka). Both generated to
  schema files and staleness-tested. Rationale: a client cannot hold a tenant key, so one struct for both
  directions meant no client could post anything at all.
- **D2 — Batching as API design; caps as DoS defence.** 1..500 events, 4 MiB body, enforced before
  allocation. Single-tenant batches (one bearer token per request) make isolation enforceable and buy one
  JWT verify per batch.
- **D3 — Single topic `career.events.raw`; gateway-derived key `<career_site_id>|<user_id_pseudo>`.**
  Per-event-type topics break sessionization. Client `partitionkey` is ignored; disagreement is `403`.
- **D4 — Asymmetric JWT (RS256 preferred on this hardware), tenant from the signed claim only.**
- **D5 — Encrypt PII, emit HMAC pseudonyms, two purpose-separated HKDF keys, AAD-bound ciphertext.**
- **D6 — Pipeline order is load-bearing: decode → auth → validate → encrypt → produce.** DLQ carries the
  post-encryption event.
- **D8 — Ground-truth ledger** `(id, source, type, tenant, user_pseudo, seq)`, one schema in
  `contracts/ledger.py`. Turns "it worked" into a receipt.
- **D12 — Sticky routing.** One `(tenant, user)` → one worker → one producer. Makes ordering true and the
  per-tenant rate limit accurate.
- **Honest `202`.** Accepted-into-buffer, not a durability receipt. `503` on full buffer / dead broker.
  `enable.idempotence=false` is forbidden (silent duplicates, not an error).

---

## Dependency Graph (Event Generator internals only)

```
contracts/{cloudevent,ingress,attributes,ledger} + app/config  (DONE, committed)
        │
        ├── app/auth ──┐
        ├── app/validate + app/dlq ──┤  (all DONE)
        ├── app/crypto + app/pseudonym ┤
        └── app/ratelimit ──┘
                │
                ▼
        app/ingest (pipeline/handler/limits/decrypt)  (DONE 8f80b2a)
                │
                ├── app/metrics  (DONE 67d1d24)
                └── app/kafka (DONE bc2fc83 + 1 uncommitted fix)
                        │
                        ▼
                app/main + docker-compose  (DONE bc2fc83)
                        │
        driver/{fsm,corpus,sources,tenants,skew,webhook,inject}  (DONE)
                        │
                        ▼
        driver replay/main → tools/verify → chaos scripts → observe/inspect → bench/load
                        (REMAINING — this plan)
```

Cross-team edges (we publish, they consume): `CONTRACT.md` + both schemas + examples → UI/Queue/DB;
effective Kafka producer config → Queue; key-derivation + twin semantics → DB. No code dependency on
their trees.

---

## Task List

### Phase A — Close the running system

#### R1: Commit the producer DLQ-count fix + process-exit guard
- [ ] `app/kafka/producer.py` uncommitted change (remove `record_dlq_published` from the sink; the
  `InstrumentedSink` wrapper owns that counter — counting in both places doubles every DLQ event) is
  committed as its own fix with a test pinning single-counting
- [ ] Producer thread is daemon **and** joined on lifespan shutdown; a test spawns the app and asserts
  the process exits (the teardown-hang class found during T3 verification)
- [ ] Full suite green serially after the commit

#### R2: Driver replay + main entry point
- [ ] `driver/replay.py` + `driver/main.py`: read the pre-generated corpus, POST single-tenant batches
  within the 500-event / 4 MiB caps, keep-alive on, reuse stable `id` on retry
- [ ] Every emitted event appended to the ledger in the T2 schema; no `application-abandoned` emitted
- [ ] `POST /v1/ingest` accepts a full driver run end to end against the composed app (FakeSink first,
  then the real compose stack)

### Checkpoint: System runs
- [ ] `docker compose up` → gateway healthy, topics created, driver run completes against it
- [ ] Ledger `sent == accepted` on a clean run; full suite green
- [ ] Review with human before building proof tooling

### Phase B — Proof (the demo beats)

#### R3: Verification oracle
- [ ] `tools/verify.py` reconciles ledger vs Kafka offsets (and Cassandra counts where reachable):
  per-`source` / per-`type` deltas, duplicate `(source, id)` count, DLQ count vs injected count
- [ ] Exit non-zero with a one-screen scorecard: sent / accepted / stored / duplicates / DLQ
- [ ] Duplicate assertion scoped to "after `(source,id)` dedup" (Chaos 1 produces wire duplicates by construction)

#### R4: Chaos scripts + demo runbook
- [ ] `scripts/chaos/`: kill-gateway (bounded, reported loss window — never "zero"), broker-down
  (`503`, fails loudly, recovers), 5%-invalid (DLQ catches exactly 5%, main topic intact, no plaintext
  PII in DLQ), tenant-flood (shed one tenant, 499 unaffected, lag reported)
- [ ] `docs/demo-runbook.md`: the six beats in order, timed, runnable from a fresh clone

### Checkpoint: Proof works
- [ ] Each chaos scenario run end to end; `verify` output inspected and correct
- [ ] Full demo script runs inside the time limit
- [ ] Review with human before polish

### Phase C — Show and hand off

#### R5: Live view + inspector + `make demo`
- [ ] `tools/observe.py`: zero-dependency live events/sec + error rate from `/metrics`
- [ ] `app/inspect.py`: auth-gated raw-vs-encrypted side-by-side for a sampled event
- [ ] `Makefile`: `up`, `demo`, `observe`, `verify`, `chaos`, `test` — `make demo` works from a clean checkout

#### R6: Honest load client + producer tuning doc
- [ ] `bench/load.py`: configurable-rate replay client reporting achieved events/sec, p50/p99, error
  breakdown, machine spec attached. Explicitly **not** a benchmark; never prints a number shaped like a
  50k claim
- [ ] `docs/kafka-producer-tuning.md`: every D11 value explicit with rationale; effective-config dump
  procedure; `enable.idempotence=false` prohibition; librdkafka-vs-Java-defaults caveat

#### R7: Contract and handoff close-out
- [ ] `CONTRACT.md` §1 ingress/egress table matches the two generated schemas; section cross-refs
  renumbered correctly; UI obligations (§9: batching, keep-alive, `sequence` maintenance, stable `id`)
  current
- [ ] Handoff bundle listed: schemas + examples, effective producer config, key-derivation doc,
  ledger schema. Nothing in this task changes gateway behavior.

### Checkpoint: Complete (FINAL)
- [ ] All tests pass; no spike code; no uncommitted fixes
- [ ] `make demo` + chaos script run clean from a fresh clone
- [ ] Reconciliation proves zero loss, zero duplicate `(source, id)`, ordering holds
- [ ] Ready for review

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| WSL2/Docker + Kafka not working on demo day | High — demo's only single point of failure | Prove `docker compose up` + driver run this week, not on demo day; `.wslconfig` memory bump documented in compose file |
| Producer teardown hang regresses | High — process never exits, suite stalls | R1 pins daemon + join + process-exit test |
| DLQ double-counting ships | Medium — every DLQ metric wrong forever | R1 pins single-counting at the wrapper |
| Chaos 1 over-claims zero loss | Medium — contradicts our own `202` semantics | R4 asserts a bounded, reported window, never zero |
| Scope creep into Queue/DB/UI trees | Medium | This plan's file lists stop at our slice; README's platform sections are explicitly out of scope |
| Stale plan copies diverge (`plan-siddharth.md`) | Low | This revision names it stale; delete or ignore it after human confirms |

## Open Questions

- Confirm `tasks/plan-siddharth.md` (stale untracked r4 copy) may be deleted after this plan is approved.
- Confirm the six demo beats are still the demo (contract slide · live session · raw-vs-encrypted · kill-gateway receipt · 5% DLQ · tenant flood).
- Kafka version 3.x vs 4.x for the demo broker (durability story differs on a single broker; config is explicit either way).

## Parallelization Opportunities

- **Safe to parallelize:** R3 (verify) and R5 (observe/inspect) after R2 lands — disjoint files, both read-only consumers of the running system.
- **Must be sequential:** R1 before anything that measures (counters change); R2 before R3/R4 (nothing to verify without replay); R4 before the final checkpoint (demo script depends on chaos).
- **Needs coordination:** R6's tuning doc with the Queue team (effective config values); R7's contract text with the UI team (client obligations).

## Verification (skill checklist)

- [x] Every task has acceptance criteria
- [x] Every task has a verification step
- [x] Task dependencies are identified and ordered correctly
- [x] Tasks are recorded in the task list target (`tasks/todo.md`)
- [x] No pre-existing incomplete plan overwritten without confirmation — same work, explicitly requested replan; stale copy named, not silently replaced
- [x] No task touches more than ~5 files
- [x] Checkpoints exist between major phases
- [ ] The human has reviewed and approved the plan — **PENDING**
