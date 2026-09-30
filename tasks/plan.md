# Implementation Plan: Event Generator (Ingestion Gateway + Synthetic Driver)

**Spec of record:** `SPEC.txt`
**Scope:** Team slice #2 of 4 — **Event Generator**. Excludes UI, Queue/Kafka+Flink, Database/Cassandra.
**Revision:** **r4** — demo posture settled; hardware answered; measured costs folded in.
**Status:** Building. 219 tests green across 8 tasks. See §5.

---

## 0. Revision log

| Rev | Change |
|---|---|
| r1 | Initial plan (Go proposed) |
| r2 | Language set to **Python**; batching, multi-process, generate-and-replay |
| r3 | Research validation. Missed CloudEvents (E1); errors corrected (E2-E4); improvements adopted (I1-I7) |
| r3.1 | Self-consistency pass. Gaps closed: event types (G1), 64 KiB vs base64 (G2), Java-client-vs-librdkafka (G3) |
| r3.2 | Independent review. 4 Critical + 9 High + 13 Medium. Root cause of the Criticals: per-process mechanisms claimed as system-wide properties. Fixed via sticky routing, honest contracts, pipeline ordering |
| **r4** | **Demo posture settled (design-justified 50k, not demonstrated). Hardware answered. Measured per-event cost and JWT throughput folded in — both invalidate earlier estimates.** |

### 0.1 What changed in r4

**The demo does not demonstrate 50k/sec. It demonstrates a working system and explains the architecture that would reach 50k.** This is the organising principle from here on.

Consequences:

| Area | Before | Now |
|---|---|---|
| **T1 spike** | P0, blocked everything, decided the fallback ladder | **Reduced.** A load client, not a benchmark. The 6-core laptop stops being a risk. |
| **T9 peak 50k** | P0, 10-min soak, prove the number | **Reduced.** Correct + documented producer config, and a modest load run to show it holds. |
| **T10a / T10b metrics** | P1, T10b first on the cut list | **Promoted.** A live view *is* the demo. |
| **T11 chaos + verify** | P1 | **The centrepiece.** Resilience is demonstrable in 3 minutes and needs no throughput. |
| **Fallback ladder** | Renegotiate / sidecar / more workers | **Moot.** Removed. |

**Hardware answered** (was Q1): demo runs **only** on a local MSI laptop — **Intel i7-9750H, 6C/12T, 16 GB RAM, Windows**. See §3.

**Two estimates in r3 were wrong, and measurement proved it:**
- Per-event cost: budgeted **23.5 µs**, actually **~42 µs for the crypto facade alone** (T6). Total realistic **~55 µs**. Root cause: budgeted 1 AES + 1 HMAC when there are **5 AES + 2 HMAC**, and omitted encode and AAD construction.
- JWT: EdDSA measured at **5,563 req/s** on this box, RS256 at **8,759 req/s** (T4). This **inverts** r3.2's "EdDSA preferred" ordering *for this hardware*. Both are implemented; it is a config choice.

---

## 1. Demo posture — what we are actually showing

The demo is a **3-minute narrative**, not a benchmark.

| # | Beat | What it proves | Status |
|---|---|---|---|
| 1 | Contract slide — CloudEvents, single topic, derived key, `(source,id)` dedup | we designed a contract, not a firehose | `CONTRACT.md` done |
| 2 | A live candidate session flows through | it works end to end | FSM + pipeline |
| 3 | Raw vs encrypted side by side | PII never leaves in the clear | crypto done, inspector pending |
| 4 | **Kill the gateway → zero loss, here's the receipt** | ground truth, not vibes | ledger done, chaos pending |
| 5 | **Inject 5% garbage → DLQ catches exactly 5%** | failures are contained and counted | DLQ done, knob pending |
| 6 | **One tenant floods → shed that one, 499 unaffected** | multi-tenant isolation | token bucket done |
| 7 | **Explain the 50k decisions** | the architecture argument | this doc |

**Every beat is falsifiable and none needs 50k/sec.** That is a stronger demo than a number on a
screen, because a judge can interrupt any of it.

### 1.1 How we talk about 50k — and the honesty rule

We claim 50k is **achievable by design**, and we back it with arithmetic. Two rules:

1. **Show the measured per-event cost, not just the conclusion.** ~55 µs/event measured (T6) → 50k
   needs ~2.75 core-seconds/sec of logic, so ~4-6 cores. Stating the cost builds credibility;
   hiding it invites a judge to find it.
2. **Do not claim it is demonstrated.** It is not, on this hardware, with the full stack. Say so
   plainly if asked. "Designed for 50k, here are the seven decisions that get us there, and here is
   the measured cost per event" is defensible. "We do 50k" would not be.

### 1.2 The seven decisions that constitute the 50k argument

| Decision | Effect |
|---|---|
| **Batching** (1..500 events/request) | the load-bearing one. 50k events = ~1k req/s at batch 50. At batch 1 the required request rate is unreachable in Python. |
| **msgspec** for parse+validate in one native pass | `jsonschema` alone would be 5-10x over budget |
| **Purpose-separated keys + cached AESGCM handles** | 3x faster than building a handle per field (measured) |
| **Sticky routing** | one producer per user ⇒ Kafka ordering actually holds |
| **Single topic, no re-serialisation hop** | one encode, not two |
| **zstd + large batches on the producer** | biggest single wire-side win on JSON |
| **Replay-mode driver** | generation cost removed from the load path |

---

## 2. Architecture decisions (unchanged from r3.2 unless noted)

Full text in the prior revision's §2. Summary of the load-bearing ones:

- **D1 — CloudEvents 1.0.3 envelope.** CNCF Graduated; required by `SPEC.txt:131`. `id`+`source` is the
  dedup rule. `application/cloudevents-batch+json` is the batch media type. Extension names are
  lowercase `[a-z0-9]`, no underscores. `data` holds PII; context attributes never do.
- **D2 — Batching mandatory; batches are single-tenant; caps 500 events / 4 MiB enforced before
  allocation.** Single-tenant is what makes isolation enforceable *and* buys one JWT verify per batch.
- **D3 — Single topic `career.events.raw`.** Per-event-type topics (the original `SPEC.txt:133`) break
  sessionization. `SPEC.txt` amended. Kafka key **derived** by the gateway, never client-supplied.
- **D4 — Asymmetric JWT; tenant from the signed claim only.** **r4: prefer RS256 over EdDSA on this
  hardware** (8,759 vs 5,563 req/s measured). H2's actual argument — a shared secret is a 500-tenant
  compromise — is unaffected by the choice.
- **D5 — Encrypt PII, emit HMAC pseudonyms, two purpose-separated keys, AAD-bound ciphertext.**
- **D6 — Pipeline order is load-bearing: decode → auth → validate → encrypt → produce.** The DLQ
  carries the **post-encryption** event.
- **D8 — Ground-truth ledger** is what turns "it worked" into a receipt.
- **D12 — Sticky routing.** One `(tenant, user)` → one worker → one producer. Makes the ordering
  guarantee *true* and the per-tenant rate limit *accurate*.

---

## 3. Hardware and platform — the real risk

**Demo machine: MSI laptop, Intel i7-9750H (6C/12T, 2.6 GHz base), 16 GB RAM, GTX 1660 Ti, Windows.**

### 3.1 The number nobody wants to hear

Effective sustained compute is roughly **5-5.5 cores** once Windows, Docker and thermal throttling take
their cut. The full stack must share them:

| Configuration | Cores for gateway | Realistic events/sec |
|---|---|---|
| Full stack (Kafka + Flink + Cassandra + gateway + driver) | ~0.5-1.0 | **5k-15k** |
| Peak config (Kafka + gateway + driver) | ~3.0-4.0 | **25k-40k** |
| Gateway alone, no broker | ~4.5 | 35k-50k |

**50k is not demonstrable with the full stack on this machine.** Under the r4 demo posture this is
acceptable — we are not demonstrating it. It is recorded here so the design write-up is honest.

### 3.2 The blocker that has NOT gone away

**Cassandra and Flink have no native Windows support.** Both teams are forced into WSL2 or Docker
Desktop, so the whole stack shares those 6 cores with Windows. This is a **day-1 risk for two of the
three other teams** and must be proven working before demo day, not on it.

**Memory is tighter than cores.** 16 GB total; Windows takes 3-4 GB; **WSL2 defaults to a 50% cap ≈
8 GB** for Kafka + Flink + Cassandra + gateway + driver. That does not fit. Raise the cap:

```ini
# %USERPROFILE%\.wslconfig
[wsl2]
memory=11GB
processors=6
swap=4GB
```

Also note 6-8 uvicorn workers is memory-hungry (~150-250 MB each). **If memory-bound, trade worker
count for batch size, not the reverse.**

### 3.3 Platform consequences for our code

- uvicorn `--workers` uses **spawn** on Windows — slower startup, higher per-worker memory than fork.
- Plan says demo on Linux/Docker. **Everything runs inside WSL2/Ubuntu.** Dev on Windows is fine;
  the running system is not.

---

## 4. Task list — status

Detail and acceptance criteria in `tasks/todo.md`.

### Built (219 tests green)

| # | Task | Commit | Status |
|---|---|---|---|
| T0 | scaffold | `4ce45c7` | done |
| T2 | CloudEvents contract, ledger schema, `SPEC.txt` amendment | `4ce45c7` | done |
| T4 | asymmetric auth, JWT-only tenant binding | `096298f` | done (50 tests) |
| T5a | validation + post-encryption DLQ | `bd49eca` | done |
| T6 | crypto: key separation, registry, AAD | `6559943` | done (37 tests) |
| T7 | per-tenant token bucket | `365add2` | done (31 tests) |
| T8a | driver funnel FSM + corpus | `26e2d49` | done (38 tests) |
| — | integration fixes from worker review | `a40bf0a` | done |

### In flight

| # | Task | Files |
|---|---|---|
| T5b | ingest pipeline, handler, limits, decrypt | `app/ingest/**` |
| T8b | 500-tenant Zipfian skew, sharding, webhook source | `driver/{tenants,skew,webhook_source}.py` |
| T10a | metrics registry | `app/metrics.py` |

### Remaining

| # | Task | Files | Priority |
|---|---|---|---|
| T3 | FastAPI app, Kafka producer sink, compose | `app/main.py`, `app/kafka/**`, `docker-compose.yml`, `driver/replay.py` | **P0** |
| T11 | verification oracle + chaos scripts | `tools/verify.py`, `scripts/chaos/**` | **P0 — the demo** |
| T10b | live CLI view, inspector, `Makefile` | `tools/observe.py`, `app/inspect.py`, `Makefile` | **P0 — the demo** |
| T1r | load client (not a benchmark) | `bench/load.py` | P1 |
| T9r | producer-config doc + measured A/B numbers | `docs/kafka-producer-tuning.md` | P1 |
| T8c | injection knobs: `--inject-invalid-rate`, tenant flood | `driver/inject.py` | P1 |

**Revised estimate: ~18-24h** (was 55-69h). The reframe is most of the saving.

---

## 5. Risks

| Risk | Impact | Mitigation |
|---|---|---|
| **WSL2/Docker + Kafka + Cassandra + Flink not working on demo day** | **Critical — sinks two other teams too** | Prove it **this week**, not on demo day. Raise the WSL2 memory cap. Owner: all four teams. |
| **Measured per-event cost (~55 µs) is double the r3 estimate** | **High** — invalidates any throughput claim | Recorded in §1.1. State it honestly in the design write-up. T6 also removed a 3x handle-construction cost. |
| **Ordering guarantee silently broken** | **High** — wrong drop-off metrics, no error | D12 sticky routing; `sequence` is authoritative; ordering-violation counter in T10a; a test that fails without sticky routing. |
| **`enable.idempotence` disabled by someone chasing throughput** | **High** — silent duplicates and reordering | Named a prohibition in `CONTRACT.md`; effective config asserted at startup. |
| **Rate limits silently N× under multi-process** | **High** | Correct only because of D12. T7 has a regression test that fails without it. |
| **DLQ becomes a plaintext PII store** | **High** | Pipeline order fixed (D6); DLQ carries the post-encryption event; canary tests, not regexes. |
| Driver and gateway contend for cores | Medium | Separate containers with CPU limits; driver reports *sent*, gateway reports *accepted*. |
| Spec contradiction (per-event-type topics) | Medium | `SPEC.txt:133` amended as a T2 deliverable. |
| Flink 30-min watermark vs demo duration | Medium | Driver supports accelerated session time. |
| *(DB team)* `application_funnel_sessions` PK will exceed 100 MB on a popular job | Medium for DB | `SPEC.txt:149` bucketing advice was not applied. Warn them — our Zipfian traffic triggers it. |

---

## 6. Not building

- Flink / stream processing, recommendation engine, vector DB — Queue team
- Cassandra access or writes — DB team
- Autofill DOM SDK — UI team
- A throughput benchmark harness (T1 reduced to a load client)
- k6 / Gatling / Locust
- CloudEvents binary content mode (incompatible with batching)
- Exactly-once end-to-end (at-least-once + `(source,id)` LWW)
- An admin UI

---

## 7. Open questions

1. **Is 50k still the number we claim in the design, or has that softened?** Under the r4 posture we
   explain 50k as a target. If it is now aspirational, we could trim the crypto hot path and spend
   the time on the chaos demos. *Affects whether T9r is a doc or a project.*
2. **Who owns the `.wslconfig` change and the WSL2/Docker smoke test?** It gates three teams.
3. **Kafka version (3.x or 4.x)?** §D11 sets every value explicitly regardless, but the durability
   story differs: on a single broker, ISR is 1, so `acks=all` is cheap but weaker.

---

## 8. Handoffs owed

| Artifact | To | When |
|---|---|---|
| `CONTRACT.md` v0.1 — CloudEvents profile, derived key, honest ordering contract, `(source,id)` dedup, caps, `202`≠durable, rate-limit semantics, ledger schema | Queue + DB + **UI** | done — `813c8e9`, `a40bf0a` |
| Kafka producer settings + measured A/B | Queue | T9r |
| Measured per-event cost breakdown | Everyone | done — T6 |
| PII key-derivation + pseudonymous-twin semantics | DB | done |
| Client SDK batching, caps, keep-alive, `sequence` maintenance | **UI** | done — `CONTRACT.md` §9 |
