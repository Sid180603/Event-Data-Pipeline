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
| 4 | **Kill the gateway → nothing lost, here's the receipt** | ground truth, not vibes | ledger done, chaos pending |
| 5 | **Inject 5% garbage → DLQ catches exactly 5%** | failures are contained and counted | DLQ done, knob pending |
| 6 | **One tenant floods → shed that one, 499 unaffected** | multi-tenant isolation | token bucket done |

**Every beat is falsifiable and none needs a throughput claim.** The demo is six beats long and its
job is to work. A scale question, if a judge asks it, gets a short honest answer from §1.2 — it is not
a beat and we do not prepare it.

### 1.1 How we talk about scale — one paragraph, not an argument

**The 50k figure is not a claim we make.** The demo does not demonstrate it and does not assert it. If
a judge asks "how would this scale?", the answer is the paragraph below and nothing more. We spend no
further time on it.

> The gateway is a synchronous ingest path whose cost is linear in events, not in tenants: batching
> (1..500 events per request), `msgspec` for parse-and-validate in one native pass, a single Kafka
> topic with no re-serialisation hop, and sticky routing so one user's events go through one producer.
> Measured per-event cost is ~55 µs, of which ~42 µs is the crypto facade (five AES-GCM fields plus
> two HMACs). Tenant count does not multiply cost — buckets and keys are per-tenant maps.

**What this changes in our work:**

- **No further optimisation toward a throughput target.** The code is correct and tested; that is
  the bar now. Latency micro-optimisation and hot-path work are explicitly out of scope.
- **Batching is no longer justified by 50k.** It stands on its own as ordinary good API design —
  it is what every analytics ingest API does. We describe it that way rather than as a requirement.
- **The architecture does not depend on the 50k claim.** CloudEvents, tenant isolation, per-tenant
  keys, AAD-bound ciphertext, the DLQ, sticky routing — all of these are *correctness and security*
  decisions that stand whether or not the throughput number exists.

### 1.2 What the design still rests on

Worth being precise about, because it is the part that does not move: the load-bearing decisions are
**correctness and security**, not speed. Tenant isolation, no-PII-in-transit, deterministic dedup,
bounded loss on failure, per-tenant fairness. Those are what a reviewer — or a judge — will actually
probe, and all of them are built and tested.


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

### Remaining — ordered by "does the demo run"

| # | Task | Files | Priority |
|---|---|---|---|
| **T3** | FastAPI app, Kafka producer sink, compose — **the whole demo depends on this** | `app/main.py`, `app/kafka/**`, `docker-compose.yml`, `driver/replay.py` | **P0 — top** |
| **T11** | verification oracle + chaos scripts (demo beats 4, 5) | `tools/verify.py`, `scripts/chaos/**` | **P0** |
| **T10b** | live CLI view, inspector, `Makefile` (demo beats 2, 3) | `tools/observe.py`, `app/inspect.py`, `Makefile` | **P0** |
| **T8c** | injection knobs: `--inject-invalid-rate`, tenant flood (beats 5, 6) | `driver/inject.py` | **P0 — promoted** |
| T1r | load client — small, just to show it holds under load | `bench/load.py` | P1 |
| T9r | producer-config doc + ordering proof | `docs/kafka-producer-tuning.md` | P1 |

**Revised estimate: ~14-18h.** T8c is promoted because demo beats 5 and 6 cannot be performed without
the injection knobs — it was previously filed as polish.

**Explicitly dropped at r4:** all further throughput optimisation, the T1 benchmark, the 10-minute
soak, and the seven-decision scale argument. The code is correct and tested; that is the bar.


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

1. ~~Is 50k still the number we claim?~~ **CLOSED at r4: no.** The demo does not demonstrate or
   assert 50k. See §1.1 — it is a one-paragraph answer if asked, and no further work is spent on it.
2. **Who owns the `.wslconfig` change and the WSL2/Docker smoke test?** It gates three teams and is
   now the single point of failure for "the demo works."
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
