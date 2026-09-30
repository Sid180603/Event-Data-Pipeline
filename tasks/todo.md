# Task List: Event Generator

Plan: `tasks/plan.md` · Spec: `SPEC.txt` · **Revision r4** (demo posture settled)
**P0** = required for the demo · **P1** = high value · **P2** = cut if time-boxed

**Demo posture (r4):** the demo does **not** demonstrate 50k/sec. It demonstrates a working system
and explains the architecture that would reach 50k. This task list is ordered around that.

**Cut order:** T1r load client → T8c third injection knob → T9r A/B benchmark (keep the doc) →
T10b polish on the live view. **Never cut: T3, T5b, T11, T10a.**

**Environment:** the running system lives in **WSL2/Ubuntu** (Cassandra and Flink have no native
Windows support, so the whole stack shares the laptop's 6 cores with Windows). Dev on Windows is fine.
Gateway and driver in **separate containers with CPU limits**.

**Status legend:** ✅ done · 🔄 in flight · ⬜ not started

**r4 note:** acceptance criteria below carry the review fixes, marked **[C#] [H#] [M#] [G#]** against
`plan.md` §0.1 of r3.2. Criteria that assumed a live 50k benchmark are marked **[r4: reduced]**.

---

## Progress

| # | Task | Status | Tests | Commit |
|---|---|---|---|---|
| T0 | scaffold | ✅ | — | `4ce45c7` |
| T2 | CloudEvents contract + ledger + spec amendment | ✅ | 32 | `4ce45c7` |
| T4 | asymmetric auth, JWT-only tenant binding | ✅ | 50 | `096298f` |
| T5a | validation + post-encryption DLQ | ✅ | 28 | `bd49eca` |
| T6 | crypto: key separation, registry, AAD | ✅ | 37 | `6559943` |
| T7 | per-tenant token bucket | ✅ | 31 | `365add2` |
| T8a | driver funnel FSM + corpus | ✅ | 38 | `26e2d49` |
| — | integration fixes from worker review | ✅ | +2 | `a40bf0a` |
| T5b | ingest pipeline, handler, limits, decrypt | 🔄 | — | — |
| T8b | 500-tenant skew, sharding, webhook source | 🔄 | — | — |
| T10a | metrics registry | 🔄 | — | — |
| T3 | FastAPI app, Kafka sink, compose | ⬜ | — | — |
| T11 | verification oracle + chaos scripts | ⬜ | — | — |
| T10b | live CLI view, inspector, Makefile | ⬜ | — | — |
| T1r | load client | ⬜ | — | — |
| T9r | producer-config doc | ⬜ | — | — |
| T8c | injection knobs | ⬜ | — | — |

**219 tests green at r4.**


---

## Phase 0: De-risk the load-bearing assumption

### T1r: Load client **[r4: reduced from the T1 throughput spike]**

**Description:** ~~A throwaway benchmark deciding whether 50k/sec is reachable.~~ **Removed under the
r4 demo posture** — we no longer need to know, and the 6-core laptop stops being a risk. What remains
is a small, honest load client that shows the gateway holding a sustained rate, so the demo can say
"here is it under load" without claiming a benchmark.

**Acceptance criteria:**
- [ ] Drives N concurrent requests against `/v1/ingest` at a configurable target rate, using the
      pre-generated corpus (replay mode — no event construction in the load path)
- [ ] Reports achieved events/sec, req/sec, p50/p99 latency, error breakdown by status code
- [ ] **Reports the measured per-event server cost** so the 50k design claim rests on a measurement
- [ ] Batches are single-tenant and within the 500-event / 4 MiB caps
- [ ] **Explicitly NOT a benchmark** — output says so, and reports the machine it ran on. Never
      prints a number that could be mistaken for the 50k figure
- [ ] Testable without a broker (stub sink) so the client's own arithmetic is covered

**Verification:**
- [ ] Runs against a stub sink; arithmetic (events/sec from batch counts) is unit-tested
- [ ] Manual: a short run against the real gateway produces a plausible figure

**Dependencies:** T3
**Files:** `bench/load.py`, `bench/test_load.py` · **Effort: 2-3h** · **First on the cut list**

### ~~T1: Python throughput spike~~ — **CANCELLED at r4**

Superseded by T1r. The measurement it existed to produce is no longer a decision input. The
per-event cost it would have measured has since been measured directly by T6 (~55 µs total, ~42 µs
of it the crypto facade), which is a better number because it is a component breakdown.

---

## Phase 1: Contract

### T2: CloudEvents contract v0.1, ledger schema, spec amendment

**Description:** Publish the contract three other teams code against. Ships in **two drops** so it never
blocks us (M3): v0.1 with the minimum viable subset, then the full profile.

**Acceptance criteria:**
- [ ] `msgspec.Struct` definitions modelling a CloudEvents envelope: `specversion`, `id`, `source`, `type`,
      `subject`, `time`, `dataschema`, `datacontenttype`, `keyversion`, `sequence`, `sourcechannel`,
      `referrertype`, `completionmethod`
- [ ] All 9 event types as reverse-DNS `type` values (plan §D1 table)
- [ ] `source` = `/careers/<career_site_id>`; `subject` = `job_id` for the 7 job events, and
      **`user_id_pseudo` for the 2 identity events** **[H3]**
- [ ] `data` holds `candidate` + `event_payload` with the §D5 pseudonymous twins
- [ ] **Attribute names are lowercase `[a-z0-9]`, ≤20 chars, no underscores** (E2). A test asserts this
- [ ] `event.schema.json` **generated from the structs** via `msgspec.json.schema()`; a test fails if stale
- [ ] **`CONTRACT.md` v0.1 ships with the minimum viable subset** — `id` / `source` / `type` / `data` +
      batch content type — and **does not block T3** **[M3]**. Full profile lands in parallel
- [ ] `CONTRACT.md` states, explicitly:
      - single topic `career.events.raw`; **`enable.idempotence=false` is forbidden** and doing it is
        **silent**, not a `ConfigException` **[M4, H5]**
      - **the Kafka key is gateway-derived** as `<career_site_id from JWT>|<user_id_pseudo>`; a client
        `partitionkey` is **ignored**, and a disagreeing one is `403` **[C2]**
      - **the honest ordering contract**: guaranteed per `(tenant, user)` *by a single worker via sticky
        routing*; not guaranteed across restarts; **consumers must order by `sequence`** **[C1]**
      - **batching mandatory** (T1 batch floor), **single-tenant batches**, **caps of 500 events and
        4 MiB**, keep-alive mandatory **[D2, H9]**
      - **`202` is not a durability receipt**; `503` + `Retry-After` on buffer-full or broker-down; clients
        must retry with a stable `id` **[C4]**
      - **asymmetric JWT** (EdDSA preferred, RS256 acceptable), cached public key **[H2]**
      - **64 KiB per-event cap**, checked on the final serialized event **[G2]**
      - **dedup key is `(source, id)`** everywhere — not `id` alone **[C1/low]**
      - **live-client `sequence` contract**: the UI SDK must maintain a per-user monotonic counter across
        page loads **[low]**
      - **duplicate `id` within a batch is rejected** **[M13]**
      - **cross-request `id` uniqueness is the client's responsibility**
- [ ] **Ground-truth ledger schema specified once** — `(source, id, type, tenant, user_pseudo, ts)`, plus
      on-disk format and how sharded driver processes merge (3M rows at peak, 15M at soak) **[M2]**
- [ ] **`SPEC.txt:133` amended** to `career.events.raw` with rationale, **plus a note that we set a raw key
      and let Kafka hash it** so nobody implements a double hash **[M9]**
- [ ] Test validates ≥2 example payloads per event type against both structs and generated schema
- [ ] Negative tests: invalid `type` enum, missing required attribute, bad `time`, bad extension name,
      `source` disagreeing with the JWT
- [ ] Acknowledged **in writing** by the Queue, DB **and UI** leads

**Verification:**
- [ ] `pytest contracts/` green
- [ ] Stale-schema test passes
- [ ] Attribute-naming test green
- [ ] `git diff SPEC.txt` shows the line-133 amendment
- [ ] **HUMAN CHECKPOINT: contract acknowledged by all three teams — but T3 does not wait on this** **[M3]**

**Dependencies:** T1 (batch floor is an input)
**Files:** `contracts/cloudevent.py`, `contracts/attributes.py`, `contracts/event.schema.json`,
`contracts/examples/*.json`, `contracts/CONTRACT.md`, `contracts/test_contracts.py`, `SPEC.txt`
**Effort: 5-6h**

---

## Checkpoint A: Foundation
- [ ] T1 results recorded: batch floor, worker count N, ceiling, hot functions, **cores available to us**
- [ ] T1 spike deleted; no spike code in the production tree
- [ ] Sidecar fallback decision made **explicitly**
- [ ] `CONTRACT.md` v0.1 published; T3 may start **without** cross-team sign-off
- [ ] **HUMAN REVIEW — topic-name contradiction settled with the Queue lead**
- [ ] **HUMAN REVIEW — the `enable.idempotence=false` prohibition and the derived-key rule are understood by
      the Queue lead.** Our ordering and dedup claims both depend on them, and violating either is **silent**.

---

## Phase 2: Vertical slice

### T3: Vertical slice #1 + driver v0 + ground-truth ledger

**Description:** First complete path: one event type, HTTP → gateway → `career.events.raw`, verified by an
independent consumer, driven by a minimal Python driver. Establishes the dedicated producer thread, the
bounded body read, and the honest `202`/`503` semantics.

**Acceptance criteria:**
- [ ] `POST /v1/ingest` accepts **1..500 CloudEvents**, `Content-Type: application/cloudevents-batch+json`,
      returns `202 {accepted, rejected}`
- [ ] **Body size capped at 4 MiB, enforced BEFORE allocation** — uvicorn has no default body limit, and
      48 KiB × 500 events would be a 24 MB body held by `await request.body()`; × 8 workers ≈ 192 MB
      **[H9]**
- [ ] `413` returned for an over-cap request, with the caps published on the endpoint and in `CONTRACT.md`
- [ ] Mode detected from the `Content-Type` prefix, not by sniffing the payload
- [ ] **A batch mixing tenants is rejected `403`** — the §D2 invariant that makes isolation enforceable
- [ ] FastAPI's Pydantic layer bypassed on the hot path: `await request.body()`, then msgspec
- [ ] **Kafka key DERIVED by the gateway** as `<career_site_id>|<user_id_pseudo>`; client `partitionkey`
      ignored; disagreement → `403` **[C2]**
- [ ] **Dedicated producer thread per worker** owns a bounded `queue.Queue` + `poll()` loop; the asyncio
      loop only `put()`s
- [ ] **`503` + `Retry-After` when the buffer is full or the broker is unreachable** — never `202` for work
      we cannot hold **[C4]**
- [ ] Driver v0 emits **≥ 1,000 events/sec** for 1 tenant across 3 `sourcechannel` values
- [ ] Driver uses keep-alive + batching at or above the T1 floor
- [ ] **Ground-truth ledger exists from day one**, in the T2 schema — precondition for T11, do not defer
- [ ] uvicorn with `--loop uvloop --http httptools --workers N` (N from T1)
- [ ] docker-compose brings up Kafka + gateway + driver as **separate services with CPU limits**
- [ ] **TLS terminates at the LB; the gateway is internal-only** — no PII or bearer token transits plaintext
      inside the perimeter **[M11]**

**Verification:**
- [ ] Integration test against real Kafka (testcontainers) passes
- [ ] Manual: `kafka-console-consumer` shows events with correct partition keys
- [ ] Test: **a multi-tenant batch is rejected `403`**; a single-tenant batch passes
- [ ] Test: **8 concurrent 24 MB requests leave RSS flat** **[H9]**
- [ ] Test: **a 24 MB batch is rejected `413` before allocation** **[H9]**
- [ ] Test: buffer-full returns `503`, not `202` **[C4]**
- [ ] Test: broker-down returns `503` and **drops nothing silently**
- [ ] Test: the bounded queue actually bounds — RSS flat under a producer flood
- [ ] Benchmark: sustained ingest rate recorded as the new baseline

**Dependencies:** T2 v0.1
**Files:** `app/main.py`, `app/ingest/handler.py`, `app/ingest/limits.py`, `app/kafka/producer.py`,
`app/config.py`, `driver/main.py`, `driver/ledger.py`, `docker-compose.yml`
**Effort: 6-7h**

### T4: Asymmetric auth, JWT-only tenant binding

**Description:** Bind every event to a verified tenant. The critical invariant is §D4: tenant identity comes
from the signed token, never from the body or a header.

**Acceptance criteria:**
- [ ] **EdDSA or RS256** with a cached public key / JWKS — the gateway holds **only a public key and cannot
      mint a token** **[H2]**. r3.1's HS256 choice was justified by arithmetic §D2 had already refuted
      (1,000 verifies/sec, not 50,000); the real HS256 cost was that a shared secret means a gateway
      compromise is a total 500-tenant compromise
- [ ] `career_site_id` extracted from claims; **must exist in the tenant registry** or the request is
      rejected **[H4]**
- [ ] **Verified once per batch, not per event**; every event in the batch must carry a `source` matching
      the token's tenant, enforced in the same pass
- [ ] Missing / malformed / bad-signature / expired token → `401`
- [ ] `source` in the body that disagrees with the token → `403`, never silently accepted
- [ ] `sourcechannel` derived from credential registration; `X-Source-Type` is a validated *hint* —
      mismatch rejected, not honored
- [ ] Verified at **≥ 10,000 req/s** with no measurable latency regression vs the T3 baseline
- [ ] Malformed-token path separately rate-limited so it cannot burn the event budget

**Verification:**
- [ ] Unit tests: valid, expired, wrong-key, wrong-tenant, missing-claim, body-tenant-mismatch
- [ ] Test: forged `X-Source-Type` on a WEB_APP credential does not change the recorded `sourcechannel`
- [ ] Test: tenant A's token cannot write an event claiming tenant B
- [ ] Test: a batch mixing two tenants is rejected `403`
- [ ] Test: a token with an unknown `career_site_id` is rejected **[H4]**
- [ ] **Test: the gateway cannot mint a valid token for any tenant** — sign with a key it does not hold,
      confirm rejection **[H2]**
- [ ] Confirmed: JWT verify count per request == 1, not N

**Dependencies:** T3
**Files:** `app/auth/jwt.py`, `app/auth/registry.py`, `app/middleware/auth.py`
**Effort: 3-4h** · *Lean path: dev-mode bypass with a loud warning — **not** acceptable for the demo*

### T5: Validation + post-encryption DLQ

**Description:** Validate with msgspec, reject per-event, and route rejects to a DLQ that is **not** a
plaintext PII store.

**Acceptance criteria:**
- [ ] `msgspec.json.decode(raw, type=list[CloudEvent])` for the whole batch in one call
- [ ] Required attributes enforced: `specversion`, `id`, `source`, `type` present; `time` parses as
      RFC 3339; extension names pass the naming rules
- [ ] Invalid event rejected **individually** — the other 49 in a 50-event batch still succeed
- [ ] **Duplicate `id` within a batch is rejected** — otherwise a client reusing one `id` across 50 events
      gets all 50 accepted and the Queue team's `(source,id)` dedup silently collapses 50 real events
      into one **[M13]**
- [ ] **Pipeline order is fixed: decode → auth → validate → encrypt → produce** **[C3]**
- [ ] **The DLQ carries the POST-encryption event** — `original_payload` is the encrypted form, never
      plaintext **[C3]**
- [ ] `error_context` carries `reason`, `field`, `index`, `stage`, plus the spec's `exception_class`,
      `error_message`, `failed_at`, `retry_count`, `original_topic`, `original_partition`,
      `original_offset` — and **never the offending value** **[C3]**
- [ ] The DLQ message is itself a valid CloudEvent, so the replay worker needs no special-case parser
- [ ] Response `202` reports `rejected: [{index, reason}]`; `413` for over-cap bodies **[H9]**
- [ ] Driver flag `--inject-invalid-rate=<pct>` produces exactly that percentage of malformed events
- [ ] No cross-tenant contamination
- [ ] **Pure-Python `jsonschema` is not on the hot path** (import guard test)

**Verification:**
- [ ] Unit tests: missing required attribute, bad `type`, bad `time`, bad extension name, wrong type,
      oversized, 50-batch with 1 bad, **50-batch with a duplicate `id`** **[M13]**
- [ ] Test: DLQ message matches the `error_context` contract field-for-field **and** validates as a
      CloudEvent
- [ ] **Test: DLQ round-trip — a DLQ event re-injected is accepted and NOT double-encrypted** **[C3]**
- [ ] **Canary test [M7]:** inject `SENTINEL-8f3a@example.invalid` into a fixture, run it through every
      error path, assert it appears in **no** log line at DEBUG **including tracebacks**, **and in no DLQ
      payload**. A regex for "email patterns" passes vacuously — `msgspec.ValidationError` reprs include the
      offending value, tracebacks capture locals, and the biggest leak isn't a log at all, it's the DLQ
- [ ] Manual: `--inject-invalid-rate=5` → DLQ gets exactly 5%, main topic unaffected

**Dependencies:** T3 (parallel with T4)
**Files:** `app/validate/`, `app/dlq/`, `app/ingest/pipeline.py`, `driver/flags.py`
**Effort: 5-6h**

### T6: Encryption, key separation, tenant registry

**Description:** Encrypt PII and emit pseudonymous twins. Three fixes from the review beyond r3.1: purpose
separation, a fixed tenant registry instead of a JWT-keyed cache, and AAD binding.

**Acceptance criteria:**
- [ ] `email`, `phone_number`, `alternate_phone_number`, `user_name`, `gender` AES-GCM encrypted before
      Kafka; **plaintext never leaves the process**
- [ ] **All PII lives inside `data`, never in a context attribute** — CloudEvents: *"Sensitive information
      SHOULD NOT be carried or represented in context attributes."* **[H3, C2]**
- [ ] **Purpose separation: two keys per tenant, one HKDF call, distinct `info`** — `info=b"enc"` and
      `info=b"mac"`. One key for AES-GCM and HMAC is a crypto-hygiene error and the first thing a reviewer
      reads **[H4]**
- [ ] **Keys derived once at startup from a fixed tenant registry**, and tokens with an unknown
      `career_site_id` rejected. r3.1 cached on a JWT-controlled key — an unbounded-growth DoS — and
      justified it with "50,000 HKDF calls/sec" when there are 500 tenants **[H4]**
- [ ] `user_id_pseudo` = `HMAC-SHA256(mac_key, raw_user_id)`
- [ ] `email_hmac` emitted for equality-grouping, documented as non-reversible
- [ ] **AES-GCM AAD bound to `(source, id, type, field_name, keyversion)`** — free hardening that blocks
      ciphertext field-swap and cross-event transplantation **[low]**
- [ ] `keyversion` emitted as an **extension attribute**, not inside `data` — a version number is not
      sensitive, and consumers need it to select a key without decoding `data` **[low]**
- [ ] Fresh random 96-bit nonce per encryption; **nonce reuse impossible**, proven by test
- [ ] Master-key version prefixed to ciphertext so rotation needs no rewrite
- [ ] 5 fields encrypted at the T1-required batch/worker config without dropping under 50k events/sec
- [ ] **Operator-only decrypt endpoint** — gated by a **separate credential from tenant auth**, rate-limited,
      with an audit log recording *who* decrypted *which* `(source, id)` **[M6]**. If a tenant JWT can
      decrypt arbitrary events, tenant isolation is gone and this becomes the most attractive endpoint in
      the system
- [ ] **The produced Kafka message key contains `user_id_pseudo`, never the raw id** — T6's earlier test
      scanned only the *envelope*, so it passed while the raw identifier sat in the most-inspected field
      in the system **[C2]**
- [ ] **64 KiB cap holds after base64 expansion [G2]:** the size check runs on the **final serialized
      event**; ingress limit ~48 KiB. Test an event under the ingress limit that crosses 64 KiB after
      encryption and assert clean rejection — not a broker-side failure

**Verification:**
- [ ] Round-trip decrypt; same plaintext → different ciphertext
- [ ] Test: tenant A's key cannot decrypt tenant B's ciphertext
- [ ] Test: **1,000,000 encryptions → zero nonce collisions**
- [ ] **Test: AAD binding enforced** — a ciphertext moved to a different field, event, or key version fails
      to authenticate **[low]**
- [ ] **Canary leak test over the full envelope AND the produced Kafka key** **[M7, C2]**
- [ ] **Field-name allowlist test, not a value regex [H3]** — every context attribute must be in a known-safe
      set (`specversion`, `id`, `source`, `type`, `subject`, `time`, `dataschema`, `datacontenttype`,
      `keyversion`, `sequence`, `sourcechannel`, `referrertype`, `completionmethod`); any other attribute
      fails regardless of value. The r3.1 regex looked for "email or phone pattern" and **could not see**
      `usr_992182741` — it passed while the rule was broken
- [ ] Test: a decrypt attempt with a **tenant** credential is rejected
- [ ] Benchmark: per-event encryption cost measured (5 fields + 2 HMAC + base64)
- [ ] **SECURITY REVIEW checkpoint before T7** — key separation, nonce handling, AAD, decrypt authz

**Dependencies:** T3 (independent of T4/T5)
**Files:** `app/crypto/aesgcm.py`, `app/crypto/keys.py`, `app/crypto/registry.py`,
`app/pseudonym/hmac.py`, `app/ingest/enrich.py`, `app/ingest/decrypt.py`, `tests/test_crypto.py`
**Effort: 6-7h**

### T7: Token bucket — correct under sticky routing

**Description:** Per-tenant rate limiting, and the load shedder that keeps the process alive. **Its accuracy
now depends entirely on §D12.**

**Acceptance criteria:**
- [ ] Per-tenant token bucket; rate and burst configurable per tenant
- [ ] **The configured rate is the ACTUAL rate, not N×** — guaranteed only because one `(tenant, user)`
      routes to one worker **[H1, D12]**
- [ ] Over-limit → `429` + `Retry-After`; response still reports per-event accept/reject
- [ ] Rate-limit rejects go to the DLQ, **not silently dropped** — a silent drop is invisible in ground-truth
      reconciliation and reads as data loss
- [ ] A configured whale tenant gets its full burst with no measurable impact on the other 499
- [ ] **Demonstrable: under deliberate overload, p99 latency stays bounded while throughput saturates** —
      the Python failure mode to defend against

**Verification:**
- [ ] Unit tests: refill math, burst allowance, boundary conditions
- [ ] Test: tenant X over its limit cannot cause tenant Y to receive a `429`
- [ ] **Test: with sticky routing ON, one tenant's measured aggregate rate at N workers is ≤ 1.2× the
      configured limit** — this test **fails without §D12** and is the reason it exists **[H1]**
- [ ] Test: gateway stays responsive under 3× overload (bounded latency, no 5xx storm)
- [ ] Benchmark: 50k/s across 500 tenants, no measurable regression
- [ ] Manual: with the T8b Zipfian driver, whale tenant spikes while the other 499 stay clean

**Dependencies:** T4, T5, T6
**Files:** `app/ratelimit/bucket.py`, `app/middleware/ratelimit.py`
**Effort: 3-4h** · **Cut candidate: the fairness *demo*, never the shedding *function***

### T10a: `/metrics` — the instrumentation T9 is graded on

**Description:** Split out of T10 precisely because **T9's acceptance criteria are unmeasurable without
it, and T10 was first on the cut list** — r3.1's cut list deleted its own prerequisites.

**Acceptance criteria:**
- [ ] `/metrics`: ingest rate, accepted/rejected, per-`type` and per-`sourcechannel` counts, 4xx/5xx,
      rate-limit rejections, DLQ depth, validation failure rate, encryption latency, Kafka produce latency,
      batch-size histogram, **per-worker** rate
- [ ] **Per-`(tenant, user)` ordering violations observable** — a monotonic counter, so T9 can prove §D12
      works and detect its absence **[C1]**
- [ ] **Consumer lag exported** (librdkafka consumer metrics or Queue team's group lag) so H8 is measurable
- [ ] Instrumentation overhead measured, not assumed

**Verification:**
- [ ] Manual: figures match the T3 baseline
- [ ] Benchmark: overhead with vs without instrumentation recorded
- [ ] **HUMAN CHECKPOINT: T9 cannot start until this lands**

**Dependencies:** T3 · **Must precede T9**
**Files:** `app/metrics.py` · **Effort: 1-2h**

---

## Checkpoint B: Gateway is production-shaped
- [ ] All tests pass; linter clean; no spike code in the tree
- [ ] End-to-end: driver → gateway → Kafka, tenants A and B cleanly separated
- [ ] PII never appears in plaintext past the gateway — canary, not regex
- [ ] **DLQ contains no plaintext PII**, and a DLQ event re-injects without double-encryption **[C3]**
- [ ] **No raw `user_id` in any context attribute or in the Kafka message key** **[C2, H3]**
- [ ] `413` / `429` / `503` semantics implemented and tested
- [ ] Overload behaviour bounded and demonstrated
- [ ] **Security review of T4 + T5 + T6 complete and signed off**
- [ ] **HUMAN REVIEW — gateway feature-complete**

---

## Phase 3: Realism and scale

### T8a: Driver — funnel FSM, payload shapes, replay corpus

**Description:** Upgrade the driver from random firehose to a realistic traffic simulator. **Split from T8b
(M1) — T9 needs T8a only, and the split was undefined in r3.1.**

**Acceptance criteria:**
- [ ] Per-session FSM: `JOB_VIEWED` → `APPLICATION_STARTED` → `STEP_COMPLETED`* →
      (`SUBMITTED` | `DRAFT_SAVED`)
- [ ] `JOB_WISHLISTED` emitted; `APPLICATION_STARTED` carries recommendation attribution
      (`referrertype: RECOMMENDATION`, `recommended_job_ids`) per spec #4/5
- [ ] **Our driver does NOT emit `APPLICATION_ABANDONED`** — Flink synthesises it on the watermark timeout
      (`SPEC.txt:338-339`). Stated in `CONTRACT.md` and confirmed with the Queue lead, or the drop-off
      metric is double-counted **[G1]**
- [ ] `completionmethod` extension tagged `MANUAL` | `RESUME_AUTOFILL` | `HYBRID` per spec #10
- [ ] **Every event carries a `sequence`** — zero-padded, monotonic per-`(source, user)`. Now the
      **authoritative ordering mechanism**, not a droppable fallback **[C1]**
- [ ] 2 distinct payload shapes: web (`userAgent`, `page`, `locale`) and mobile (`app.version`, `device`,
      `screen`) per `SPEC.txt:176-184`
- [ ] Configurable: drop-off rate per step, sessions/sec, burst size
- [ ] **Accelerated session time** flag so a 30-min abandonment window is observable in 3 min
- [ ] **Replay corpus generated** — fixed single-tenant CloudEvents batches + ledger in the T2 schema
- [ ] Corpus **partitioned by tenant** so every replay batch is single-tenant (§D2) — required for the
      gateway's `403` check to pass, not just hygiene
- [ ] `completionmethod` + autofill detection contract written and handed to the UI lead

**Verification:**
- [ ] Generated sequences pass the T2 validators 100% of the time
- [ ] Test: FSM never emits `STEP_COMPLETED` without a preceding `APPLICATION_STARTED`
- [ ] Test: every `id` unique within a `source`; ledger length == sent count
- [ ] Test: `sequence` monotonic per `(source, user)` and lexicographically sortable
- [ ] Test: every generated batch is single-tenant
- [ ] Manual: funnel survival curve plausibly monotonic
- [ ] Human checkpoint: autofill contract acknowledged by the UI lead

**Dependencies:** T3 (parallel with T4-T7)
**Files:** `driver/fsm.py`, `driver/sources.py`, `driver/corpus.py`, `driver/ledger.py`,
`contracts/autofill-detection.md`
**Effort: 5-6h**

### T8b: Driver — 500-tenant Zipfian skew, sharding

**Description:** Scale the driver to 500+ tenants with a realistic skew, and shard across processes.

**Acceptance criteria:**
- [ ] **500+ tenants**, Zipfian volume (configurable skew exponent) — `SPEC.txt:148` says enterprise
      tenants generate "exponentially higher traffic during hiring drives than SMBs". Uniform traffic would
      hide exactly the hot-partition and rate-starvation problems the spec worries about
- [ ] 3rd payload shape: third-party webhook (`referrer_url`, `utm_*`) — **cut candidate → 1**
- [ ] Driver sharded across processes so generation is not the bottleneck
- [ ] **Per-user hot-spot check** — sticky routing (§D12) concentrates a user on one worker, so confirm no
      single `(tenant, user)` produces enough traffic to saturate a worker **[C1]**

**Verification:**
- [ ] Test: no single worker exceeds its share under the Zipfian distribution
- [ ] Manual: Zipfian skew observable in Kafka (a few tenants dominate)

**Dependencies:** T8a
**Files:** `driver/tenants.py`, `driver/skew.py`, `driver/webhook_source.py`
**Effort: 3-4h**

### T9r: Producer-config doc + ordering proof **[r4: reduced from "peak 50k/s"]**

**Description:** ~~Prove a 50,000 events/sec peak with a 10-minute soak.~~ **Reduced under the r4 demo
posture** — the demo explains 50k as a design target rather than demonstrating it, so holding 50k on
this 6-core laptop is neither possible with the full stack nor necessary. What survives is genuinely
valuable: a **correct, fully-explicit, documented producer configuration** (which *is* part of the
50k argument), an **empirical proof of the ordering guarantee**, and a **measured run showing the
system holds under load**.

**Acceptance criteria:**
- [ ] Replay mode: near-zero CPU in the load path
- [ ] **A sustained load run at whatever rate this machine sustains**, reported honestly with the
      machine spec attached. The number is whatever it is — it is NOT presented as 50k **[r4]**
- [ ] **Zero lost events, zero duplicate `(source, id)`** vs the ground-truth ledger
- [ ] **Latency stays bounded under overload** — throughput saturates, p99 does not run away
- [ ] **[C1] Per-`(tenant, user)` ordering PROVEN:** consume the topic and assert events for one
      `(source, user)` arrive in `sequence` order. **Sticky routing must be enabled for this to
      pass** — the test is the guard, not a formality
- [ ] **[C1] Ordering-violation counter from T10a reads zero**, and a control run **with sticky
      routing disabled is expected to show violations** — proving we understand the guarantee we offer
- [ ] **[H8] A consumer runs throughout. Max end-to-end lag is bounded and reported.** If downstream
      cannot keep up, **the number is reduced, not the assertion**
- [ ] Kafka producer configured **exactly per D11** — every value explicit, nothing "left to default"
- [ ] **The producer's EFFECTIVE configuration is printed and asserted** against every key in the D11
      table **[H5]** — requested config is not evidence, effective config is
- [ ] **`enable.idempotence=true` and `acks=all` confirmed** before the run
- [ ] **`delivery.timeout.ms=5000` and `queuing.strategy=fifo` confirmed** — without these, broker
      failure surfaces after ~5 minutes with millions of events buffered in retry **[H5]**
- [ ] **librdkafka configuration reference consulted** and any divergence from the Java-client values
      reconciled in `docs/kafka-producer-tuning.md` — r3.1 flagged this as unconsulted and never closed it
- [ ] A/B: throughput and CPU at `compression.type=none` vs `zstd`, and 3 values of `batch.size`
- [ ] Graceful shutdown: SIGTERM drains in-flight batches, no partial batch, no loss
- [ ] Profile captured with `py-spy`; hot-path allocations addressed if the ceiling is missed
- [ ] Single host proven sufficient — multi-instance **only** if the ceiling is missed

**Verification:**
- [ ] Peak run: `sent == accepted == kafka-received`, `duplicates == 0`
- [ ] Soak run: memory, p99 and error rate flat across the run
- [ ] **Ordering test passes with sticky routing ON; the control run with it OFF shows violations** **[C1]**
- [ ] **Effective-config dump attached, every D11 key present and correct** **[H5]**
- [ ] **End-to-end lag reported and bounded** **[H8]**
- [ ] Test: SIGTERM mid-run loses nothing
- [ ] Test: 3× overload → bounded latency, correct 429/503, no 5xx storm
- [ ] **HUMAN REVIEW — headline number met, measured, reproducible**

**Dependencies:** T3, T4, T5, T6, T7, T10a, T8a
**Files:** `driver/replay.py`, `app/kafka/producer.py`, `bench/soak.py`,
`docs/kafka-producer-tuning.md`
**Effort: 7-9h**

---

## Checkpoint C: Load run + ordering proven
- [ ] A sustained load run completed at the rate this machine sustains, reported honestly
- [ ] Zero loss, zero duplicate `(source, id)` vs the ground-truth ledger
- [ ] **Ordering proven, with the sticky-routing control run demonstrating the counterfactual** **[C1]**
- [ ] **Effective Kafka config verified against every D11 key** **[H5]**
- [ ] **End-to-end lag bounded and reported** **[H8]**
- [ ] Bounded latency under overload
- [ ] Graceful shutdown verified
- [ ] **HUMAN REVIEW**

---

## Phase 4: Proof and demo

### T11: Verification oracle + chaos demos — **P0, depends on T9 only**

**Description:** Turns ground truth into one command that proves the pipeline, plus chaos scenarios.
**Reparented from T9+T10 to T9 only** — `verify` needs the ledger and Kafka plumbing, not a dashboard, so
T10b becomes a true leaf that can be dropped without consequence **[H7]**.

**Acceptance criteria:**
- [ ] `verify` reconciles ground truth against Kafka offsets and (where reachable) Cassandra row counts;
      reports exact per-`source` and per-`type` deltas
- [ ] Reports **duplicate `(source, id)` count == 0** using the CloudEvents-defined dedup rule
- [ ] **Chaos 1 — gateway killed mid-run: loss ≤ the documented in-flight window** (bounded queue depth ÷
      drain rate), **reported as a number, not zero** **[C4]**. `kill -9` is uncatchable and `202` is not a
      durability receipt, so "zero loss" is unachievable here and r3.1's version of this test contradicted
      the plan's own "no exactly-once" statement
- [ ] **Chaos 2 — broker unreachable:** `503` returned, **fails loudly, drops nothing silently**; surfaces
      within ~5s thanks to `delivery.timeout.ms=5000`, not 5 minutes **[H5]**
- [ ] **Chaos 3 — 5% invalid injection:** DLQ captures exactly 5%, main topic unaffected, other 95% intact,
      **and the DLQ contains no plaintext PII** **[C3]**
- [ ] **Chaos 4 — downstream (Cassandra) blocked:** the queue's backpressure path holds without dropping;
      exercises the **Queue team's** component. **Also reports lag growth, since that is the failure mode
      the demo would otherwise hide** **[H8]**
- [ ] **Chaos 5 — one tenant floods:** the bucket sheds that tenant only; other 499 unaffected
- [ ] Printed demo scorecard: sent / accepted / stored / duplicates / DLQ on one screen
- [ ] All scenarios in one runnable script, timed, for the live demo

**Verification:**
- [ ] Each scenario run end-to-end; `verify` output inspected and correct
- [ ] **Chaos 1 reports a bounded loss number, and the driver re-POSTs unacked batches** — note this means
      **duplicates on the wire by construction**, so the `duplicates == 0` assertion is scoped to
      **"duplicates after `(source,id)` dedup"**. This scoping is stated in the contract, not glossed **[C4]**
- [ ] Test: `kill -9` mid-run → loss within the documented window, reconciling exactly after
- [ ] Test: duplicates == 0 after dedup across a full soak
- [ ] **Demo line confirmed working:** *"we emit CloudEvents 1.0 — the same format AWS EventBridge, Azure
      Event Grid, Google Eventarc and Knative use"*
- [ ] **HUMAN REVIEW — full demo script run end to end, inside the time limit**

**Dependencies:** T9 (and T10a's metrics). **Not** T10b.
**Files:** `tools/verify.py`, `scripts/chaos/*.sh`, `docs/demo-runbook.md`
**Effort: 5-6h**

### T10b: Dashboard, CLI live view, inspector, `make demo` — P1, cuttable

**Description:** You cannot demo 50k/s without a way to *show* 50k/s — but this is genuinely cuttable now
that T10a carries the metrics T9 needs.

**Acceptance criteria:**
- [ ] CLI live view showing live events/sec and error rate — **zero external dependencies**
- [ ] Inspector: side-by-side raw vs encrypted payload for a sampled event, auth-gated
- [ ] `make demo` single entry point starting the whole stack + live view
- [ ] Per-tenant throughput visible, so Zipfian skew is demonstrable
- [ ] Metrics instrumentation overhead at 50k/s measured (T10a carries the counters; this is the display)

**Verification:**
- [ ] Manual: `make demo` from a clean checkout
- [ ] Test: sampled ciphertext matches re-encryption under the same tenant key
- [ ] Manual: live view events/sec matches the T9 soak figure

**Dependencies:** T9
**Files:** `tools/observe.py`, `app/inspect.py`, `Makefile`
**Effort: 3-4h** · **First item on the cut list**

---

## Checkpoint D: Demo ready — FINAL
- [ ] All tests pass; linter clean; no spike code in the tree
- [ ] `CONTRACT.md` + CloudEvents schema + **amended `SPEC.txt:133`** + Kafka tuning doc + effective-config
      dump + key-derivation doc + autofill contract all handed over
- [ ] `make demo` + chaos script run clean from a fresh clone, inside the time limit
- [ ] Open questions (plan §7) answered and the plan reflects them
- [ ] End-to-end reconciliation: zero loss, zero duplicate `(source, id)`, ordering proven
- [ ] **The 50k claim in the design write-up is honest** — measured per-event cost stated, and it is
      clear the figure is a design target rather than a demonstrated result
- [ ] **FINAL HUMAN REVIEW — ready to demo**

---

## Review-fix traceability

| Finding | Where fixed |
|---|---|
| C1 ordering | §D12 sticky routing, D3 contract, T3, T9 (proof + control run), T10a (counter), T8a (`sequence`) |
| C2 partitionkey | D1, D3 (derived key), T2, T3, T6 (Kafka key test) |
| C3 DLQ PII | D6 pipeline order, T5 (canary + round-trip), T6 |
| C4 `202`/kill -9 | D4 semantics, T3 (`503`), T9, T11 (Chaos 1 reframed) |
| H1 rate limit | D12, T7 (regression test) |
| H2 HS256 | D4, T4 |
| H3 `subject` PII | D1 table, T2, T6 (field-allowlist test) |
| H4 key separation | D5, T6 |
| H5 Kafka config | D11 (full rewrite), T9 (effective config) |
| H6 budget | §1.1, T1 |
| H7 T10 split | T10a/T10b, T11 reparented, cut list |
| H8 consumer lag | T9, T10a, T11 Chaos 4, §7 Q1 |
| H9 batch cap | D2, T2, T3 |
| M1 T8 split | T8a/T8b, §3 graph |
| M2 ledger schema | D8, T2 |
| M3 T2 not blocking | T2 v0.1, §3 graph |
| M4 idempotence | D11, T2 |
| M5 master secret | §7 Q4 *(open — needs human answer)* |
| M6 decrypt authz | T6 |
| M7 canary tests | T5, T6 |
| M8 50k origin | §7 Q1 *(open — needs human answer)* |
| M9 amend spec | T2, §7 |
| M10 cut re-rank | §4.1 |
| M11 TLS | D10, T3 |
| M12 `sequence` scope | D1 (rationale recorded) |
| M13 duplicate `id` | D6, T2, T5 |
| Low | header, effort totals, T9↔T11 dedup unified, §3 graph, AAD, `keyversion` ext, live `sequence` contract |
