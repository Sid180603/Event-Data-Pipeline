# Implementation Plan: Event Generator (Ingestion Gateway + Synthetic Driver)

**Spec of record:** `SPEC.txt`
**Scope:** Team slice #2 of 4 — **Event Generator**. Excludes UI, Queue/Kafka+Flink, Database/Cassandra.
**Revision:** **r3.2** — independent code review completed. All Critical and High findings accepted and
folded in; most Medium findings accepted. **This plan is now internally consistent and safe to work from.**
**Status:** Awaiting human approval. No code written.

---

## 0. Revision log

| Rev | Change |
|---|---|
| r1 | Initial plan (Go proposed) |
| r2 | Language set to **Python**; full architecture revision (batching, multi-process, generate-and-replay) |
| r3 | Research validation. Missed CloudEvents (E1); three errors corrected (E2-E4); seven improvements adopted (I1-I7) |
| r3.1 | Self-consistency pass. Three gaps closed: event-type enumeration (G1), 64 KiB vs base64 expansion (G2), Java-client-vs-librdkafka defaults (G3) |
| **r3.2** | **Independent review. 4 Critical + 9 High + 13 Medium findings. All Critical and High accepted. Root cause of the Criticals: per-process mechanisms claimed as system-wide properties. Fixed via sticky routing, honest contracts, and pipeline ordering.** |

### 0.1 Review outcome

**Accepted — the four Criticals (all were real):**

| # | Finding | Resolution |
|---|---|---|
| **C1** | Ordering guarantee false as written. 6-8 workers × own producers ⇒ cross-producer order is nondeterministic. Idempotence only orders *within one producer's* retry sequence. | **Sticky routing** — §D12. One `(tenant, user)` → one worker → one producer. Guarantee made true rather than downgraded. Honest contract text added. `sequence` **promoted from droppable fallback to required mechanism.** |
| **C2** | `partitionkey` client-supplied (tenant-isolation hole one row below §D4) **and** carried raw `user_id`, undoing §D5. | Gateway **derives** the key as `<career_site_id from JWT>\|<user_id_pseudo>` and **ignores** any client `partitionkey`; mismatch → `403`. §D1, §D5, T3, T6. |
| **C3** | DLQ republished the **pre-encryption** event ⇒ a second plaintext-PII Kafka topic. | Pipeline order fixed and stated: **decode → auth → validate → encrypt → produce** (§D6). DLQ payload is the **post-encryption** event; `error_context` never carries the offending value. |
| **C4** | `202` returned from an in-memory queue, so `kill -9` → zero-loss is unachievable. Plan contradicted itself (§6 said no exactly-once; T11 demanded exactly-once). | `202` redefined as *"accepted into the buffer — **not** a durability receipt"*. `503` on buffer-full / broker-down. Chaos 1 reframed to a **bounded, reported** loss window. Drift removed. |

**Accepted — the nine Highs:**

| # | Finding | Resolution |
|---|---|---|
| **H1** | Rate limiting is per-process; 8 workers ⇒ 8× the configured rate, and no T7 test could see it. | Fixed by the same **sticky routing** as C1 (§D12). Plus a test that fails without it. |
| **H2** | HS256 justified by arithmetic §D2 had already refuted (1,000 verifies/sec, not 50,000). Worse: a shared secret can mint tokens for all 500 tenants. | **Asymmetric JWT (EdDSA preferred, RS256 acceptable)** with a cached public key. Gateway holds a *public* key and cannot mint. §D4. |
| **H3** | `subject` carried a raw `user_id` for the two identity events — the exact rule §D5 exists to prevent. The log-scan test (regex for email/phone) **could not see it** and passed anyway. | `subject` = `user_id_pseudo`. Test rewritten as a **field-name allowlist**, which cannot be defeated by a value pattern. §D1, T6. |
| **H4** | One key for both HMAC and AES-GCM, no purpose separation, HKDF called with no `info`. Plus a JWT-controlled cache key with no bound (DoS). | `info=b"enc"` / `info=b"mac"`. Keys derived once from a **fixed tenant registry**; unknown `career_site_id` → reject. Bounds the cache, removes derivation cost, adds a tenant-existence check. §D5, T6. |
| **H5** | §D11 said "set everything explicitly" then said "leave" four times — and asserted a librdkafka `linger.ms` divergence I had not verified. | Every value explicit. **Unverified divergence claim removed.** Added `delivery.timeout.ms`, `queuing.strategy`, explicit `max.in.flight`. Corrected framing: idempotence-off is **silent**, not a `ConfigException`. §D11. |
| **H6** | Per-event budget omitted encode and undercounted crypto ~4× (5 encrypted fields, 2 HMACs, not 1+1). | Budget corrected to **22-28 µs**. Worker count is now an **output of T1**, not a Stack-table decision. Fallback ladder now leads with the honest small-box answer. §1. |
| **H7** | Cut list deleted its own prerequisites — T10 is first to cut, but T9 needs its metrics and T11 depends on it. T11 was P1 *and* never-cut. | **T10 split** into T10a (metrics, P0, ~1h) and T10b (dashboard/CLI, cuttable). **T11 reparented to T9 only**, relabelled **P0**. |
| **H8** | No consumer-lag budget. A 50k/s gateway in front of a stalled Flink reports success while lag grows invisibly; our soak would still pass. | T9 requires a running consumer with **bounded, reported end-to-end lag**, and states that if downstream can't keep up, **the demo number is reduced, not the assertion**. New §7 Q1. |
| **H9** | No per-batch byte cap. 48 KiB × 500 events = a 24 MB request body; uvicorn has no default limit. Trivial authenticated DoS. | **Max batch bytes (4 MB) and max events (500)**, `413` on violation, both published. CloudEvents HTTP binding *"SHOULD allow the receiver to choose the maximum size of a batch"* — accepting the media type obliges us to advertise the limit. §D2, T3. |

**Accepted — Medium findings:** M1 (T8a/T8b split defined), M2 (single ledger schema in T2), M3 (T2 no longer blocks T3), M4 (`enable.idempotence=false` is the real prohibition), M5 (master-secret location → open question), M6 (decrypt endpoint = operator-only, separate credential), M7 (canary-based PII leak test), M9 (amend `SPEC.txt:133` as a T2 deliverable; note we set a raw key and let Kafka hash it), M10 (cut list re-ranked), M11 (TLS termination stated), M13 (duplicate `id` in a batch rejected).

**Partially accepted:**

- **M8 — "50k/s appears nowhere in `SPEC.txt`."** Accepted and important: it is **user-supplied, not spec-derived**, which sets the whole risk posture. Now §7 Q1.
- **M12 — `sequence` scope.** We deliberately keep `source` = tenant and scope `sequence` per-`(source, user)`. The extension explicitly permits *"an out-of-band agreement"*, and folding the user into `source` would break broker-level **tenant** filtering, which matters more in a multi-tenant SaaS. Rationale now recorded rather than the choice being silent.

**Noted, not actioned:** T4/T5/T6 remain cross-cutting rather than strictly vertical slices. The genuinely missing second slice is the **third-party webhook path with its own credential**. Accepted as a known shape rather than a phase restructure, because correctness outranks slice purity here.

**Low items fixed:** header revision, effort totals, r3 summary counts, T9-vs-T11 dedup assertion unified to `(source, id)`, §3 graph T8 placement, AES-GCM **AAD** bound to `(source, id, type, field, keyversion)`, `keyversion` promoted to an extension attribute, live-client `sequence` contract added to T2.

---

## 1. What Python Changes

### 1.1 Corrected per-event budget (H6)

The r3.1 figure of ~15 µs was wrong in two ways: it omitted encode, and it budgeted crypto as one AES-GCM and one HMAC per *event* when T6 encrypts **five** fields and D5 computes **two** HMACs.

| Component | Count | µs each | Subtotal |
|---|---|---|---|
| msgspec decode + validate | 1 | ~3 | 3.0 |
| msgspec **encode** (required — size check runs post-encryption, G2) | 1 | ~4 | 4.0 |
| AES-GCM | **5 fields** | ~1.5 | 7.5 |
| base64 of 5 ciphertexts | 5 | ~0.4 | 2.0 |
| HMAC-SHA256 (`user_id_pseudo`, `email_hmac`) | **2** | ~1 | 2.0 |
| PII field extraction + loop | — | — | 5.0 |
| **Total** | | | **~23.5 µs** |

`50,000 × 23.5 µs = 1.18 core-seconds/sec`, plus ~2-3 cores of HTTP/asyncio/socket overhead.

**Conclusion that survives:** batching is still mandatory. The per-request framework cost is ~20-40 µs for a handler that bypasses FastAPI's Pydantic layer, so:

| Batch | req/s | Framework cost | Verdict |
|---|---|---|---|
| 1 | 50,000 | 1.0-2.0 core-sec | **impossible** |
| 20 | 2,500 | 0.05-0.10 | fine |
| 50 | 1,000 | 0.02-0.04 | **comfortable** |

**Worker count is an OUTPUT of T1, not an input.** r3.1 committed to "6-8 workers" in the Stack table while the batch floor was still provisional — that was backwards. The T1 sweep produces both numbers.

**Fallback ladder, reordered honestly (H6):** if the demo box has **fewer than 8 physical cores**, "more workers" is the wrong answer — past core count you add contention and memory, not throughput. The honest ladder is: (a) raise the minimum batch size, (b) renegotiate the target, (c) Go/Rust sidecar for the validate+crypto loop only, (d) *(not first, contrary to r3.1)* more workers. **Find this out in T1 on day one.**

### 1.2 Stack

| Concern | Choice | Why not the obvious one |
|---|---|---|
| Envelope | **CloudEvents 1.0.3** | §D1. Non-negotiable per `SPEC.txt:131`. |
| ASGI | **FastAPI** | OpenAPI docs are a free demo artifact. Hot path reads `await request.body()` and bypasses FastAPI's Pydantic layer. |
| Validation | **msgspec** | Validated: *"decodes **and** validates JSON faster than orjson can decode it alone."* Also **generates the JSON Schema** from the structs — single source of truth. |
| Encryption | `cryptography` (AES-GCM) | OpenSSL + AES-NI. |
| Key derivation | `HKDF` **from a fixed tenant registry**, `info`-separated | §D5. Not derived per event, and not cache-keyed on a JWT claim. |
| Kafka | **confluent-kafka** (librdkafka) | `kafka-python` is pure Python and becomes the ceiling. |
| Event loop | **uvloop + httptools** | C implementations. |
| JWT | **EdDSA (or RS256)**, cached public key | §D4. Gateway cannot mint tokens. |
| Processes | **1 + N uvicorn workers, N from T1** | GIL caps a single process at 1 core. |
| Routing | **Sticky hash on `(tenant, user)`** | **§D12 — load-bearing for C1 and H1.** |

**Deploy on Linux/Docker.** Windows forces `multiprocessing` spawn semantics and librdkafka wheels are less reliable.

### 1.3 Python's failure mode and the defence

Python degrades slowly at overload rather than crashing. Three defences, all load-bearing:

1. **Bounded queues with a defined full-behaviour** — `503` on full, never unbounded growth (C4, H9).
2. **Per-tenant token bucket (T7) = the load shedder.** Correct **only** under sticky routing (H1).
3. **Dedicated producer thread per worker**, with `delivery.timeout.ms=5000` so broker problems surface in seconds rather than buffering 15M events (§D11).

---

## 2. Architecture Decisions

### D1 — CloudEvents 1.0.3 is the wire envelope

`SPEC.txt:131` requires validation to "conform to CloudEvents standards". CloudEvents is CNCF *Graduated*,
adopted by AWS EventBridge, Azure Event Grid, Google Eventarc, Knative, SAP, IBM, Adobe and GitHub.

**Attribute mapping:**

| Field | Attribute | Notes |
|---|---|---|
| `event_id` | **`id`** | Client-generated. **Idempotency key.** Spec: *"Consumers MAY assume that Events with identical `source` and `id` are duplicates."* |
| `career_site_id` | **`source`** | `/careers/<career_site_id>`. Makes the dedup key `(source, id)`. |
| `event_type` | **`type`** | Reverse-DNS: `com.<org>.career.job-viewed` |
| `timestamp` | **`time`** | RFC 3339 |
| `job_id` | **`subject`** | Job events |
| `user_id` | **`subject`** = **`user_id_pseudo`** | **Identity events only (H3).** Raw identifier never in a context attribute. |
| schema version | **`dataschema`** | *"Incompatible changes to the schema SHOULD be reflected by a different URI"* |
| payload + PII | **`data`** | PII lives here, never in context attributes |
| key version | **`keyversion`** *(extension)* | Lets consumers select a key without decoding `data`. Low |
| ordering | **`sequence`** *(extension)* | Zero-padded, per-`(source, user)`. Scope recorded in `CONTRACT.md` — the extension permits an out-of-band agreement |
| `source_channel` | `sourcechannel` *(ext.)* | **No underscores** — naming rules require `[a-z0-9]` only |
| `referrer_type` | `referrertype` *(ext.)* | |
| `completion_method` | `completionmethod` *(ext.)* | |
| ~~partition key~~ | **not a context attribute** | **C2 — the gateway derives the Kafka key. See §D3.** |

**The nine event types:**

| Spec enum | `type` | `subject` | Notes |
|---|---|---|---|
| `JOB_VIEWED` | `com.<org>.career.job-viewed` | `job_id` | spec #1 |
| `JOB_WISHLISTED` | `com.<org>.career.job-wishlisted` | `job_id` | spec #3 |
| `APPLICATION_STARTED` | `com.<org>.career.application-started` | `job_id` | spec #4/5; carries `referrertype` + `recommended_job_ids`; triggers the Queue team's recommendation consumer |
| `APPLICATION_STEP_COMPLETED` | `com.<org>.career.application-step-completed` | `job_id` | spec #9 |
| `APPLICATION_DRAFT_SAVED` | `com.<org>.career.application-draft-saved` | `job_id` | spec #2 "save and apply later" |
| `APPLICATION_SUBMITTED` | `com.<org>.career.application-submitted` | `job_id` | terminal |
| `APPLICATION_ABANDONED` | `com.<org>.career.application-abandoned` | `job_id` | **Queue team's Flink synthesises it, not us** (G1) |
| `USER_REGISTERED` | `com.<org>.career.user-registered` | `user_id_pseudo` | spec #7 |
| `USER_LOGGED_IN` | `com.<org>.career.user-logged-in` | `user_id_pseudo` | spec #7 |

**Content modes** — discriminated by `Content-Type` prefix:

| Mode | Content-Type | Use |
|---|---|---|
| structured | `application/cloudevents+json` | single event |
| **batched** | **`application/cloudevents-batch+json`** | **our default — a JSON array** |
| binary | anything else | attributes in `ce-*` headers — incompatible with batching, not used |

**Size cap: 64 KiB per event**, enforced on the **final serialized event** (G2). Base64 expands ciphertext
~33% plus 12-byte nonce and 16-byte tag per field, so the check runs *after* encryption. Ingress limit
provisionally 48 KiB; T6 measures the true figure.

**`sequence` scope (M12):** scoped per-`(source, user)`, documented in `CONTRACT.md`. We considered folding
the user into `source` (`/careers/<site>/u/<user_pseudo>`) to match the extension's multi-dimensional
guidance, and **rejected it**: `source` is the broker-level tenant filter in a multi-tenant SaaS, and
per-user `source` would fragment that. The extension explicitly permits an out-of-band agreement.

### D2 — Batching is mandatory, batches are single-tenant, and both are bounded

1. **Batching is mandatory** — 50,000 events/sec is unreachable from Python without it. Expressed as
   `application/cloudevents-batch+json`. HTTP keep-alive equally mandatory.
2. **A batch contains events for exactly one tenant.** One request carries one bearer token; accepting a
   multi-tenant batch would mean trusting `source` from the payload — the exact hole §D4 closes. Turns out
   to be a throughput win too: **one JWT verify per batch, not per event.**
3. **Both dimensions are bounded (H9):** max **500 events** and max **4 MiB per request body**. Violations
   return `413` **before allocation**. Published in `CONTRACT.md` and advertised on the endpoint — CloudEvents
   HTTP binding: *"the gesture SHOULD allow the receiver to choose the maximum size of a batch."*
   Without this, 48 KiB × 500 = a 24 MB body held by `await request.body()`; × 8 workers ≈ 192 MB of
   concurrent bodies from 8 requests.

### D3 — ONE topic, one derived key, and a real ordering guarantee

**Topic.** `SPEC.txt:133` proposes `career.events.<env>.<event_type>`; lines 313/379 propose one
`career.events.raw`. **Incompatible, and the first is wrong** — sessionization must order
`STARTED → STEP_COMPLETED → SUBMITTED` per `(tenant, user, session)`, and splitting across topics forces a
cross-topic merge-join that makes the 30-minute window a correctness problem.

**Decision: single `career.events.raw`.** `SPEC.txt:133` is **amended as a T2 deliverable** (M9), not merely
overruled in our doc — otherwise the Queue team reads the spec and we ship two incompatible implementations.
Event type is the `type` attribute, not a topic.

**Key — derived, not supplied (C2).**

> The gateway computes the Kafka message key as `<career_site_id from JWT>|<user_id_pseudo>` and
> **ignores any client-supplied `partitionkey`**. A client `partitionkey` that disagrees is rejected `403`,
> never honored.

`user_id_pseudo` is a deterministic HMAC, so partitioning is unchanged. The raw identifier never enters
Kafka. We set a **raw key string** and let Kafka's partitioner hash it — `SPEC.txt:314/379` say
`hash(career_site_id + user_id)`, so the contract must state this to prevent a **double hash** by the DB or
Queue team (M9).

**Ordering guarantee (C1) — what we can actually promise:**

> Events for the same `(career_site_id, user_id)` are produced in order **by a single gateway worker**,
> guaranteed by sticky routing (§D12). Ordering is **not** guaranteed across gateway restarts or if a
> worker dies mid-batch. **Consumers MUST order by the `sequence` extension**, which is authoritative
> and partition-order-independent.

Kafka idempotence is **necessary but not sufficient** — it orders within one producer's retry sequence and
says nothing about cross-producer interleaving. The original guarantee is only true because of §D12.

### D4 — Asymmetric JWT, JWT-only tenant binding, honest response semantics

**Algorithm (H2).** **EdDSA preferred, RS256 acceptable**, with a cached public key / JWKS. The gateway
holds only a *public* key and **cannot mint a token for any tenant**. r3.1's HS256 choice was justified by
"RS256 at 50k/s is a real tax" — arithmetic §D2 had already refuted, since batching makes this 1,000
verifications/sec, not 50,000 (~0.02 core-sec). The real HS256 cost was that a shared secret means a gateway
compromise is a total 500-tenant compromise.

**Tenant binding.** `career_site_id` from the **signed claim only**; a `source` in the body that disagrees →
`403`. `sourcechannel` derived from credential registration; `X-Source-Type` is a validated *hint* — mismatch
rejected, never honored. `career_site_id` must exist in the tenant registry (§D5) or the request is rejected.

**Response semantics (C4) — the plan's own drift removed:**

| Status | Meaning |
|---|---|
| `202 {accepted, rejected}` | **Accepted into the in-memory buffer. NOT a durability receipt.** |
| `413` | Batch exceeds 500 events or 4 MiB |
| `429` | Per-tenant rate limit; carries `Retry-After` |
| `503` + `Retry-After` | Buffer full, or broker unreachable — **fail loudly, never accept work we cannot hold** |

Clients **MUST** retry on `503` / connection reset using a stable `id`; `(source, id)` makes that idempotent.
This replaces r3.1's simultaneous claims of "no exactly-once" and "zero loss on `kill -9`".

### D5 — Encrypt PII, emit pseudonymous twins, separate the keys

CloudEvents *Privacy & Security* independently prescribes this: *"Sensitive information SHOULD NOT be
carried or represented in context attributes"* and *"Domain specific event data SHOULD be encrypted to
restrict visibility to trusted parties."*

| Field | Form | Downstream can |
|---|---|---|
| `user_id_pseudo` | `HMAC-SHA256(mac_key, raw_user_id)` | Join, group, count |
| `email_hmac` | `HMAC-SHA256(mac_key, email)` | Group, join. **Cannot reverse** |
| `email_enc` / `phone_enc` / `name_enc` / `gender_enc` | `AES-GCM(enc_key, …)` | Decrypt with tenant key |

**Purpose separation (H4).** One key for two primitives is a crypto-hygiene error. Derive **two** keys per
tenant from one HKDF call with distinct `info`:

```
enc_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"enc")
mac_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"mac")
```

**Key registry, not a cache (H4).** r3.1 cached derived keys on the JWT's `career_site_id` and justified it
with "50,000 HKDF calls/sec" — there are **500 tenants**, and even uncached this is ~0.1 core-sec. Worse, a
JWT-controlled cache key is an unbounded-growth DoS. Instead: derive all tenant keys **once at startup from a
fixed tenant registry** (which the per-tenant rate config needs anyway) and **reject tokens whose
`career_site_id` is not in it**. This bounds the cache, removes the derivation cost, and adds a
tenant-existence check — better on all three axes.

**AAD (Low fix).** Bind AES-GCM additional-authenticated-data to `(source, id, type, field_name, keyversion)`.
Free hardening: blocks ciphertext field-swap and cross-event transplantation.

**Security gate.** AES-GCM nonce reuse is catastrophic — it leaks the GHASH key. Fresh random 96-bit nonce
per encryption, never a resettable counter. Explicit test.

### D6 — Per-event rejection, post-encryption DLQ

**Pipeline order is now fixed and load-bearing (C3):**

```
decode → auth → validate → encrypt → produce
```

**The DLQ carries the POST-encryption event.** `error_context` carries `reason`, `field`, `index`, `stage`
and the spec's `exception_class` / `error_message` / `failed_at` / `retry_count` / `original_topic` /
`original_partition` / `original_offset` — but **never the offending value**. The DLQ is a CloudEvent too, so
the Queue team's replay worker needs no special-case parser.

Without this the DLQ is a **plaintext PII store** on a second Kafka topic, and re-injecting a DLQ event
double-encrypts it into undecryptable ciphertext that only the DB team would ever discover.

**Rejection is per-event, never per-batch** (§D2) — one tenant's garbage must not affect the other 499.
**Duplicate `id` within a batch is rejected (M13)** — otherwise a client reusing one `id` across 50 events
gets all 50 accepted, and the Queue team's `(source,id)` dedup silently collapses 50 real events into one.

### D7 — The driver models a funnel, not a firehose

```
JOB_VIEWED ─► JOB_WISHLISTED (optional)
  └─ APPLICATION_STARTED
       ├─ APPLICATION_STEP_COMPLETED ×n     (each MANUAL | RESUME_AUTOFILL | HYBRID)
       ├─ APPLICATION_DRAFT_SAVED            (spec #2)
       ├─ APPLICATION_ABANDONED              ← NOT ours; Flink synthesises it (G1)
       └─ APPLICATION_SUBMITTED
```

Tenant volume is **Zipfian** — `SPEC.txt:148` says enterprise tenants generate "exponentially higher traffic
during hiring drives than SMBs". Uniform traffic would hide exactly the hot-partition and rate-starvation
problems the spec worries about. Corpus is **partitioned by tenant** so every replay batch is single-tenant.

### D8 — The ground-truth ledger (single schema, defined in T2)

The ledger backs every headline claim, and r3.1 gave it **three different schemas in three documents** (M2).
It is now specified **once, in T2**, as part of `CONTRACT.md`:

**Schema:** `(source, id, type, tenant, user_pseudo, ts)` — the `(source, id)` pair is the dedup key
throughout; `id` alone is only unique within a source, and a shared ULID generator across tenants would
fail one assertion and pass the other.

Also specified in T2: on-disk format, and how sharded driver processes merge (the peak run is ~3M rows, the
soak ~15M — storage and join strategy must be stated before T8, not discovered during T11).

Used to assert: sent == accepted == Kafka == Cassandra · **duplicate `(source, id)` == 0** · DLQ count ==
exactly the injected invalid count.

### D9 — Driver runs in generate-and-replay mode

The driver is also Python, so generating 50k events/sec is a **second** load problem.

| Mode | Does | When | CPU |
|---|---|---|---|
| **generate** | Funnel FSM, Zipfian skew, payload shapes, `sequence` counters. Builds a fixed **replay corpus** + ledger. | Once, offline | Heavy, no deadline |
| **replay** | POSTs pre-serialized batches. No event construction. | Load tests | ~0, near-pure I/O |

Decouples "generate realistic events" from "drive 50k/sec" — which is what makes 50k reachable from Python
at all. Every run replays **identical** traffic, so results are comparable run-to-run.

**Hard requirement:** driver and gateway in **separate containers with CPU limits**. Driver reports *sent*,
gateway reports *accepted* — different quantities, and they must never share a core.

### D10 — Runtime topology

```
                          ┌───────────────────────────────────────────────┐
   TLS 1.3 terminates ───►│ sticky LB: hash(career_site_id|user_pseudo)  │  §D12
   at the LB               └───────────────────────┬───────────────────────┘
                                                  │  → one (tenant,user) = one worker
      ┌───────────────────────────────────────────▼───────────────────────────────────────────┐
      │ 1 + N uvicorn workers (N from T1) · uvloop + httptools                                │
      │   ├ Content-Type → mode detection (D1)                                                │
      │   ├ 1× JWT verify per batch, asymmetric (D4)                                          │
      │   ├ 500-bucket token bucket — correct BECAUSE of sticky routing (D12)                 │
      │   ├ bounded body (4 MiB) read first, 413 before allocation (D2)                       │
      │   └ decode → auth → validate → encrypt (D6) ──┐                                      │
      │                                               │ bounded queue; 503 when full (C4)    │
      └───────────────────────────────────────────────┼──────────────────────────────────────┘
                                                      ▼
                           ┌───────────────────────────────────────┐
                           │ dedicated producer thread per worker  │
                           │ confluent-kafka / librdkafka         │
                           │ delivery.timeout.ms = 5000 (D11)      │
                           └───────────────────┬───────────────────┘
                                               ▼
                             Kafka  career.events.raw (+ .dlq)
```

**TLS (M11).** `SPEC.txt:363` specifies TLS 1.3 and D10 places an LB in front. **The LB terminates TLS 1.3;
the gateway is internal-only.** No PII and no bearer token ever transits plaintext inside the perimeter.

### D11 — Kafka producer configuration (rewritten — no default is relied upon)

**r3.1's claim was wrong twice.** It said "leave on" / "leave at 5" / "leave" for four settings *in the same
paragraph that said never rely on a default*, and it asserted a librdkafka `linger.ms=0` divergence **that was
never verified**. The Java-client defaults it cited are correct for the Java client; `confluent-kafka` wraps
**librdkafka**, a different client.

**Operating rule: every value below is written explicitly.** Behaviour is then identical regardless of
client library or broker version.

| Config | Value | Rationale |
|---|---|---|
| `enable.idempotence` | **`true`** | Dedup + single-producer ordering. **Explicitly enabled, not defaulted** |
| `acks` | **`all`** | Required by idempotence |
| `retries` | **`2147483647`** | Required by idempotence |
| `max.in.flight.requests.per.connection` | **`5`** | Ordering preserved when idempotence is on. Default is far higher — must be lowered explicitly |
| `compression.type` | **`zstd`** | **Biggest single win.** JSON compresses extremely well. *"Compression is of full batches of data, so the efficacy of batching will also impact the compression ratio"* |
| `compression.zstd.level` | **`3`** (A/B to 22) | Measure, don't guess |
| `batch.size` | **`262144`** | Fewer, larger requests at 50k/s |
| `linger.ms` | **`10`** | Bounded extra latency for better batching |
| `buffer.memory` | **`67108864`** | Headroom for larger batches |
| **`delivery.timeout.ms`** | **`5000`** | **New, and load-bearing.** Default is 300,000 ms. At 50k/s a 5-minute timeout means up to **15M events** buffered in retry before anything surfaces. This is precisely how *"Python degrades slowly"* actually bites — Chaos 2's "fails loudly" would take five minutes |
| **`queuing.strategy`** | **`fifo`** | **New.** What actually preserves order in librdkafka's local queue |
| `partitioner.class` | default (key hash) | We set a raw key; Kafka hashes it |

**The real prohibition (M4) — and it is not a `ConfigException`.** r3.1 leaned on *"a `ConfigException` is
thrown"*, which only fires if someone **enables** idempotence. The realistic failure is someone **disabling**
it to chase throughput — and that is **silent**. Correct statement for `CONTRACT.md`:

> **`enable.idempotence=false` is forbidden.** With it off, `acks=all` + `delivery.timeout.ms=5000` +
> in-flight retries produce duplicates and reordering **with no error raised by any client**. Our
> `(source, id)` dedup and our ordering claim both depend on it.

T9 prints the producer's **effective** configuration — not the requested one — and asserts **every** key in
this table before the peak run.

### D12 — Sticky routing: the single fix for C1 and H1

The root cause of both Critical-class ordering and rate-limit failures was the same: **per-process
mechanisms were being claimed as system-wide properties.** One change removes both.

**The LB routes on `hash(career_site_id | user_id_pseudo)`, so every event for one user lands on exactly one
worker.** That worker owns:

- **one** Kafka producer ⇒ Kafka's per-partition ordering applies to that user's whole stream
- **one** token-bucket set ⇒ the configured per-tenant rate is the *actual* rate, not N× it

**Consequences, stated honestly:**

| Property | Withsticky routing | Without |
|---|---|---|
| Per-`(tenant, user)` ordering | **Guaranteed** (single producer) | **Not guaranteed** — nondeterministic across producers |
| Per-tenant rate limit | **Accurate** | **N× the configured rate** (H1) |
| Load distribution | Only as even as the user hash | Even |
| Restart / worker death | In-flight batch may reorder or be lost | Same |

**Costs:** per-user hot spots concentrate on one worker (mitigated by the Zipfian distribution and by
50k aggregate being divisible across workers); a worker restart affects that user's stream. Both are
acceptable; the alternative — a guarantee we cannot keep — is not.

T9 **proves both properties** with tests that fail without sticky routing (§ tasks).

---

## 3. Dependency Graph

```
T1  Python spike ── decides: batch floor, worker count N, ceiling
             │
             ▼
T2  Contract v0.1 (MVS ships immediately — does NOT block T3)
             │
             ▼
T3  Vertical slice #1 + driver v0 + ground-truth ledger
             │
             ├──────────────┬──────────────┬─────────────┐
             ▼              ▼              ▼             │
T4  Asymmetric auth  T5  Validate+DLQ  T6  Encryption   │
             │              │              │             │
             └──────────────┴──────────────┘             │
                          ▼                              │
                    T7  Token bucket ──► T10a /metrics ───┤
                                       (P0, 1h)         │
                          │                              │
T8a Funnel FSM + corpus ──┤ (depends on T3 only)        │
T8b 500-tenant skew ──────┘                              │
                          │                              │
                          └──────────────┬───────────────┘
                                         ▼
                          T9  Replay + peak 50k/s + ordering proof
                                         │
                            ┌────────────┴─────────────┐
                            ▼                          ▼
                  T11  Verify + chaos (P0)     T10b  Dashboard/CLI
                                                  (P1, cuttable)
```

**Key changes from r3.1 (M1, H7):**

- **T8 depends only on T3** — it is now drawn as a **parallel branch**, not downstream of T7. r3.1's graph
  contradicted its own prose and `todo.md`; for a 1-person team this is the difference between parallelising T8
  and not.
- **T10a (`/metrics`) is P0, ~1h, depends on nothing, and must precede T9** — T9's acceptance criteria
  (CPU < 80%, p99 bounded, lag bounded) are unmeasurable without it.
- **T11 depends on T9 only** — `verify` needs the ledger and Kafka plumbing, not a dashboard. T10b is now a
  true leaf that can be dropped without consequence.
- **T3 no longer waits on cross-team acknowledgement** (M3).

---

## 4. Task List

Detail in **`tasks/todo.md`**.

| # | Task | Size | Est | Priority |
|---|---|---|---|---|
| T1 | Python throughput spike | XS | 3-4h | **P0** |
| T2 | Contract v0.1 + ledger schema + spec amendment | M | 5-6h | **P0** |
| T3 | Vertical slice #1 + driver v0 + ledger | M | 6-7h | **P0** |
| T4 | Asymmetric auth, JWT-only tenant binding | S | 3-4h | P0 |
| T5 | Validation + post-encryption DLQ | M | 5-6h | P0 |
| T6 | Encryption: key separation, registry, AAD | M | 6-7h | P0 |
| T7 | Token bucket (correct under D12) | S | 3-4h | P0 |
| **T10a** | **`/metrics` — counters, per-worker, per-type** | **XS** | **1-2h** | **P0** |
| T8a | Driver: funnel FSM, payload shapes, corpus | M | 5-6h | P0 |
| T8b | Driver: 500-tenant Zipfian skew, sharding | S | 3-4h | P1 |
| T9 | Replay mode + peak 50k/s + ordering proof | M | 7-9h | **P0** |
| T11 | Verification oracle + chaos demos | M | 5-6h | **P0** |
| T10b | Dashboard, CLI live view, inspector, `make demo` | M | 3-4h | P1 |

**Total: 55-69h** (sum of the table). This exceeds a realistic 1-person allocation. See §4.1.

### 4.1 Cut list, re-ranked (M10 + H7)

Ordered by *return per hour lost*:

1. **T10b** — dashboard, CLI live view, inspector, `make demo`. First cut. T10a is already separate and stays.
2. **Shorten the soak from 10 min to 3.** It is a *duration*, not a feature — zero functionality lost, ~7h of
   wall-clock back. **Best ratio on the entire list**, and r3.1 missed it.
3. **T8b's third payload shape (third-party webhook) → 1.** Two shapes still demonstrate "browser, mobile,
   third-party" adequately; the third is the most expensive and least load-bearing.
4. **T7's fairness *demo*** — only after the above. It is a script, and it is the proof that the load shedder
   works, so cutting it saves little and costs credibility.
5. **T4's `X-Source-Type` hint validation** — last.

**Never cut: T1, T2, T3, T6, T9, T10a, T11.**

**Lean demo path (~35h, if the hackathon is short):** T1 → T2 → T3 → T6 → T10a → T8a → T9 → T11. Skips T4
(dev-mode bypass with a loud warning), T5, T7, T8b, T10b. **T7 must be present even in the lean path as a
rate limit, though its fairness demo is dropped** — it is the Python survival mechanism (§1.3), not a feature.

---

## 5. Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| **Ordering guarantee cannot be kept** | **High** | **§D12 sticky routing** makes it true. T9 proves it with a test that fails without it. `sequence` is the authoritative fallback. |
| **Python cannot reach 50k/s on the demo box** | **High** | T1 first, on the actual hardware. Corrected budget (§1.1). Fallback ladder leads with the honest small-box answer. |
| **Batching requirement ignored by the UI team** | **High** | Hard requirement in `CONTRACT.md` with the math. Our driver always batches, so the load test passes; the risk is production-shaped, not demo-shaped. |
| **Driver and gateway contend for the same cores** | **High** | Separate containers, CPU limits (D9). |
| **Rate limiting silently N× in production** | **High** | §D12. Without sticky routing this is invisible to every test — which is why T7 gains a regression test. |
| **Someone disables idempotence** | **High** | **Silent, not a `ConfigException`** (D11). Named a prohibition. T9 asserts the effective config. |
| **Downstream (Flink) can't keep up; lag grows invisibly** | **High** | T9 requires a running consumer and **bounded, reported end-to-end lag**. If downstream can't keep up, **the demo number is reduced, not the assertion**. §7 Q1. |
| **DLQ becomes a plaintext PII store** | **High** | Pipeline order fixed (D6); DLQ payload is post-encryption; canary test (T5, T6). |
| Spec contradiction: per-event-type topics vs single topic | **High** | D3, and **`SPEC.txt:133` is amended as a T2 deliverable** (M9) — not merely overruled in our doc. |
| **CloudEvents adoption overruns the budget** | Medium | T2 ships **v0.1 with the minimum viable subset** and does not block T3 (M3). `keyversion` is droppable. |
| `jsonschema`/Pydantic chosen instead of msgspec | High | Locked in T2; T1 measures the delta. |
| `kafka-python` chosen instead of confluent-kafka | Medium | Locked in T2, verified in T3. |
| Python degrades slowly under overload | Medium | Bounded queues with defined full-behaviour, 429 shedding (T7), `delivery.timeout.ms=5000` (D11). |
| Flink 30-min watermark vs demo duration | Medium | Driver supports **accelerated session time**; coordinate with Queue team. |
| Cross-team schema churn | Low | Versioned via `dataschema`; additive-only; one owner (us). |
| *(FYI, DB team)* `application_funnel_sessions` PK `((career_site_id, job_id), …)` will exceed the 100 MB partition limit on a popular job | Medium for DB | `SPEC.txt:149` bucketing advice was not applied there. **Warn the DB team** — our Zipfian traffic is exactly what triggers it. |

---

## 6. Deliberately Not Building

- **No Flink / stream processing** — Queue team's slice. Includes `APPLICATION_ABANDONED`, which they
  synthesise on the watermark timeout, not us (D1, G1).
- **No Cassandra access or writes** — DB team's slice.
- **No recommendation engine / vector DB** — `SPEC.txt:342-343`, Queue team's slice.
- **No autofill DOM SDK** — **UI team.** We ship the `completionmethod` enum + detection contract only.
- **No k6 / Gatling / Locust** — cannot model the funnel FSM, the payload shapes, or the ground-truth ledger.
- **No CloudEvents binary content mode** — incompatible with mandatory batching (D1).
- **No Go/Rust sidecar** unless T1 forces it (pre-decided fallback step (c)).
- **No clustering** unless T1 shows one host cannot hold 50k with headroom. "Aggregate" permits multiple
  instances; it does not require them.
- **No exactly-once end-to-end** — at-least-once + `(source, id)` LWW idempotency, with `202` honestly
  documented as *not* a durability receipt (C4).
- **No admin UI** — CLI + one operator-only decrypt endpoint.

---

## 7. Open Questions — needed before T1

1. **Is 50,000 events/sec a hard requirement, and how many demo-box cores are ours after Kafka, Flink and
   Cassandra?** *(M8 + H8.)* The 50k figure appears **nowhere in `SPEC.txt`** — it is user-supplied, and it
   sets the entire risk posture including the sidecar fallback. Separately: r3.1's capacity plan assumed
   6-8 workers, which is wrong below ~8 physical cores, and nobody has budgeted cores for the other three
   teams or for consumer lag. **This is a five-minute conversation that can change the plan's shape.**
2. **Kafka version (3.x or 4.x) and deployment (single broker or 3-broker)?** The 4.0 client default changes
   are client-side and librdkafka does not necessarily share them. We set everything explicitly regardless,
   but this changes the honesty of the durability story — on a single broker, ISR is 1, so `acks=all` is cheap
   but weaker. T9 also needs the **librdkafka** configuration reference, which r3.1 flagged as unconsulted
   and this revision still has not independently verified.
3. **Minimum batch size to mandate.** T1 produces it; the UI team needs it early. Provisional: **≥ 20, target
   ≥ 50.** Both **500 events** and **4 MiB** caps are published alongside it.
4. **Where does the master secret live, and who may decrypt?** *(M5.)* §D8 hands the DB team
   `HKDF(master, career_site_id)`, which is undecryptable without the master — a secret-distribution decision
   currently buried in a handoff table. `SPEC.txt:347` implies only `candidate_profiles` is decrypted under
   RBAC, which suggests **the gateway is the sole key holder** and the DB team calls the decrypt endpoint.
   Needs confirming; it is security-critical and currently unstated.
5. **Single instance or multi?** — T1 decides.
6. **Who owns `APPLICATION_ABANDONED`?** — plan assumes Flink. Confirm, or the drop-off metric double-counts.
7. **Avro/Protobuf or JSON?** — CloudEvents JSON assumed. Confirm nobody is building an Avro schema registry
   expecting raw Avro records.

---

## 8. Handoffs We Owe the Other Teams

| Artifact | To | When |
|---|---|---|
| `CONTRACT.md` v0.1 — CloudEvents 1.0.3 profile: single topic, **derived** key, honest ordering contract **and its sticky-routing precondition**, `(source, id)` dedup, **mandatory batching + single-tenant + 500/4 MiB caps + keep-alive**, 64 KiB cap, asymmetric JWT, `202`≠durable / `503` semantics, **`enable.idempotence=false` prohibition**, `sequence` scope, **live-client `sequence` contract**, and the ledger schema | Queue + DB + **UI** | T2 |
| **`SPEC.txt:133` amended** to `career.events.raw` with rationale, plus a note that we set a raw key and let Kafka hash it (prevents a double hash) | Queue | T2 |
| `event.schema.json` **generated from msgspec structs** + 10+ example payloads | Queue + DB | T2 |
| Kafka producer settings per **D11** + A/B measurements + effective-config dump | Queue | T9 |
| Measured peak, ceiling, p50/p99, error rates, **end-to-end lag** | Everyone | T9 |
| PII key-derivation `HKDF(master, salt=site, info=enc\|mac)` + pseudonymous-twin semantics + AAD binding | DB | T6 |
| Rate-limit semantics (429 / 503 shapes, `Retry-After`) | UI | T7 |
| `completionmethod` enum + autofill detection contract | **UI** | T8a |
| Client SDK batching, caps, keep-alive, CloudEvents envelope, `sequence` maintenance | **UI** | T3 |

---

## 9. Sources

- CloudEvents v1.0.3-wip — core spec, JSON format (§4 JSON Batch Format), HTTP binding (structured / binary /
  batched), Partitioning + Sequence extensions, Privacy & Security — `github.com/cloudevents/spec`
- CloudEvents project status (CNCF *Graduated*) — `cloudevents.io`
- msgspec README (zero-cost validation, benchmarks) — `github.com/msgspec/msgspec`
- Apache Kafka 4.1 Producer Configs — `kafka.apache.org`. **Java client only.** librdkafka's
  `CONFIGURATION.md` was **not** independently verified in this revision; D11 therefore sets every value
  explicitly rather than relying on any default.
- Python 3.14 free-threading status — `docs.python.org`, community reports
- Segment common event spec (field-mapping cross-check) — `twilio.com/docs/segment`
- OpenTelemetry Logs Data Model (envelope cross-check) — `opentelemetry.io`
- Independent code review of r3.1 (4 Critical, 9 High, 13 Medium) — outcomes in §0.1
