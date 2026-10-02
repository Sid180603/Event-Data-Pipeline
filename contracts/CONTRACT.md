# Ingestion Contract v0.1

**Owner:** Event Generator team. **Consumers:** Queue (Kafka + Flink), Database (Cassandra), UI.
**Wire format:** [CloudEvents](https://cloudevents.io) 1.0 (CNCF Graduated) — required by `SPEC.txt:131`.
**Schema of record:** the structs in `contracts/ingress.py` and `contracts/cloudevent.py`. The
`.schema.json` files are **generated** from them; `python -m contracts.gen_schema` writes
`event.schema.json` and the test suite fails if either drifts.

Every claim in this document is traceable to code. Where something could not be established from the
code, it says **"not established here"** rather than carrying a number this project did not measure.
**This project claims no throughput and publishes no benchmark.** The full bundle, including what each
team owns and what it is handed, is `docs/handoff.md`.

---

## 1. Two schemas, and which is which

This is the single most important thing to get right when writing a client, so it comes first.

| | **Ingress** — what you POST | **Egress** — what lands on Kafka |
|---|---|---|
| Struct of record | `contracts/ingress.py` (`IngressEvent`) | `contracts/cloudevent.py` (`CloudEvent`) |
| Schema file | `contracts/ingress.schema.json` | `contracts/event.schema.json` |
| `data.candidate.user_id` | your raw per-user id | — |
| `data.candidate.user_id_pseudo` | — | `HMAC-SHA256(mac_key, user_id)`, lowercase hex |
| `data.candidate.email` | plaintext | — |
| `data.candidate.email_hmac` | — | HMAC, for grouping |
| `data.candidate.email_enc` | — | AES-GCM ciphertext |
| Other plaintext PII | `phone`, `alternate_phone`, `name`, `gender` | — |
| Other ciphertexts | — | `phone_enc`, `alternate_phone_enc`, `name_enc`, `gender_enc` |
| Not encrypted, carried through | — | `experience_status`, `years_of_experience`, `education_degree`, `education_branch` |
| Who produces it | **you** | the gateway |

Exactly five fields are encrypted (`app/crypto/facade.py: PII_FIELDS`) and exactly
**two** values are HMACed: `user_id_pseudo`, from your `user_id`, and `email_hmac`,
from your `email`. `experience_status`, `years_of_experience`,
`education_degree` and `education_branch` are **neither encrypted nor
pseudonymised** — they are published as plaintext, because they are the
non-identifying columns the analytics path aggregates on. That is a deliberate
field-level decision, not an oversight: read it before deciding whether a field
belongs in the candidate block at all.

**A client cannot hold a tenant key, so it sends plaintext.** The gateway encrypts on the way in.
The two shapes are deliberately different structs, and both reject the other's PII fields — you
cannot post a ciphertext you made up, and you cannot receive a plaintext one.

**Do not send `*_enc`, `*_hmac`, or `user_id_pseudo`.** `IngressCandidate.forbid_unknown_fields`
refuses all three, and the reason you get is **`SCHEMA at $.data.candidate`** — *measured*, by
posting each of the three through `ingest_batch` and reading the `202`'s `rejected` list. It is
**not** `UNKNOWN_ATTRIBUTE`: that code is reserved for an unknown attribute at the *top level* of
the envelope (`UNKNOWN_ATTRIBUTE at $[0]: source_channel`), because `extra_attributes` only looks
at the envelope's own keys and not inside `data`. The gateway must never publish ciphertext it did
not produce, and an `email_hmac` from a client would let a caller choose how a candidate is grouped
in analytics.

**`user_id` vs `user_id_pseudo` are not interchangeable.** `user_id` is your raw identifier;
`user_id_pseudo` is an HMAC the gateway computes. They are named differently so the two are never
confused in a log line or a key.

Worked examples are in `contracts/examples/` — ten files, all nine event types, **in the ingress
shape only**, because that is the only shape a client can send. There is no published egress
example file; §1's table above is the egress shape, and `contracts/cloudevent.py` is what defines
it. Every example is decoded and envelope-checked in CI.

Both schema files are **generated** and the suite fails on drift:
`python -m contracts.gen_schema` writes `event.schema.json`; `ingress.schema.json`
is produced by `contracts/ingress.py: generate_ingress_schema_json` and is checked
the same way (`contracts/test_contracts.py`, `test_the_ingress_schema_is_published_and_not_stale`).
The ingress file is self-contained — its `$defs` travel with the root — while
`event.schema.json` is a bare `$ref` into a `$defs` block the generator does not
emit, so **a third-party validator cannot resolve `event.schema.json` on its own**.
Code against `contracts/cloudevent.py` for the egress shape; that is the struct
the pipeline enforces.

## 2. Endpoints

Four routes. Three are the gateway's own; `/v1/decrypt` is operator-only and is
the only one that returns plaintext PII.

| Method | Path | Purpose | Auth |
|---|---|---|---|
| `POST` | `/v1/ingest` | Ingest a CloudEvents batch | `Authorization: Bearer <tenant JWT>` |
| `POST` | `/v1/decrypt` | Decrypt **one field of one event**. Returns plaintext PII | `X-Operator-Key: <operator key>` |
| `GET` | `/metrics` | Prometheus exposition (`text/plain; version=0.0.4`). Most counters and gauges carry a `worker` label; the four histograms carry none, and `gateway_label_values_dropped_total` is labelled by `label` alone | none |
| `GET` | `/healthz` | Liveness only. `{"status": "ok", "worker": ...}` | none |

`/healthz` deliberately does **not** check the broker: a gateway that answers
"no" when Kafka is down gets restarted by its orchestrator, which is a worse
outage than one waiting for a broker (`app/main.py`).

### `POST /v1/ingest` — request

- `Content-Type: application/cloudevents-batch+json` — a JSON **array** of CloudEvents.
  The header is **not enforced**; the body is decoded as a JSON array regardless,
  and an absent `Content-Type` is accepted (a body that is not a JSON array is a
  `400`, not a `415`).
- `Authorization: Bearer <JWT>`
- `X-Source-Type: WEB_APP | MOBILE_APP | THIRD_PARTY_SERVICE` — a **hint** only. The
  published `sourcechannel` comes from the credential registration, and a hint
  that disagrees is `403`.
- `Ce-Request-Delivery-Id` is **not** used; idempotency is carried per-event in `id`.

### `POST /v1/decrypt` — the plaintext endpoint

Everything about this one differs from `/v1/ingest`, and the differences are the
point:

- **A different credential.** `X-Operator-Key`, compared with
  `secrets.compare_digest`. It is **not** a tenant JWT and is **not** verified
  with the tenant verifier, so a valid tenant token presented here is refused.
  A tenant can never ask for another tenant's plaintext. The gateway refuses to
  start if the operator key is empty — a fail-open on the one credential guarding
  plaintext PII is not a runtime surprise to discover.
- **Its own rate limit**, separate from every tenant's ingest budget
  (`DECRYPT_LIMIT`, 10/sec burst 10). Decryption is a key operation and a cheap
  oracle. A tenant cannot spend the operator's budget.
- **Body: `DecryptRequest`, and it is NOT a batch.**

  ```
  { "career_site_id": "...", "event": { ...one CloudEvents egress event... },
    "field": "email_enc" }
  ```

  One event, one field, `forbid_unknown_fields`. An operator asking for 500
  fields is asking for an export, which is a different and differently-authorised
  thing.
- **The whitelist is exactly five fields** (`DECRYPTABLE_FIELDS`): `email_enc`,
  `phone_enc`, `alternate_phone_enc`, `name_enc`, `gender_enc`.
  **`user_id_pseudo` and `email_hmac` are HMACs and are NOT decryptable** — they
  are not on the list, and asking for them is a `400 UNKNOWN_FIELD`.
- **Every attempt is audit-logged**, not every success: successes, denials, rate
  limits and failures alike. The record is `DecryptAudit` and carries exactly
  `actor`, `career_site_id`, `source`, `id`, `field`, `outcome`, `at` — and **never
  the value**. A denied caller still produces a record, with `actor` = `unidentified`.
- **The value is returned in the body and nowhere else**, with
  `Cache-Control: no-store`. Not logged, not audited.
- The ciphertext authenticates against AAD `(source, id, type, field, keyversion)`,
  so pointing tenant A's key at tenant B's event fails authentication rather than
  returning a plausible wrong value.

Status codes: `200` · `400` (`BAD_REQUEST`, `UNKNOWN_FIELD`, `DECRYPT_FAILED`) ·
`401` (+`WWW-Authenticate: OperatorKey`) · `404` (`UNKNOWN_TENANT`) ·
`429` (+`Retry-After`).

### Limits — hard, enforced before allocation

These are the **caps**, and the two ways they bite are different, so do not read the "Violation"
column as one rule. **A *request* over `MAX_BATCH_BYTES` or `MAX_EVENTS_PER_BATCH`, or carrying an
*event* over `INGRESS_EVENT_BYTES`, is refused whole with `413`** (`app/ingest/limits.py`, before
anything is parsed). **An *event* over `MAX_EVENT_BYTES` is refused per event** — `OVERSIZED`, into
the DLQ, in the `rejected` list of the `202`, while the rest of the batch proceeds
(`app/ingest/pipeline.py` stage 6). The distinction is deliberate: the first is a client capacity
error, the second is one event's payload.

| Limit | Value | Violation |
|---|---|---|
| Events per batch | **500** | request over → `413`, whole batch |
| Request body | **4 MiB** | request over → `413`, whole batch |
| Event size, **after** encryption | **64 KiB** | event over → per-event `OVERSIZED` → DLQ |
| Ingress event size (plaintext) | **48 KiB** | request carrying one → `413`, whole batch |

The CloudEvents HTTP binding expects a receiver to advertise its maximum batch size. These are that
advertisement.

**The 48 KiB ingress cap is measured, and it is not a guarantee about the 64 KiB ceiling.**
`app/test_config.py` pushes cap-sized events through the real pipeline and reads the encrypted
event's length off the sink:

| Where an event's bulk sits | Ingress | Egress | Headroom vs 64 KiB |
|---|---|---|---|
| `data.event_payload.client_metadata` — the only unencrypted field the measurement was actually run against | 49,152 B | 49,380 B | **16,156 B** |
| Spread evenly across all five encrypted PII fields | 49,152 B | 65,923 B | **−387 B (over)** |

`recommended_job_ids` and `subject` are also carried through unencrypted and would behave like
`client_metadata`, but they were **not** the shape measured; treat them as the same shape by
inspection, not by measurement.

The largest fully-encrypted ingress event that still fits is **48,855 B**, so the **top 297 B of the
cap is admitted and then refused** — per event, with `OVERSIZED`, into the DLQ, while the rest of the
batch proceeds. Reproduce all four numbers with `python -m pytest app/test_config.py -s -q`, which
prints them; nothing in this table is a calculation done in this document.

The cap was left at 48 KiB rather than lowered to 48,855 B: it is a published contract value,
changing it changes what clients may send, and an oversized event is refused rather than admitted to
the topic either way. **This is a decision for the consumer teams, not a settled fact — see
`docs/handoff.md` §7.** **Practical rule for a client: keep the five encrypted fields small.** A
48 KiB event that is mostly `client_metadata` is fine; a 48 KiB event that is mostly candidate PII is
refused.

Note also that this is the only path that yields `OVERSIZED` in the DLQ. The other size check,
`app/validate/events.py: validate_batch`, also emits `OVERSIZED`, but it measures the *contract form*
— the request as it arrived, with `user_id_pseudo` standing in for `user_id` and the plaintext PII
dropped — which is bounded by the 48 KiB ingress cap and therefore cannot reach 64 KiB.

> **Client guidance, not a cap:** the UI SDK should flush at **≤ 200 events / ≤ 2 MiB**. That is a
> recommended operating point that leaves headroom below the 500 / 4 MiB ceiling. Sending 500 events
> is legal and will not be refused.

`app/config.py` is the single source of truth for all four numbers (`MAX_EVENTS_PER_BATCH = 500`,
`MAX_BATCH_BYTES = 4 MiB`, `INGRESS_EVENT_BYTES = 48 KiB`, `MAX_EVENT_BYTES = 64 KiB`).

## 3. Response semantics - read this before writing a client

| Status | Meaning |
|---|---|
| `202 {accepted, rejected}` | **Accepted into the in-memory buffer. This is NOT a durability receipt.** |
| `400` | Body is not a JSON array at all (`BODY_NOT_A_BATCH`) |
| `401` | Missing / malformed / expired / bad-signature / wrong-audience JWT, or the unauthenticated malformed-token budget is exhausted |
| `403` | Body `source` ≠ token tenant · unknown tenant · `X-Source-Type` disagreeing with the registered credential · `partitionkey` mismatch |
| `413` | Batch over 500 events, over 4 MiB, or carrying an event over 48 KiB. **Never** the way an event over 64 KiB after encryption is answered — that is per-event, in the `202` (§2 limits) |
| `429` + `Retry-After` | Tenant's event budget exhausted. **The whole batch is denied.** |
| `503` + `Retry-After: 1` | Producer buffer full, or broker unreachable. `reason` is one of `PRODUCER_QUEUE_FULL`, `PRODUCER_CLOSED`, `SINK_UNAVAILABLE`, `DLQ_UNAVAILABLE` |

`accepted` and `rejected` are counts and per-event `{index, reason}` pairs; **every `reason` is a
code or a code plus a JSON path** (`SCHEMA at $.type`, `OVERSIZED at $[3]: exceeds 65536 bytes`), never
a message quoting the offending value — a reason is written to a Kafka topic.

**Clients MUST retry on `503` and on connection reset, reusing the same event `id`.** Retries are
idempotent under the dedup rule in §5. Note that idempotence is a property of the *producer's* own
retries: the gateway does **not** dedup across requests, so your consumer is the only place
`(source, id)` dedup can happen (§5).

**Rate limiting is charged per event but denied per batch.** Each event in the batch costs one token
from the tenant's bucket. If any event cannot be afforded, the **entire batch is refused with `429`**
rather than partially accepted. Three reasons: a `429` is an HTTP request-level signal and a partial
one is incoherent; the batch is a unit the client chose to send; and because `id` is stable, retrying
the whole batch is free of duplicates. Partial acceptance would leave the client unable to tell
whether the events it did not receive are rejected, deferred, or lost.

**A `429` does NOT produce a DLQ entry.** A rate-limited batch was not poisoned — it will succeed on
retry, so filing it in the DLQ would bury real poison events under retry noise. It is not silently
dropped either: the client receives an explicit `429` and the ground-truth ledger in §12 records the
events as sent-but-not-accepted, so reconciliation shows a shortfall rather than a loss.

`202` deliberately does not mean "durably in Kafka". With a bounded in-memory buffer
(`PRODUCER_QUEUE_MAXSIZE = 50,000` records, `queue.buffering.max.kbytes = 64 MiB`,
`_IN_FLIGHT_HIGH_WATER = 1,000`), a gateway killed mid-flight loses the un-acked window and the
caller already holds a `202`. The constant that names this is
`DURABILITY_ACCEPTED_INTO_BUFFER = "accepted-into-buffer"`. **The size of that window is not
established here** — no run in this project measured it. The procedure for observing it is
`scripts/chaos/kill_gateway.sh`, which kills the gateway mid-flight and then runs
`python -m tools.verify` over what survived; `make chaos` runs that beat and three others, in order.
That is how to produce the figure. It is not one.

## 4. Pipeline order — load-bearing, and what the DLQ therefore holds

```
decode → auth → validate → encrypt → produce
```

Order in full (`app/ingest/pipeline.py`): **1** batch limits → **2** decode → **3** auth →
**4** validate → **5** rate limit → **6** encrypt → **7** DLQ → **8** produce.

Auth precedes every per-event decision and every write, so an unauthenticated caller can neither
learn a rejection verdict nor make the gateway write a DLQ record. Rate limiting precedes encryption
and the DLQ, because a `429` is load-shedding, not a poison pill.

### ⚠ The DLQ carries two different payload shapes, and one of them is plaintext PII

**This is the sentence most likely to be checked by an auditor, so it is stated exactly.**
The stage order decides which shape a rejection was stored in:

| Rejected… | `error_context.stage` | `original_payload` holds |
|---|---|---|
| **after** encrypt — encrypted, then crossed the 64 KiB ceiling | `"encrypt"` | the **encrypted** event, ciphertexts and all |
| **during** validation — never encrypted | `"validate"` | the **plaintext request element**, PII and all |

**So a validation-stage DLQ record does contain plaintext PII.** This is a consequence of the order
above and it is accepted deliberately: encrypting an event already judged invalid would be work spent
on a dead record, and redacting it would leave an operator holding a DLQ record they cannot diagnose.
**Anyone setting retention or access policy on `career.events.dlq` must treat it as a PII store.**
Encrypting before validating would remove it, at the cost of spending crypto on every malformed
event in the batch.

### What is true of every DLQ record, regardless of stage

- `error_context` carries `reason`, `failed_at`, `exception_class`, `field`, `index`, `stage`,
  `retry_count`, and `original_topic` / `original_partition` / `original_offset` — and
  **never the offending value**. The `reason` is a code plus a JSON path; msgspec puts the offending
  value in the *front* of its message and the path at the end, and only the path is ever read
  (`app/validate/events.py: _error_path`).
- The envelope is a CloudEvent (`type = com.careerpage.career.ingest-rejected`, `id = dlq-<hex>`), so
  a replay worker needs no special parser. A malformed `source` in the payload falls back to
  `/careers/_unknown` rather than propagating as if it were a tenant.
- **The DLQ key is the tenant `source`, not the derived user key** — a rejected event frequently has
  no usable `user_id_pseudo` (often that is *why* it was rejected), and DLQ consumers group by tenant.
- **A DLQ write that fails is a `503`, never a silent drop** (`DLQ_UNAVAILABLE`): the event is not
  lost, because the client resends the whole batch with the same `id`s.
- `reason` codes: `SCHEMA`, `UNKNOWN_ATTRIBUTE`, `DUPLICATE_ID`, `MIXED_TENANT`, `BAD_TIME`,
  `BAD_SOURCE`, `BAD_ID`, `RAW_SUBJECT`, `OVERSIZED`.

### Replay: `dlq_data_to_ingress` round-trips each shape to the struct it was stored as

- A **validate-stage** record round-trips to an `IngressEvent` and is re-submitted, so it is
  **encrypted exactly once** on re-injection.
- An **encrypt-stage** record round-trips to a `CloudEvent` and **must not be re-encrypted** — that
  would produce undecryptable ciphertext.

`dlq_data_to_ingress(data, target=CloudEvent)` is the whole mechanism; `target` is the shape the
payload was stored in. Getting this wrong in the replay worker is a double-encryption bug, and it is
the Queue team's to get right.

## 5. Idempotency and dedup

- **Dedup key is `(source, id)`** — the CloudEvents rule: *"Consumers MAY assume that Events with
  identical `source` and `id` are duplicates."*
- `id` alone is **not** sufficient: it is only unique within a source.
- `id` is **client-generated** and must be stable across retries.
- **Cross-request `id` uniqueness is the client's responsibility** ("Producers MUST ensure that
  `source` + `id` is unique").
- **Duplicate `id` within a single batch is rejected** — reason code `DUPLICATE_ID`, per-event, in
  the `rejected` list of the `202`, and the survivor is kept — because silently accepting them would
  let downstream dedup collapse N real events into one.
- **What the gateway does *not* do:** it never dedups across requests. Its only duplicate check is
  within one batch. So a `503` + resend puts the same `(source, id)` on the topic twice **by
  design**, and `enable.idempotence=true` does not prevent it — that covers the *producer's* retries,
  not your HTTP retry. **Your consumer is the only place `(source, id)` dedup can happen.**

### The nine event types

`com.careerpage.career.` + one of: `job-viewed`, `job-wishlisted`, `application-started`,
`application-step-completed`, `application-draft-saved`, `application-submitted`,
`application-abandoned`, `user-registered`, `user-logged-in` (`contracts/cloudevent.py: EVENT_TYPES`).
The DLQ's `ingest-rejected` type is deliberately **not** in that list — it is an internal type and the
published wire contract stays untouched by it.

## 6. Ordering — what we actually promise

> **The load-balancer-level part is not implemented in this repo, and this project does not claim it.**
> `Settings.sticky_routing` exists and reads `STICKY_ROUTING` (set to `"1"` in compose), but **no code
> path reads the setting** — there is no consistent-hash layer between the listener and the workers,
> because in this deployment there is none to put there: uvicorn `--workers 4` shares one listening
> socket and the kernel hands each connection to whichever worker accepts it.
>
> **Therefore what is actually guaranteed is the weaker, partition-level half:**
> `derive_kafka_key(career_site_id, user_id_pseudo)` is computed by the gateway and
> `partitioner=consistent_random` hashes it, so **every event for one `(tenant, user)` pair lands on
> one Kafka partition.** Kafka preserves append order within a partition for a single producer.
>
> Ordering is **not** guaranteed across gateway restarts, if a worker dies mid-batch, or if two
> different workers produce for the same `(tenant, user)` pair — which, with no affinity layer, is
> possible.
>
> **Consumers MUST order by the `sequence` extension**, which is authoritative and
> partition-order-independent. `sequence` is a zero-padded, monotonically increasing counter scoped per
> `(source, user)`.

**Why `sequence` is not optional:** the gateway runs multiple worker processes, each with its own Kafka
producer and its own idempotence/producer-id domain. Kafka orders by partition-append within one
producer; across independent producers the order is nondeterministic. Partition affinity is what makes
single-producer ordering *usually* hold, and `sequence` is the only thing that survives a restart or a
two-worker race. **The Queue team's Flink sessionization must use `sequence`, not partition order.**

Two observable consequences, both of which the other teams should know:

- `gateway_ordering_violations_total` counts events whose `sequence` went **backwards** for a
  `(source, user)` pair, over a bounded high-water window. `gateway_ordering_unchecked_total` counts
  only records the window could not compare at all — a `sequence` that is absent or non-numeric.
  Eviction is counted **separately**, in `gateway_sequence_evictions_total`, because each eviction
  can hide a later violation. A zero violation count is only evidence when the unchecked count is
  small *and* the eviction count is small.
- `tools/verify.py: ordering_violations` recomputes the same thing end-to-end from the topic, applying
  the same rule. Sticky/affinity failure shows up there as violations rather than being smoothed over.

## 7. Kafka

- **One topic: `career.events.raw`** (12 partitions, RF 1, `compression.type=zstd`, `retention.ms`
  6 h). DLQ: **`career.events.dlq`** (3 partitions, RF 1, 24 h). Both created by `kafka-init`
  (`docker-compose.yml`, one-shot init container, not an implicitly-created topic);
  `auto.create.topics.enable` is off, because an implicitly-created topic gets the broker's default
  partition count and partition count is a routing decision.
- **The key is derived by the gateway** as `f"{career_site_id}|{user_id_pseudo}"`
  (`contracts/attributes.py: derive_kafka_key`). A client-supplied `partitionkey` is **never used for
  routing**; if it disagrees with the derived key the request is `403`, rather than being ignored and
  leaving the client believing it controls placement. We set a **raw key string** and let Kafka's
  partitioner hash it — *do not pre-hash*, or the key is hashed twice.
- `enable.idempotence=false` is **forbidden**. With it off, retries produce duplicates and reordering
  **with no error raised by any client**. The shipped values are `enable.idempotence=true`,
  `acks=all`, `retries=2147483647`, `max.in.flight.requests.per.connection=5`, `queuing.strategy=fifo`.
  This librdkafka **rejects** the three incompatible combinations loudly at construction, so the
  config is the assertion and the library is the belt.
- Idempotence is a **per-producer** property: each uvicorn worker builds its own `KafkaSink` and
  therefore its own producer id and sequence space. Four workers are four dedup domains.
- The raw `user_id` **never** appears in a context attribute, in the Kafka key, or on either Kafka
  topic. **It does appear in the driver's ground-truth ledger** — `LedgerRecord.user_pseudo` holds
  the client's plaintext `user_id`, not the topic's HMAC (§12) — so `ledger.jsonl` is a PII store too
  and needs a retention policy of its own.
- Every producer value, with its rationale and the cost of changing it, is in
  `docs/kafka-producer-tuning.md`. If that document and `kafka_producer_config` ever disagree, **the
  function is right**.

**Metrics the Queue team should watch**, all from `app/metrics.py` (`METRICS_NAMESPACE = "gateway"`).
Every name below was read out of a rendered `Metrics().render()`, not from the source:
`gateway_events_accepted_total`, `gateway_events_rejected_total` (by `type`, `sourcechannel`),
`gateway_validation_failures_total` (by `reason`), `gateway_dlq_published_total`,
`gateway_dlq_depth`, `gateway_ordering_violations_total`, `gateway_ordering_unchecked_total`,
`gateway_sequence_evictions_total`, `gateway_buffer_items` / `gateway_buffer_items_max` /
`gateway_buffer_utilisation`, `gateway_in_flight_batches`, `gateway_batches_accepted_total` /
`gateway_batches_rejected_total`, `gateway_http_responses_total` (by status *class*, never the raw
status), `gateway_rate_limit_denials_total`, `gateway_kafka_produce_latency_seconds`,
`gateway_batch_size_events`. Three label facts, all verified in the exposition: the per-worker
families carry `worker="<host:pid>"` (or `$WORKER_ID` when the orchestrator sets it); the four
histograms — `gateway_batch_size_events`, `gateway_encryption_latency_seconds`,
`gateway_kafka_produce_latency_seconds`, `gateway_request_latency_seconds` — carry **no labels at
all**, only `le`, so on a four-worker gateway they are already summed across workers and must not
be summed again by the scraper; and `gateway_label_values_dropped_total` is keyed by `label` alone,
so it is the one counter with no `worker` series.

`gateway_request_latency_seconds` is the handler's own time, excluding the network, as a
**fixed-bucket histogram** — a percentile read out of it is a bucket *bound*, not a value.
`gateway_buffer_utilisation`'s HELP text says so itself, and says more than "0..1": *"Can exceed 1:
the numerator counts records anywhere in the un-acked set … 1 is therefore not the 503 point … alert
on gateway_buffer_items against its max instead."* Read it that way.

## 8. Tenancy and auth

- **Asymmetric JWT only** — `ALLOWED_ALGORITHMS = ("EdDSA", "RS256")`, EdDSA preferred, public key
  from `JWT_PUBLIC_KEY_PEM`. `algorithms` is pinned to the one configured value, which rejects both
  `alg: none` and the key-confusion attack (HS256 signed with the public key we already hold). The
  gateway holds **only a public key and cannot mint a token** — `load_pem_public_key` refuses a
  private key, so it could not hold one even if handed one.
- `exp` and `aud` are **required**. `aud` defaults to `career-api`.
- `career_site_id` comes **from the signed claim only** (`TENANT_CLAIM`). A `source` in the body that
  disagrees → `403`. The claim is put through the contract's own `source` parser, so an `a/b` or
  65-character id is refused rather than reaching a path or a Kafka key.
- `sourcechannel` is derived from credential registration (`CREDENTIAL_CLAIM` selects it).
  `X-Source-Type` is a validated **hint**; a mismatch is rejected, never silently honored.
- **A batch must contain events for exactly one tenant** (`MIXED_TENANT`). This is what makes tenant
  isolation enforceable, and it also means **one JWT verification per batch, not per event**
  (`TokenVerifier.verify_count` is the counter that shows it).
- A `career_site_id` absent from the tenant registry is rejected — by auth (`403`) and again by
  `TenantKeyRegistry` (`UnknownTenantError`), so an accepted tenant can never be one with no key.
- Unparseable tokens are charged to a separate per-process **parse budget** (default 1,000) and
  refused `401`, not `429`: a token we cannot read has no tenant to charge.

## 9. PII

- All PII is inside `data`. **Context attributes carry identifiers and routing, never PII values.**
- Per tenant, two purpose-separated keys, **one HKDF extract and two expands**:
  `enc_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"enc")` and
  `mac_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"mac")`. The master secret must be
  at least 32 bytes. **The DB team derives these itself and must match them byte-for-byte.**
- Ciphertext wire form is `"<keyversion>.<base64url(nonce || ciphertext || tag)>"`, 96-bit random
  nonce, and AAD bound to `(source, id, type, field_name, keyversion)`, length-prefixed so distinct
  inputs cannot collide. The version is a plain-text prefix so a consumer can select a key before
  reading a byte of payload.
- `user_id_pseudo` and `email_hmac` are `HMAC-SHA256(mac_key, value)` as lowercase hex: joinable and
  groupable, **not reversible**. No normalisation is applied, so a caller that wants case folding must
  apply it before the HMAC or it will split the group.
- **Absent PII is left absent**, not encrypted as an empty string: "we do not know this candidate's
  gender" and "this candidate is not a woman" must not be the same bytes.
- `experience_status`, `years_of_experience`, `education_degree` and `education_branch` are
  **published as plaintext** — they are the non-identifying columns analytics aggregates on.
- The decrypt endpoint is **operator-only**, gated by `X-Operator-Key` (a credential separate from
  tenant auth), and **every attempt** is audit-logged with *who* and *which `(source, id)`* — never
  with the value. See §2 for the full endpoint contract.
- **Retention and access decisions on `career.events.dlq` must treat it as a PII store**: a
  validate-stage record holds the plaintext request element. See §4.

## 10. Client obligations (UI team)

These five are binding. Each is here with the reason it exists in the code, not with a performance
target — **this project makes no throughput claim and runs no benchmark**, so nothing below is
justified by a number about events per second.

1. **Batch** — ≤ 200 events / ≤ 2 MiB per request. Ordinary good API design, and the reason is
   structural rather than volumetric: **one JWT verification per batch** (a batch must be
   single-tenant, so there is exactly one tenant to verify), **one round trip per batch**, and
   **bounded per-request work** — every cap is checked against raw byte slices before anything is
   materialised (`app/ingest/limits.py`), so a batch is the unit the caps are written in. 200 / 2 MiB
   is a recommended operating point that leaves headroom below the 500 / 4 MiB ceiling.
2. **Keep-alive** — a TLS handshake per event is fatal and will be misdiagnosed as a gateway problem.
3. **Maintain `sequence`** — a per-user monotonic counter, zero-padded (10 digits), stable across page
   loads and continuing across sessions for the same user. It is the **only** thing that survives a
   gateway restart or a two-worker race, so a UI that resets or reuses it hands its consumer an
   ordering it cannot trust. See §6.
4. **Reuse `id` on retry** — and keep it unique across requests, because dedup is on `(source, id)`
   and the gateway does not dedup across requests. See §5.
5. **Do not pre-hash the partition key** and do not rely on `partitionkey`** — the gateway derives it
   and refuses a disagreeing value. See §7.

## 11. Amends `SPEC.txt`

**Verified against the file, not from memory.** `SPEC.txt:133` now reads:

> Topic Naming Convention: career.events.raw (single topic, all event types). NOTE: an earlier draft of
> this spec proposed career.events.<environment>.<event_type> (a topic per event type). That is
> SUPERSEDED: splitting event types across topics breaks sessionization, which must order
> APPLICATION_STARTED -> STEP_COMPLETED -> SUBMITTED per (career_site_id, user_id, session_id). See
> contracts/CONTRACT.md section 6.

`SPEC.txt:131` is the CloudEvents schema-validation line, and `SPEC.txt:137` names CloudEvents 1.0 as
the wire format. So the topic-per-type proposal **is** superseded in the spec and §6 is the section it
points at.

`APPLICATION_ABANDONED` is synthesised by the **Queue team's Flink job** on the watermark timeout
(`SPEC.txt:350`, in the sessionization/drop-off section, which also fixes the session window at 30
minutes), **not** by the client or by the generator. Both teams emitting it would double-count the
drop-off metric. Note that the event type is nonetheless in the published `EVENT_TYPES` enum, because
the client's "save and apply later" journey needs the type to exist on the wire even when the gateway
does not originate it.

## 12. Ledger schema — the reconciliation contract

Ground truth, one schema, defined once, in `contracts/ledger.py`:

```
(id, source, type, tenant, user_pseudo, seq)
```

That is `LedgerRecord`'s field list, in that order — `id`, `source`, `type`, `tenant`, `user_pseudo`,
`seq` — and JSONL, append-only, written with `json.dumps(..., separators=(",", ":"))`.

Two things a consumer must know before using it:

- **`user_pseudo` is the client's plaintext `user_id`, not the topic's HMAC.** Those two strings are
  never equal, so the join is *not* `ledger.user_pseudo == topic.user_id_pseudo`; it is
  `pseudonymize(tenant mac_key, ledger.user_pseudo) == topic.user_id_pseudo`. That needs the master
  secret, so `tools.verify` reports the dimension as **`skipped (no master secret)`** when it is
  absent — never as a passing zero. **The ledger is therefore a PII store**: it is the one file in
  this repo holding raw identifiers, and it needs a retention policy of its own.
- `(source, id)` is the other join and the one reconciliation runs on by default, because it is
  shape-stable across the ingress and egress forms.

**What the Queue team reconciles against it:** `python -m tools.verify` reads this ledger plus both
topics and asserts `missing == 0`, `unexpected == 0`, `dlq == --expect-dlq`,
`wire_duplicates == --expect-duplicates`, `ordering_violations == 0`, `ledger_duplicates == 0`,
`seq_mismatches == 0`, no unattributable DLQ record, no unknown DLQ reason code, and the pseudonym
check when a master secret is supplied (`tools/verify.py: reconcile`). Exit codes are part of the
contract: `0` pass, `1` mismatch, **`2` unverified** — and on `2` **no scorecard is printed at
all**, because a read that did not drain to every partition's end offsets, an unreadable ledger or
an unreachable broker all print one line and stop. A truncated view that looks balanced is worse
than no tool.

**Duplicates are counted after `(source, id)` dedup, and the assertion is a flag, not a constant.**
`tools.verify --expect-duplicates N` defaults to 0. The flag exists because a gateway killed
mid-flight makes the driver resend the same batch with the same `id`s, which puts one `(source, id)`
on the topic twice **by construction** — the metric to watch is `wire_duplicates`, and the shipped
`scripts/chaos/kill_gateway.sh` deliberately asserts `--expect-duplicates 0`, because it resets both
topics before verifying so the reconciliation is of one known run. So a non-zero duplicate count is
not automatically a bug, and a non-zero *expected* count is not automatically fine: read which run
produced it. `scripts/chaos/tenant_flood.sh` is the opposite case and **expects exit 1**, because a
shed tenant's batches are recorded by the driver but never accepted.
