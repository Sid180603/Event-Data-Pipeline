# Ingestion Contract v0.1

**Owner:** Event Generator team. **Consumers:** Queue (Kafka + Flink), Database (Cassandra), UI.
**Wire format:** [CloudEvents](https://cloudevents.io) 1.0 (CNCF Graduated) — required by `SPEC.txt:131`.
**Schema of record:** `contracts/event.schema.json`, **generated** from `contracts/cloudevent.py`.
Regenerate with `python -m contracts.gen_schema`; the test suite fails if it drifts.

---

## 1. Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/ingest` | Ingest a CloudEvents batch |
| `GET` | `/metrics` | Counters (T10a) |

### Request

- `Content-Type: application/cloudevents-batch+json` — a JSON **array** of CloudEvents
- `Authorization: Bearer <JWT>`
- `Ce-Request-Delivery-Id` is **not** used; idempotency is carried per-event in `id`

### Limits — hard, enforced before allocation

| Limit | Value | Violation |
|---|---|---|
| Events per batch | **500** | `413` |
| Request body | **4 MiB** | `413` |
| Event size, after encryption | **64 KiB** | rejected per-event → DLQ |
| Ingress event size (plaintext) | **48 KiB** (provisional; T6 measures) | `413` |

The CloudEvents HTTP binding expects a receiver to advertise its maximum batch size. These are that
advertisement. The UI SDK should flush at **≤ 200 events / ≤ 2 MiB** to leave headroom.

## 2. Response semantics — read this before writing a client

| Status | Meaning |
|---|---|
| `202 {accepted, rejected}` | **Accepted into the in-memory buffer. This is NOT a durability receipt.** |
| `401` | Missing / malformed / expired / bad-signature JWT |
| `403` | Body `source` ≠ token tenant · multi-tenant batch · `partitionkey` mismatch |
| `413` | Batch over 500 events or 4 MiB |
| `429` + `Retry-After` | Per-tenant rate limit |
| `503` + `Retry-After` | Producer buffer full, or broker unreachable |

**Clients MUST retry on `503` and on connection reset, reusing the same event `id`.** Retries are
idempotent under the dedup rule in §4.

`202` deliberately does not mean "durably in Kafka". With a bounded in-memory buffer, a gateway killed
mid-flight loses the un-acked window. That window is measured and reported in T11 Chaos 1 rather than
claimed to be zero.

## 3. Pipeline order — load-bearing

```
decode → auth → validate → encrypt → produce
```

The DLQ therefore carries the **post-encryption** event. `error_context` carries `reason`, `field`,
`index`, `stage` and the error metadata, and **never the offending value**. A DLQ event can be
re-injected without double-encryption.

## 4. Idempotency and dedup

- **Dedup key is `(source, id)`** — the CloudEvents rule: *"Consumers MAY assume that Events with
  identical `source` and `id` are duplicates."*
- `id` alone is **not** sufficient: it is only unique within a source.
- `id` is **client-generated** and must be stable across retries.
- **Cross-request `id` uniqueness is the client's responsibility** ("Producers MUST ensure that
  `source` + `id` is unique").
- **Duplicate `id` within a single batch is rejected** with a per-event `400`-equivalent entry, because
  silently accepting them would let downstream dedup collapse N real events into one.

## 5. Ordering — what we actually promise

> Events for the same `(career_site_id, user_id)` are produced in order **by a single gateway worker**,
> guaranteed by sticky routing on `hash(career_site_id | user_id_pseudo)`. Ordering is **not** guaranteed
> across gateway restarts, or if a worker dies mid-batch.
>
> **Consumers MUST order by the `sequence` extension**, which is authoritative and
> partition-order-independent. `sequence` is a zero-padded, monotonically increasing counter scoped per
> `(source, user)`.

**Why `sequence` is not optional:** the gateway runs multiple worker processes, each with its own Kafka
producer. Kafka orders by partition-append within a producer; across independent producers the order is
nondeterministic. Sticky routing is what makes single-producer ordering hold, and `sequence` is what
survives a worker restart. **The Queue team's Flink sessionization must use `sequence`.**

## 6. Kafka

- **One topic: `career.events.raw`.** DLQ: `career.events.dlq`.
- **The key is derived by the gateway** as `<career_site_id>|<user_id_pseudo>`.
  A client-supplied `partitionkey` is **ignored**; if it disagrees, the request is `403`.
  We set a **raw key string** and let Kafka's partitioner hash it — *do not pre-hash*, or the key is
  hashed twice.
- `enable.idempotence=false` is **forbidden**. With it off, retries produce duplicates and reordering
  **with no error raised by any client**. Our ordering and dedup claims both depend on it.
- The raw `user_id` **never** appears in a context attribute or in the Kafka key.

## 7. Tenancy and auth

- **Asymmetric JWT** — EdDSA preferred, RS256 acceptable, public key from `JWT_PUBLIC_KEY_PEM`.
  The gateway holds **only a public key and cannot mint a token**.
- `career_site_id` comes **from the signed claim only**. A `source` in the body that disagrees → `403`.
- `sourcechannel` is derived from credential registration. `X-Source-Type` is a validated **hint**; a
  mismatch is rejected, never silently honored.
- **A batch must contain events for exactly one tenant.** This is what makes tenant isolation
  enforceable, and it also means **one JWT verification per batch, not per event**.
- A `career_site_id` absent from the tenant registry is rejected.

## 8. PII

- All PII is inside `data`. **Context attributes carry identifiers and routing, never PII values.**
- Per tenant, two purpose-separated keys:
  `enc_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"enc")` and
  `mac_key = HKDF-SHA256(ikm=master, salt=career_site_id, info=b"mac")`.
- Ciphertext is bound by AAD to `(source, id, type, field_name, keyversion)`.
- `user_id_pseudo` and `email_hmac` are HMACs: joinable and groupable, **not reversible**.
- The decrypt endpoint is **operator-only**, gated by a credential separate from tenant auth, and every
  decrypt is audit-logged with *who* and *which `(source, id)`*.

## 9. Client obligations (UI team)

1. **Batch** — ≤ 200 events / ≤ 2 MiB per request. Batching is mandatory, not an optimisation: at
   50,000 events/sec with one event per request, the required request rate is not reachable in Python.
2. **Keep-alive** — a TLS handshake per event is fatal and will be misdiagnosed as a gateway problem.
3. **Maintain `sequence`** — a per-user monotonic counter, zero-padded, stable across page loads.
4. **Reuse `id` on retry.**
5. **Do not pre-hash the partition key** and do not rely on `partitionkey`.

## 10. Amends `SPEC.txt`

`SPEC.txt:133` proposed a topic per event type. **That is superseded** — see §6. Splitting event types
across topics would break sessionization, which must order `APPLICATION_STARTED` → `STEP_COMPLETED` →
`SUBMITTED` per `(tenant, user, session)`. `SPEC.txt` has been updated.

`APPLICATION_ABANDONED` is synthesised by the **Queue team's Flink job** on the watermark timeout
(`SPEC.txt:338-339`), not by the client or the generator. Both teams emitting it would double-count the
drop-off metric.

## 11. Ledger schema (T11 depends on this)

Ground truth, one schema, defined once:

```
(id, source, type, tenant, user_pseudo, seq)
```

`contracts/ledger.py`. JSONL, append-only. Used to assert `sent == accepted == stored`,
`duplicate (source, id) == 0`, and `DLQ count == injected invalid count`.
