# Event Generator — multi-tenant event ingestion gateway

**Team slice 2 of 4.** This repository is the pipeline's front door: a hardened
FastAPI gateway that authenticates, validates, rate-limits, encrypts and publishes
CloudEvents to Kafka, plus the synthetic driver that proves it works.

| Slice | Owns | In this repo |
|---|---|---|
| **1 · UI** | career-page DOM SDK | no — we publish the contract they build on |
| **2 · Event Generator** | ingestion gateway + driver | **yes** |
| 3 · Queue | Kafka + Flink sessionization | no — we produce into it |
| 4 · Database | Cassandra schema + queries | no — we publish the field set they store |

Everything below describes what this repository contains. It does not describe the
platform; the other three slices have their own trees.

---

## The path

```
clients ──HTTPS · application/cloudevents-batch+json · Bearer JWT──▶
┌──────────────────────────────────────────────────────────────────┐
│  decode ─▶ auth ─▶ validate ─▶ encrypt ─▶ produce               │
└──────────────────────────────────────────────────────────────────┘
        │              │            │            │
   one JWT per     per-event    AES-GCM over   career.events.raw
   single-tenant   verdict,     5 PII fields   key = tenant|user_pseudo
   batch           DLQ'd        + 2 HMACs
```

**The order is load-bearing, not incidental.** Validate runs before encrypt, so an
event rejected during validation was never encrypted — which is exactly why the DLQ
carries the plaintext request element for those, and ciphertext for a rejection
that came after encryption. See [Honest limitations](#honest-limitations).

Pipeline: `app/ingest/pipeline.py` · contract: `contracts/CONTRACT.md`

---

## Quick start

Requires Python 3.11+, and for the full stack WSL2 + Docker Desktop (Kafka and
Flink have no native Windows support).

```bash
git clone git@github.com:Sid180603/Event-Data-Pipeline.git
cd Event-Data-Pipeline
pip install -e ".[dev]"
python -m pytest -q          # 775 tests
```

The running system — gateway, broker, driver, chaos beats — runs from `make`:

```bash
make up        # stack healthy, topics created
make demo      # load run with the live view over it
make verify    # reconcile the ledger against Kafka; exit 0 = reconciled
make chaos     # the four failure beats
make test      # full suite
```

`make help` lists every target. **`.env` and `driver-signing-key.pem` must exist
first** — the generator is in `docker-compose.yml` lines 23–36. WSL2 needs
`memory=11GB` in `%USERPROFILE%\.wslconfig`; the default cap on a 16 GB box is
~8 GB and will not fit Kafka + gateway + driver. See `docs/demo-runbook.md`.

---

## What is here

| Area | Where | Notes |
|---|---|---|
| Wire contract | `contracts/` | CloudEvents 1.0. **Two schemas**, both generated and staleness-tested |
| Auth | `app/auth/` | Asymmetric JWT. The gateway holds a public key and **cannot mint a token** |
| Validation | `app/validate/` | Per-event verdicts, reason codes, never the offending value |
| Crypto | `app/crypto/`, `app/pseudonym/` | Per-tenant purpose-separated HKDF keys, AAD-bound AES-GCM |
| Rate limiting | `app/ratelimit/` | Per-tenant token bucket, charged per event and denied per batch |
| Pipeline | `app/ingest/` | The five stages, the DLQ, the operator-only decrypt endpoint |
| Kafka | `app/kafka/` | One owner thread, bounded queue, `202` = accepted into that buffer |
| Metrics | `app/metrics.py` | 24 families / 4 histograms, Prometheus 0.0.4 |
| Composition | `app/main.py` | The factory. One tenant list feeds all three registries |
| Driver | `driver/` | 500-tenant Zipfian corpus, funnel FSM, replay client, fault injection |
| Tools | `tools/` | `verify` reconciles ground truth; `observe` is the live view |
| Load | `bench/load.py` | Honest load client. **Not a benchmark**, and says so |
| Chaos | `scripts/chaos/` | Four failure beats, each failing loudly |

### Caps and topics

| | |
|---|---|
| Events per batch | 500 (`413` above) |
| Request body | 4 MiB (`413` above) |
| Event size, post-encryption | 64 KiB (per-event rejection) |
| Event size, ingress plaintext | 48 KiB (per-event rejection) |
| Producer queue | 50,000 records, then `503` — never a block, never a silent drop |
| Topics | `career.events.raw`, `career.events.dlq` |
| Kafka key | `<career_site_id>\|<user_id_pseudo>`, derived by the gateway |

A client-supplied `partitionkey` is never used for routing; if it disagrees with the
derived key the request is `403`.

### Response semantics

| Status | Meaning |
|---|---|
| `202 {accepted, rejected}` | **Accepted into an in-memory buffer. Not a durability receipt.** |
| `401` / `403` | Bad or absent token · body `source` ≠ token tenant · multi-tenant batch |
| `413` | Batch over 500 events or 4 MiB |
| `429` + `Retry-After` | Tenant budget exhausted. **The whole batch is denied, and no DLQ entry is made** |
| `503` + `Retry-After` | Producer buffer full, or broker unreachable |

Retry `503` and connection resets **reusing the same event `id`**. Dedup is on
`(source, id)`, which is what makes a resend free of duplicates.

---

## For the other three teams

Everything below is published and versioned. `contracts/CONTRACT.md` is binding.

- **UI** — post the *ingress* shape (plaintext PII under plain names), never the
  egress one. Do not send `*_enc`, `*_hmac` or `user_id_pseudo`; all three are
  refused. Your obligations: batch at ≤200 events / ≤2 MiB, keep-alive on, maintain
  the per-user `sequence` counter, reuse `id` on retry, do not pre-hash the key.
- **Queue** — one topic, not one per event type (`SPEC.txt:133` is superseded;
  splitting would break sessionization). Order by `sequence`, which is
  authoritative and partition-order-independent. `application-abandoned` is
  **yours** to synthesise on the watermark timeout, never ours or the driver's.
  The DLQ shape is in `app/dlq/envelope.py`.
- **Database** — `user_id_pseudo` and `email_hmac` are HMACs: joinable and
  groupable, never reversible. The five `*_enc` fields need the tenant key you get
  separately. `user_id` is not stored anywhere except the DLQ, and see the
  limitation below.

Full bundle and open questions: **`docs/handoff.md`**.
Producer configuration: **`docs/kafka-producer-tuning.md`**.

---

## The demo

Six falsifiable beats. The argument is falsifiability, not throughput — **this
project makes no throughput claim and the load client is explicitly not a
benchmark.**

1. Contract slide
2. Live session — events/sec and error rate, live
3. Raw vs encrypted, side by side, for one sampled event
4. Kill the gateway — the receipts balance, with a **bounded and reported** loss window
5. Inject 5% garbage — the DLQ catches **exactly** 5%
6. One tenant floods — it is shed, the other 499 are unaffected

Timed runbook with a failure drill per beat: `docs/demo-runbook.md`.

---

## Honest limitations

Read these before building on anything above.

1. **Nothing here has run against a real broker.** 775 tests pass, but every one
   ran against `FakeSink`, a stubbed `docker`, or a loopback socket. `make demo`,
   `make chaos` and all four beats are unexecuted. Correct components are not the
   same as a working system.

2. **The DLQ is a PII store for validation-stage rejections.** A validation
   rejection happens *before* encryption, so the only payload that exists is the
   plaintext request element — written deliberately, because encrypting an
   already-judged-invalid event wastes work and redacting it leaves an operator
   unable to diagnose it. Anyone setting retention or access policy on
   `career.events.dlq` must treat it accordingly. A rejection *after* the encrypt
   stage carries ciphertext. `error_context` never carries the offending value in
   either case.

3. **CONTRACT §4's post-encryption DLQ path is undemonstrated.** All four injected
   variants fail during validation, so the ciphertext guarantee is never exercised
   by a chaos beat. Reaching it needs an oversized encrypted event and no shipped
   tool injects one.

4. **`STICKY_ROUTING` is read by nothing.** `Settings.sticky_routing` parses the
   env var and no code path consults it — there is no affinity layer in front of
   uvicorn's `--workers`. Partitioning is correct and deterministic, and the flood
   beat's isolation claim holds, but a tenant's *effective* budget is up to N× the
   configured rate. No claim about the exact enforced rate is supportable. Closing
   this needs an L7 balancer that hashes the gateway-derived key.

5. **The top 297 B of the 48 KiB ingress cap is a dead zone.** Measured through the
   real pipeline: an event whose bulk sits in the encrypted PII fields inflates to
   65,923 B, past the 64 KiB ceiling, and is refused per-event. The largest that
   fits is 48,855 B. `INGRESS_EVENT_BYTES` is deliberately unchanged — it is a
   published contract value and lowering it changes what all three teams may send.
   Practical rule: keep the encrypted fields small.

6. **`make verify` cannot pass after `make chaos`.** The flood beat ends with a
   deliberate shed shortfall, so verify exits 1. That is the design, not a
   regression.

---

## Tests

```bash
python -m pytest -q                                  # 775 tests
python -m pytest contracts driver tools bench -q      # no app, fast
```

Areas run independently if you want a tight loop. The suite is written so that a
failure names the property that broke rather than the line — see the module
docstrings in `app/ingest/pipeline.py` and `tools/verify.py`.

---

## Layout

```
app/            gateway: auth, validate, crypto, ratelimit, ingest, kafka, metrics
bench/          load client (not a benchmark)
contracts/      CloudEvents schemas, attributes, ledger — the binding contract
driver/         corpus, funnel FSM, replay client, fault injection
scripts/chaos/  four failure beats
tools/          verify (reconcile), observe (live view)
docs/           handoff, tuning, demo runbook
tasks/          plan and task list
```