# Demo runbook — six beats, in order

**Everything the demo claims is measured on screen, from the running system.** Nothing
here is a slide with a number typed into it. If a beat does not reproduce, the failure
output is the same tooling and it is shown as-is: a demo that quietly skips a failing
step is worse than no demo.

**The demo does not claim or demonstrate 50,000 events/sec.** That question is closed.
There is no throughput figure, no capacity figure and no scaling argument anywhere in
this document or in the beats. The argument is falsifiability: every claim below is
something the audience can watch either hold or not hold.

| | |
| --- | --- |
| Machine | local MSI laptop, Intel i7-9750H (6 cores / 12 threads), 16 GB RAM, Windows |
| Where the stack runs | **WSL2 + Docker Desktop.** Cassandra and Flink have no native Windows support, and neither has the broker image |
| Wall-clock budget | **13 minutes** of beats, plus up to 3 minutes of setup that is not on the clock |
| One-time setup | `.env` + `driver-signing-key.pem` (section below), once per clone |
| Beats | 1 contract · 2 live session · 3 raw vs encrypted · 4 kill the gateway · 5 inject 5% garbage · 6 one tenant floods |
| Beats 4-6 are scripts | `scripts/chaos/*.sh`, each runnable on its own with `--help` |

---

## Before the clock starts

### One-time setup, once per clone

Compose will not invent secrets and the gateway refuses to start without a master secret
and a public key. Run this in **WSL2**, from the repository root. It is the generator
from the header of `docker-compose.yml` (lines 18-53), copied here verbatim so this
document is runnable on its own — keep it in step with that header.

```bash
python - <<'PY'
import base64, secrets
from cryptography.hazmat.primitives import serialization as s
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
key = Ed25519PrivateKey.generate()
pub = key.public_key().public_bytes(s.Encoding.PEM, s.PublicFormat.SubjectPublicKeyInfo)
with open(".env", "w", encoding="utf-8") as fh:
    fh.write("MASTER_SECRET=" + base64.b64encode(secrets.token_bytes(48)).decode() + "\n")
    fh.write('JWT_PUBLIC_KEY_PEM="' + pub.decode().strip() + '"\n')
    fh.write("OPERATOR_KEY=" + secrets.token_urlsafe(32) + "\n")
    fh.write("GATEWAY_TENANTS=" + ",".join(f"tenant_{i:04d}" for i in range(1, 501)) + "\n")
with open("driver-signing-key.pem", "wb") as fh:   # the gateway must NEVER see this
    fh.write(key.private_bytes(s.Encoding.PEM, s.PrivateFormat.PKCS8, s.NoEncryption()))
PY
```

Both files are in `.gitignore`. `make preflight` checks for them and says which one is
missing.

### Give WSL2 the memory

The default 50% cap on a 16 GB box is about 8 GB, which does not fit the broker, the
gateway and the driver. Set this once, then `wsl --shutdown` from PowerShell:

```
# %USERPROFILE%\.wslconfig
[wsl2]
memory=11GB
processors=6
swap=4GB
```

### Bring the stack up before you present

```bash
make up          # compose stack + poll /healthz until the gateway answers
make test        # the whole suite, serially
```

`make up` is **not** on the clock. On a cold pip cache the gateway takes a couple of
minutes to start; after that it is a few seconds.

`make test` takes minutes, not seconds: it includes two micro-benchmark guards that assert
a per-call cost ceiling, and those measure the machine. Run it when nothing else is
happening. A failure in `app/test_metrics.py::test_the_increment_path_stays_cheap` on the
day means this laptop is loaded or throttled, not that something regressed — the whole
suite is worth running, that one assertion is worth re-running on its own
(`python -m pytest -q app/test_metrics.py::test_the_increment_path_stays_cheap`) before
you read anything into it.

### Read this if a beat misbehaves

Every beat script exits non-zero with the failed assertion named. Section
"Failure drill" at the bottom says what to do about each one. Two rules:

* **Never claim a number you did not read off the screen.** If a beat fails, say so and
  show the failure. The output is the same oracle either way.
* **`tools.verify` exits 2 means "I verified nothing"**, which is not "it matched".
  Exit 0 is the only pass. Every beat script treats 2 as a failure and says so.

---

## The six beats

### Beat 1 — the contract, in two tables · budget 0:45

**Run:** open `contracts/CONTRACT.md`. Nothing is executed.

**Say:**
> Two schemas, and this is the single most important thing to get right. What you POST is
> the ingress schema: plaintext `data.candidate.user_id`, because a client cannot hold a
> tenant key. What lands on Kafka is the egress schema: `user_id_pseudo` is an HMAC, the
> five PII fields are ciphertexts. They are separate structs and they reject each other's
> PII fields — you cannot post a ciphertext you made up.

**Show:** section 1's ingress/egress table, then section 3's status table.

**Then land the one that matters for the rest of the demo:**

> `202` is **accepted into an in-memory buffer. It is not a durability receipt.** That is
> why beat 4 exists: we kill the gateway mid-flight and measure the window rather than
> claiming it is zero. And a `429` denies the *whole* batch and produces *no* DLQ entry —
> which is why beat 6 can assert the DLQ is untouched while a tenant is being shed.

**Expect:** nothing to run. 45 seconds.

---

### Beat 2 — a live session, then the receipt · budget 1:30

**Run:**

```bash
make demo
```

That is `up` → a load run → the live view → `make verify`, in that order, with the live
view overlapping the load run (a view that starts after the run finishes reports zero
events/sec, which is true and useless).

**Say:**

> The driver is a separate container on its own CPU share, because it is the measuring
> instrument and an instrument competing with what it measures reports the scheduler, not
> the gateway. It posts batches of at most 200 events over one keep-alive connection, and
> writes a ground-truth ledger of everything it sent.

**Show:** the live view while the run is going (events/sec, error rate, producer queue
occupancy, p50/p99), then the two documents underneath it.

1. **The driver's receipt** — `sent`, `accepted`, `rejected`, `unaccepted`, `clean`. The
   driver exits non-zero unless `sent == accepted`, so a clean run is a claim the tool
   makes about itself.
2. **The scorecard** — `tools.verify` reconciles that ledger against what the broker
   actually stored: sent / accepted / stored / duplicates / dlq, per source and per
   type, and `PASS` at the bottom.

**Say while it reconciles:**

> `tools.verify` reads the topic from the retention floor to its end offsets and says
> whether it got there. A short read returns no scorecard at all and exits 2, because a
> truncated view that looks balanced is the one outcome worse than having no tool. And
> every stored `user_id_pseudo` is re-derived here with the gateway's own key functions and
> compared to the ledger's plaintext `user_id` — that is the one field most likely to be
> mangled by a pipeline bug, and it is the field that would be silently wrong.

**Expect:** exit 0 and `PASS`. About 90 seconds, most of it the reconciliation read.

**If you want the live view on your own screen instead**, uncomment the
`127.0.0.1:8000:8000` mapping in `docker-compose.yml` and run
`OBSERVE_ARGS='--samples 8 --interval 1' IN_NETWORK=0 make observe`. Never `0.0.0.0`:
that listener speaks plaintext and carries bearer tokens and plaintext PII.

---

### Beat 3 — one event, before and after · budget 0:30

**Run:**

```bash
make inspect
```

Runs in-process: it builds the real gateway from this machine's `MASTER_SECRET`, posts
one synthetic event through the real `POST /v1/ingest`, reads the record back out of the
sink, and asks the operator-only `POST /v1/decrypt` for each ciphertext.

**Say:**

> Same event, three columns. What the client sent — plaintext, because it cannot hold a
> key. What is stored — the record the sink actually kept. And what the operator endpoint
> hands back, and only if the operator credential was accepted. The two HMACs are shown
> but not decrypted: they are joinable, not reversible.

**Show:** the table. Point at one ciphertext's length (76 characters of base64 for a
30-character address) and at the footer saying no plaintext is stored or logged.

**Also say, because it is the honest part:**

> The decrypt is what an auditor will ask about, and it is gated behind a credential
> separate from tenant auth, and every call is audit-logged with who asked and which
> `(source, id)`.

**Expect:** no `REFUSED:` line at the bottom. About 30 seconds.

---

### Beat 4 — kill the gateway mid-flight · budget 2:30

**Run:**

```bash
bash scripts/chaos/kill_gateway.sh
```

**Say before it starts:**

> SIGKILL, not SIGTERM. SIGTERM would run the lifespan shutdown, the producer would
> drain, and there would be nothing left to measure. A `202` is acceptance into an
> in-memory buffer, so a hard kill drops whatever was un-acked. We are going to measure
> that window, not claim it is zero.

**Show, in this order:**

1. The driver's receipt, mid-run, and the moment of the kill.
2. **The loss window, as an identity:**

   ```
   unaccepted (never delivered to the gateway)  N
   un-acked window (202'd into the buffer)      M
   bound on that window (buffer capacity)       50000
   ```

   `missing == unaccepted + M` exactly, because an event that got a `202` is either on
   the topic or in the window that the kill dropped, and an event that never got a `202`
   is in neither. The window is bounded by the buffer's capacity because that capacity is
   what "bounded" means here.
3. **`tools.verify` exits 1 for this run, on purpose**, and its own failure list names the
   shortfall. Say it out loud: a non-zero exit here is the receipt balancing, not the
   system failing.
4. **The restart: a fresh run reconciles clean** — `tools.verify` exit 0 with
   `--expect-duplicates 0 --expect-dlq 0`: zero wire duplicates, zero DLQ, zero missing,
   zero unexpected, every stored pseudonym the expected HMAC.

**Watch for:** any sentence of the form "no data lost". There is none, and there must not
be. If the measured window is zero, the script says so as *this run's* measurement with
the bound beside it.

**If the kill lands after the run finished**, the script fails on purpose — a kill that
arrived late proves nothing. See the failure drill.

**Expect:** `PASS`, with a non-zero shortfall printed. About 2 minutes 30 seconds.

---

### Beat 5 — inject exactly 5% garbage · budget 2:00

**Run:**

```bash
bash scripts/chaos/bad_events.sh
```

**Say:**

> The bad events are *placed*, not sampled. `plan_injection` computes an exact integer
> count and a stratified set of positions, so "the DLQ caught exactly what we sent" is an
> equality rather than something that happened to land near five percent. A Bernoulli draw
> at 5% misses by hundreds over a long run, and "roughly 5%" is not an assertion.

**Show:**

1. The receipt's `injected`, and the corpus recounted from the driver's own
   catalogue and seed: same session count, same seed, same integer.
   `plan_injection` *places* the malformed events at stratified positions rather
   than drawing them, so the count is `floor(corpus * 5/100 + 0.5)` and not a
   sample.
2. **`tools.verify --expect-dlq <that same integer>` exits 0** — which also proves
   the main topic is intact: every good event stored, nothing unexpected, no
   ordering violation, every pseudonym the expected HMAC.
3. **The per-code breakdown equals the per-variant breakdown.** Every injected variant is
   on the DLQ, and nothing else is: nothing was rejected for a reason we did not inject,
   and nothing injected escaped.
4. **What the DLQ does *not* carry.** Say this plainly, it is the part an auditor checks:

   > A validate-stage rejection is DLQ'd with the **plaintext** request element, because it
   > was never encrypted — encrypting a record we have already judged invalid would be
   > work spent on a dead event. So "the DLQ never holds plaintext" is false and we do not
   > claim it. What we do assert is narrower and checkable: the `error_context` never
   > carries the offending value, an identifier, or a PII field. The script derives every
   > user id in the ground truth and its HMAC and scans every rejection's `error_context`
   > for them.

**Expect:** `PASS`, `dlq == injected` exactly, `error_context_leaks 0`.

**Expect the count to be a specific integer, and read it off the screen rather than
saying this sentence.** With the shipped defaults (`CHAOS_SESSIONS=6000`, 500 tenants,
5%) and a `.env` listing `tenant_0001..tenant_0500`, the corpus is **31,212** events, so
`plan_injection` places **1,561** malformed ones — 391 `SCHEMA` and 390 each of
`BAD_TIME`, `DUPLICATE_ID` and `UNKNOWN_ATTRIBUTE`. The script also prints a second
opinion from `driver.inject`, which plans **1,556** over its own catalogue, and the two
are deliberately *not* asserted equal: `driver/tenants.py` numbers tenants from 0 while
the `.env` generator numbers them from 1, so the two catalogues are not the same 500
ids and their corpora differ by 96 events. The beat's claim is about the run, and the run
is the one recounted over the list the gateway actually serves.

**Expect:** about 2 minutes. Most of it is the driver run plus two full topic reads.

---

### Beat 6 — one tenant floods · budget 2:00

**Run:**

```bash
bash scripts/chaos/tenant_flood.sh
```

**Say:**

> The flood is a *separate stream*, added alongside an untouched baseline corpus. That is
> the whole reason the "the other 499 are unaffected" half is falsifiable: because the two
> corpora never interact, the per-tenant counts with and without the flood have to be
> identical. Overlaying the flood onto the Zipf model instead would perturb the
> apportionment every other tenant's volume was derived from, and the claim would become
> unfalsifiable.
>
> And it takes **three concurrent clients**, which is the part worth understanding. The
> budget being exhausted is *per tenant* — 2,000 events a second sustained, with a burst
> of 4,000 — and a bucket that refills as fast as one client can fill it can never be
> exhausted. No volume of traffic produces a 429 from a single client. Three clients
> together outrun the refill, and the per-tenant limiter fires. The three are given
> **disjoint slices of the flood's users**, so every user's journey still goes out in one
> piece and the ordering check still reads zero.

**Show, in this order:**

1. **The offline check first.** `driver.inject` run twice with the same seed, once with
   `--flood-tenant`: every other tenant's event count unchanged, and the flooded tenant's
   grew by exactly the flood stream's event count.
2. **The baseline run and its reconciliation** — a clean run, `tools.verify` exit 0. This
   is the "before" snapshot, taken from the topic, not from the generator. It is the same
   topic the flood lands on, which is what makes the before/after per-source comparison
   meaningful.
3. **The flood.** Each client's own receipt: `sent`, `accepted`, `429s`, `unaccepted`, and
   `RATE_LIMITED` as the gateway's own refusal code. Then
   `gateway_rate_limit_denials_total` rising and `gateway_http_responses_total{4xx}`
   rising with it.
4. **The DLQ did not move.** Same depth before and after. A `429` denies the whole batch,
   it will succeed on retry, so filing it in the DLQ would bury real poison under retry
   noise.
5. **The other 499 tenants, per source, from the topic: identical sent and stored counts
   before and after.** 499 tenants compared, zero drift.
6. **The shortfall is accounted for exactly.** `tools.verify` is *expected to exit 1*
   here — the shed batches are in the ground truth and on no topic, which is the contract
   working. The script asserts the identity `missing == the flood's unaccepted count`, so
   every shed event is accounted for and nothing vanished without being refused. It also
   asserts **zero ordering violations**: splitting the flood across three clients must not
   have reordered anybody's journey, and that is checked rather than assumed.

**Mention if asked:** `gateway_consumer_lag` is printed and reads 0. That is not a claim
that lag is bounded — the gateway produces and never consumes, so nothing in this stack
sets it; it is an input for whoever runs the consumer group. The backlog this beat *can*
measure is the shed tenant's shortfall and the producer's own un-acked window.
`gateway_buffer_utilisation` can read above 1.0 and 1.0 is not the 503 point; the metric's
own HELP text says so, and the script says so rather than alerting on it.

**Expect:** `PASS`. About 2 minutes 30 seconds.

---

## Timings

Every figure below is a **budget, not a measurement**. They were not timed on the demo
machine, because no run of this stack was possible on the machine these scripts were
written on — see the last section.

| Beat | Budget | Where the time goes |
| --- | --- | --- |
| 1 contract | 0:45 | talking |
| 2 live session | 1:30 | load run + 8 live samples + reconciliation read |
| 3 raw vs encrypted | 0:30 | in-process, one event, three decrypts |
| 4 kill the gateway | 2:30 | topic reset, 6,000-session run, SIGKILL, restart, two reconciliations |
| 5 inject 5% garbage | 2:00 | topic reset, 31,212-event run, one corpus recount, two topic reads, one DLQ read |
| 6 tenant flood | 2:30 | topic reset, two offline corpora, 6,000-session baseline + read, three flood clients, one combined read |
| buffer | 3:15 | questions, a slow machine, one re-run |
| **total** | **13:00** | |

Not on the clock: `make up` (2–3 minutes from cold), the one-time `.env` generation, and
`make test`. Run all three before you present, not during.

If you are running long, cut in this order: beat 3 (0:30, the least load-bearing — beat 5
shows encryption happened), then beat 2's reconciliation (keep the live view, drop the
scorecard). Do not cut beats 4, 5 or 6: they are the proof.

---

## Failure drill

The demo's job is to work. When a beat does not reproduce, do this.

### The stack is wedged, or a topic is not empty

```bash
make down -v      # stop everything and drop the volumes
make up           # fresh broker, fresh topics
```

Then restart from beat 2. **Why this matters:** `tools.verify` reconciles the *whole*
topic against *one* ledger, so a topic carrying an earlier run's records will report them
as unexpected. Every beat script resets both topics at the start for exactly this reason.
If you interrupt a beat, reset before re-running it.

### Beat 4: "the driver run exited clean, so the SIGKILL landed after the run had finished"

The run was shorter than `CHAOS_KILL_AFTER`. Re-run with a bigger run or an earlier kill:

```bash
CHAOS_SESSIONS=12000 CHAOS_KILL_AFTER=5 bash scripts/chaos/kill_gateway.sh
```

### Beat 4: "every ledger event is missing"

Total loss. Either the gateway never produced anything, or the topic was not empty when
the run started. `make down -v && make up`, then re-run.

### Beat 4: the loss window is 0

That is a measurement of that run, and it is allowed: with a healthy broker the un-acked
window is small and can be empty at the instant of the kill. Say so *as this run's
measurement*, with the bound beside it. Do not turn it into a claim. If you want a bigger
window to show, kill during a heavier run (`CHAOS_SESSIONS=12000`).

### Beat 5: "the DLQ holds N records and M were injected"

Either the topic was not reset, or the ledger is stale. `make down -v && make up`, then
re-run. If the two numbers differ by one or two rather than wildly, suspect a topic reset
that did not take.

### Beat 5: "the run injected N events where the plan over its own corpus says M"

The corpus recount and the run disagree, so they were not the same run. Check that
`CHAOS_SEED` is the same in both places — the script passes `--seed "$SEED"` to the run
and recounts with the same seed. If you overrode `CHAOS_SEED` by exporting it, the run and
the recount both move together; if you edited the script, they will not.

### Beat 5: `error_context_leaks` above 0

Stop and look. That means a rejection reason quoted the offending value or an identifier,
which is a real PII finding in a second Kafka topic. `make logs` and find the reason
format. Do not present this beat as a pass.

### Beat 6: "the flood produced no 429 at all"

**The most likely failure on the day, and it is arithmetic, not a bug.** The per-tenant
budget is 2,000 events/sec with a burst of 4,000. A flood only exceeds it if the clients
offer *more* than 2,000 events/sec to that one tenant, so a slow machine, a cold pip cache
or a gateway that is sharing its CPU with the rest of the stack can all leave the flood
under the refill rate. Raise the volume, and if the three clients are not saturating the
gateway, raise the client count:

```bash
CHAOS_FLOOD_SESSIONS=14000 bash scripts/chaos/tenant_flood.sh
# or, if one client is CPU-starved and the other two are not:
CHAOS_FLOOD_CLIENTS=4 bash scripts/chaos/tenant_flood.sh
```

The script prints how many events each client sent, so you can see whether they were
refused at all. Do not drop `CHAOS_FLOOD_CLIENTS` to 1: a single client has never been
observed to outrun the bucket, at any volume.

### Beat 6: "compared 223 tenants, expected 499"

`CHAOS_SESSIONS` is too small for the Zipf tail. Apportionment only gives the smallest of
500 tenants a session at about 6,000 sessions; below that "the other 499" is not yet a true
sentence about the run. Raise it:

```bash
CHAOS_SESSIONS=9000 bash scripts/chaos/tenant_flood.sh
```

### Beat 6: "N event(s) arrived with a sequence lower than one already seen"

The flood clients' user slices overlapped, so two clients interleaved one user's
journey. The script fails rather than smoothing it over because that is exactly the
ordering regression the partition key exists to prevent. `CHAOS_FLOOD_USERS` must be at
least `CHAOS_FLOOD_CLIENTS`, and the slices are `flood.users[shard::clients]`.

### Beat 6: "tenant_0042 is not in GATEWAY_TENANTS"

Pick a tenant that is, or set `CHAOS_FLOOD_TENANT`. A tenant the gateway does not know is
a `403` on every request, which looks like a limiter failure and is not one.

### Beat 2: the gateway does not become healthy

`make logs` follows the gateway's log. `make up` polls `/healthz` for ten minutes and
prints the last 40 lines if it never answers. The most common cause on a fresh clone is
the memory cap in `.wslconfig` — check that first, then `wsl --shutdown`.

### `scripts/chaos/broker_down.sh` is not part of the demo

It is the fourth scripted failure and `make chaos` runs it, but it is not one of the six
beats: it takes the broker away, and the audience watching a demo does not need to see the
stack's dependency graph removed from under it. It is also the slowest of the four, because
the producer buffer holds 50,000 records and a `503` is only possible once that many have
been enqueued. Budget about two minutes.

Run it the day before, not on the day. Two things it depends on are worth knowing if it
misbehaves: it restores the broker on exit, including on Ctrl-C and on a failed assertion,
so a failed run cannot leave the machine without one; and the outage run is deliberately
**not** reconciled, because with the broker gone `tools.verify` would exit 2 — "I read
nothing" — and a scorecard for an unreadable topic is worse than no scorecard. The
reconciled evidence is the run *after* the broker returns.

### The rule

If a beat fails and you cannot get it green in one re-run, **say so on stage and show the
failure output.** Every failure line in these scripts names the assertion that did not
hold. A demo that shows a real, correctly-reported failure and then explains it is a
better argument for this system than a demo that skipped it.

---

## What was verified, and what was not

Being exact about this, because the runbook asks for it.

**Verified without Docker:**

* `bash -n` on all four scripts (Git bash 5.2, GNU bash).
* **Every script's control flow, assertions and exit codes, run against a stubbed
  `docker` on `PATH`** that records its arguments and answers from scripted
  fixtures: thirteen scenarios, and each one produced the exit status it is
  supposed to produce. Four happy paths (exit 0) and nine deliberately broken
  ones (exit 1, with the failed assertion named):
  * `kill_gateway` — the happy path, and a run where the SIGKILL landed after the
    driver had already finished cleanly, which must fail because a late kill
    measures nothing;
  * `bad_events` — the happy path; a DLQ count off by one; `tools.verify` exiting
    2 (unverified, which is neither pass nor mismatch); and a rejection whose
    `error_context` quotes an identifier;
  * `broker_down` — the happy path; a probe that never sees a 503; and a 503 with
    no `Retry-After`;
  * `tenant_flood` — the happy path; a flood that was never shed at all; a
    baseline too small for the Zipf tail to reach all 500 tenants; and a flood
    that moved another tenant's stored count.
* The **real** ingest pipeline driven in-process (the production
  `app.main.create_app`, real HTTP, real encryption, real `BucketRegistry`,
  `FakeSink` instead of Kafka) to settle two things a stub cannot: that posting
  the published ingress example at a tenant the gateway does not know earns a
  **403**, and that repointing that example's `source` at a configured tenant
  earns a **202**; and that splitting `TenantFlood` across clients by user
  produces **zero** ordering violations under `tools.verify`'s own end-to-end
  recomputation, where regrouping it by channel produces 2,024.
* The offered rate a client can reach, measured over a real socket against that
  in-process gateway: ~1,000–2,800 events/sec from one client against a
  2,000/sec budget (which is why one client sheds nothing), and ~3,600
  events/sec from three, which shed 2,242 batches outright.
* The corpus arithmetic, computed offline with the driver's own
  `SkewedCorpus`/`plan_injection`: all 500 tenants receive at least one session at
  6,000 sessions and only 223 of them at 1,000; 6,000 sessions is 31,212 events
  and 5% of that is 1,561 injected.
* `python -m pytest -q -p no:cacheprovider`, serially.
* Every command and file path named in this document exists.

**Not verified, and it is the main caveat:** **no run of the compose stack
happened on the machine these scripts were written on — Docker was not available
there.** Every timing above is a budget, not a measurement, and these specific
things have not been observed to work:

* that `docker compose run` accepts the command override the way the scripts use
  it, and that the flood client program arrives on the container's stdin (the
  same mechanism `bad_events.sh` already relies on for its in-container
  inspection);
* that `docker compose run --rm -T driver` really does need `--no-deps` during
  the broker outage (`broker_down.sh` assumes it would otherwise start the
  broker, and gets it wrong the beat is measuring);
* that `docker compose up -d --force-recreate kafka-init` re-runs the one-shot
  topic creator after its container has already exited successfully;
* that the gateway answers `503` within `CHAOS_PROBE_BUDGET` seconds of a broker
  outage on this hardware, and that the outage run is large enough to fill the
  50,000-record buffer first;
* the aggregate event rate three flood clients reach against the real gateway
  behind a real broker. The measurement above used a `FakeSink`, so the demo
  machine's number will differ; the beat asserts on the *outcome* (429s, a rising
  denial counter) and not on a rate, and it fails loudly rather than quietly if
  the clients cannot outrun the bucket.

**Therefore: run `make up`, `make test`, and all four chaos scripts the day
before you present.** Not on the day. `make chaos` runs the four beats in order
and propagates the first failure, which is what you want to discover in rehearsal
rather than in front of people.