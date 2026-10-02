# Kafka producer tuning (plan D11)

**For: the Queue team, and for whoever changes the producer configuration next.**

## What this document is, and what it is not

It is a description of the producer configuration the gateway already ships, and
of the reasons in the code for each value. It is the doc you read *after* the
code, never instead of it.

Two things it deliberately does not contain:

* **No measured figure of any kind.** Nothing here was measured under load, and
  no number in it is a throughput, a latency or a percentile. Every number below
  is a configuration value read out of a file, or the result of constructing a
  client and observing what the client library did with it. If you need a number
  that describes behaviour under traffic, `bench/load.py` will produce one and
  attach the machine it came from — but that is a different task, with a
  different honesty rule, and this document is not it.
* **No claim about a value the code does not set.** Where a librdkafka default
  mattered to this analysis and I could not read it from the installed library,
  this document says so rather than quoting a number from memory or from a Kafka
  doc written about a different client. Those places are marked **[GAP]**.

## The single source of truth

`kafka_producer_config(bootstrap_servers, client_id)` in `app/config.py`. Every
producer value comes from that one dict; `app/kafka/producer.py` does not add to
it or override it (`KafkaSink.__init__` passes it straight to the client factory).
If this document and that function ever disagree, **the function is right** and
this document is a bug.

Its own docstring states the reason the values are explicit at all:

> Producer config with EVERY value explicit.
>
> Plan D11: no default may be relied upon, because `confluent-kafka` wraps
> librdkafka rather than the Java client whose defaults were researched, and a
> silent divergence would void our ordering and dedup claims.

The library in this repo's environment, established by running it, not by
reading a lockfile:

```
python -c "import confluent_kafka as c; print(c.version(), c.libversion())"
2.15.1 ('2.15.1', 34537983)
```

and the same build announcing itself in its own `INIT` log line — abridged here,
and emitted under `debug=all` rather than `debug=conf`:

```
librdkafka v2.15.1 (0x20f01ff) dump-probe#producer-1 initialized
  (builtin.features gzip,snappy,ssl,sasl,regex,lz4,sasl_gssapi,sasl_plain,
   sasl_scram,plugins,zstd,sasl_oauthbearer,http,oidc, SSL ZLIB SNAPPY ZSTD
   CURL SASL_SCRAM SASL_OAUTHBEARER PLUGINS HDRHISTOGRAM, debug 0xfffff)
```

Note `zstd` in `builtin.features`: the compression below is compiled into this
build, so a `zstd` failure would be a configuration failure and not a missing
codec.

`pyproject.toml` floors it at `confluent-kafka>=2.3`, not an exact pin. So the
build under compose and the build on a laptop are not guaranteed to be the same
librdkafka, and anything below that depends on a version has to be re-checked
against the version actually deployed. **[GAP: the compose and host builds were
not compared — this document's observations come from the host interpreter.]**

## Every value, and what changing it costs

Values are quoted from the function. Rationale is quoted from the code that
depends on the value; where the code gives no reason, the row says so rather than
inventing one.

| Key | Value | Why, quoted from the code | What changing it costs |
| --- | --- | --- | --- |
| `bootstrap.servers` | the argument | `Settings.kafka_bootstrap_servers` reads `KAFKA_BOOTSTRAP`, falling back to `localhost:9092`; `docker-compose.yml` sets `KAFKA_BOOTSTRAP: kafka:9092` for the gateway service. | Nothing clever. It is the one value with no tuning dimension, and it is set explicitly so a default can never be what found the broker. |
| `client.id` | the argument, default `"event-gateway"` | No rationale is given in the code. It is the client identity that appears in the broker's request logs and quota accounting. | Renaming it changes what a broker log shows and which client quota bucket the gateway's traffic lands in. |
| `enable.idempotence` | `True` | "`enable.idempotence=false` is forbidden: with it off, retries produce duplicates and reordering with no error raised by any client." | See the section below. It is the one value that must not change. |
| `acks` | `"all"` | Named in `app/kafka/producer.py`'s module docstring as one of "the two that matter most". | `acks=1` with idempotence is rejected outright by this librdkafka (measured — see below). Without idempotence, `acks=1` would be a choice to ack on the leader alone, which is the setting the dedup claim would then not survive. |
| `retries` | `2_147_483_647` | No rationale is given in the code. It is `INT32_MAX`: retry effectively forever, and let `message.timeout.ms` end it. | `retries=0` with idempotence is rejected outright by this librdkafka (measured). Lowering it below `INT32_MAX` trades "give up only on the timeout" for "give up sooner", which turns a transient broker blip into a failed produce. |
| `max.in.flight.requests.per.connection` | `5` | No rationale is given in the code. It is the largest value that still preserves ordering under idempotent retries, so it is set rather than inherited. | `6` or more with idempotence is rejected outright by this librdkafka (measured). Lowering it to 1 serialises per-connection and trades throughput for the same ordering guarantee. |
| `compression.type` | `"zstd"` | No rationale in the producer config. Compression happens client-side, before the produce request, which is why it is set here and not left to the topic. `docker-compose.yml` also sets `compression.type=zstd` on both topics at creation, so the topic and the client state the same intent. | Setting it to `none` makes every request bigger on the wire and on the log, and leaves the topic's `zstd` config describing something no producer is using. It changes bytes, not ordering and not dedup. |
| `compression.zstd.level` | `3` | **This value does not work.** See the finding below. | — |
| `batch.size` | `262_144` (256 KiB) | No rationale in the producer config. `app/metrics.py`'s `KAFKA_PRODUCE_LATENCY_BUCKETS` docstring notes the produce-latency histogram's top bucket is `5.0`, "comfortably past `message.timeout.ms`". | It is the *upper* bound on one produce request, not a latency setting: a request goes out when either `batch.size` bytes have accumulated or `linger.ms` has elapsed, so the smallest possible produce latency is set by `linger.ms` alone. Raising this only lets requests grow under heavy traffic — and raises the chance of one exceeding the broker's `message.max.bytes`. |
| `linger.ms` | `10` | `app/kafka/producer.py`: `_IDLE_POLL_SECONDS` "Kept at `linger.ms` (10 ms, in the producer config): the client batches for up to that long anyway, so a longer poll interval here would add its own delay to every record that arrives while the queue is empty, and make the produce-latency histogram measure this loop rather than the broker." | Raising it past 10 ms without raising `_IDLE_POLL_SECONDS` reintroduces exactly that: the owner thread's poll would be the thing the produce-latency histogram is measuring. The two are coupled. |
| `queue.buffering.max.kbytes` | `65_536` (64 MiB) | `app/kafka/producer.py`: without `_IN_FLIGHT_HIGH_WATER`, "the Python queue drains into librdkafka's queue (capped at `queue.buffering.max.kbytes`, which is bytes, not records) and the process grows to whatever the broker's failure mode allows." | Raising it raises a worker's worst-case RSS by roughly the same amount. Lowering it below the working set turns into produce failures, not a smaller footprint. |
| `message.timeout.ms` | `5_000` | The inline comment: `not delivery.timeout.ms: librdkafka name`. `app/metrics.py` gives the consequence: the produce-latency histogram's tail is "the wide tail ... `message.timeout.ms=5000` from the producer config", and "the buckets have to reach it or the p99 would look fine while the failures hide in `+Inf`." `app/kafka/producer.py`'s `_FLUSH_TIMEOUT_SECONDS` is chosen against it. | Lowering it makes more produces fail on a slow broker, and a failed produce is a record whose caller already holds a `202` and which nothing else will report. Raising it past the `KAFKA_PRODUCE_LATENCY_BUCKETS` top bound (`5.0`) would leave timeouts invisible in the histogram. |
| `queuing.strategy` | `"fifo"` | No rationale in the producer config. It fixes the order in which the local queue hands records to the sender, which `enable.idempotence` alone does not: idempotence makes a *retry* safe, it does not stop the queue from handing records over out of production order. | Setting it to `"lifo"` reorders the stream within a partition while raising nothing and duplicating nothing — the worst kind of breakage, because nothing counts it. That is why it is pinned rather than inherited. |
| `partitioner` | `"consistent_random"` | The inline comment: `hashes the key we supply`. The key is `contracts.attributes.derive_kafka_key` = `f"{career_site_id}|{user_id_pseudo}"`, computed by the gateway; a client-supplied `partitionkey` is never used for routing. | Changing it breaks the mechanism the ordering claim rests on: one `(tenant, user)` pair must land on one partition so a sticky-routed user stays on one consumer. |

### Finding: `compression.zstd.level` is not a librdkafka property

Measured on the installed library, by constructing one `confluent_kafka.Producer`
per value:

```python
from confluent_kafka import Producer
from app.config import kafka_producer_config
for k, v in kafka_producer_config("localhost:9092", "event-gateway").items():
    Producer({"bootstrap.servers": "localhost:9092", k: v})
```

Twelve of the thirteen values are accepted. One raises:

```
REJECT  compression.zstd.level = 3
        -> KafkaError{code=_INVALID_ARG, val=-186,
                      str='No such configuration property: "compression.zstd.level"'}
```

So `kafka_producer_config()`'s output **cannot be handed to `Producer()` at all**
in this environment: the whole dict fails, not just the one key. `librdkafka`'s
codec property is `compression.codec`, and `compression.type` is accepted as its
alias — a separate `debug=conf` probe setting `compression.type=zstd` and
`compression.level=3` dumped:

```
%7|...|CONF|rdkafka#producer-1| [thrd:app]: Client configuration:
%7|...|CONF|rdkafka#producer-1| [thrd:app]:   compression.codec = zstd
...
%7|...|CONF|rdkafka#producer-1| [thrd:app]: Default topic configuration:
%7|...|CONF|rdkafka#producer-1| [thrd:app]:   compression.level = 3
```

So `compression.level` is the level property this library has, and it lives in a
different namespace (`Default topic configuration:`) from the codec. The two
alternative spellings `zstd.level` and `zstd.compression.level` are also
rejected.

This is reported, not fixed: `app/config.py` is not this task's file, and
changing a D11 value is a decision for whoever owns D11. The likely one-line fix
is `compression.zstd.level` → `compression.level`, but **[GAP: it has not been
verified that `compression.level=3` alongside `compression.type=zstd` is what
was intended, nor what a different zstd level would do to the topics]**, and no
broker is running here to observe either. What *is* verified is the current
state: the shipped dict is rejected at construction.

## The module constants, which are the other half of the tuning story

These live in `app/kafka/producer.py`, not in the config dict, and they bound the
same system. `KafkaSink` is one owner thread per worker process; the request path
only ever `put()`s into a bounded queue.

| Constant | Value | Rationale, quoted |
| --- | --- | --- |
| `_IN_FLIGHT_HIGH_WATER` | `1_000` records | "Un-acked records we let librdkafka hold before the owner stops taking more from the queue. Two buffers in memory is the thing this bound exists to prevent: without it the Python queue drains into librdkafka's queue (capped at `queue.buffering.max.kbytes`, which is bytes, not records) and the process grows to whatever the broker's failure mode allows." |
| `_IDLE_POLL_SECONDS` | `0.01` s | "Kept at `linger.ms` (10 ms, in the producer config)..." — see the `linger.ms` row. |
| `_SHUTDOWN_DRAIN_SECONDS` | `10.0` s | "Upper bound on the drain at shutdown, so a dead broker cannot hang the process forever. What is still queued when it expires is logged, not hidden." |
| `_FLUSH_TIMEOUT_SECONDS` | `5.0` s | "Passed to `flush()`. librdkafka's own `message.timeout.ms` is 5s, so anything still outstanding after 5s has already failed; this is the headroom." |
| `PRODUCER_QUEUE_MAXSIZE` | `50_000` records | `app/config.py`: "Bounded producer queue. When full the handler returns 503 rather than accepting work it cannot hold (plan C4)." |
| `TOPIC_RAW` / `TOPIC_DLQ` | `career.events.raw` / `career.events.dlq` | `kafka-init` creates them at 12 and 3 partitions, replication factor 1, `compression.type=zstd`, and `retention.ms=21600000` (6h) and `86400000` (24h) respectively. |

A note on the record cap. `PRODUCER_QUEUE_MAXSIZE` counts **records, and one
record is one event** — `app/ingest/pipeline.py` calls `sink()` once per event,
not once per batch. So 50,000 is 50,000 events in flight un-acked, and
`queue.buffering.max.kbytes` is 64 MiB of the same events further downstream. The
two caps are on different units over overlapping populations, so **the arithmetic
will not tell you which binds first**; the gauges will, with the caveat below.

## `enable.idempotence=True`, and why it may not be turned off

The prohibition, quoted from `kafka_producer_config`'s docstring:

> `enable.idempotence=false` is forbidden: with it off, retries produce
> duplicates and reordering with no error raised by any client.

That sentence is the whole argument, and the last clause is what makes it
serious. **The corruption is silent.** Nothing raises. No exception, no log line,
no non-200, no metric movement. The client resends a record the broker already
committed, the broker appends a second copy, and every consumer downstream sees
two events with the same identity — and if retries also reordered, a consumer
sees them in the wrong order. A demo that claims "exactly once" and has
`enable.idempotence=false` is not slightly wrong; it is wrong in a way nothing in
the system will report.

Both of this project's claims rest on it:

* **The dedup claim** — one logical event, one record on the topic. That is a
  property of the producer's retry behaviour under idempotence, not of anything
  the gateway does. The gateway's own only duplicate check is *within a single
  batch* (`app/validate/events.py`, pass 4, a `seen` set over the decoded events);
  it does not and cannot dedup across requests.
* **The ordering claim** — a sticky-routed user's events stay in sequence
  (`gateway_ordering_violations_total` must read 0). Per-partition order is
  preserved across a producer retry only under idempotence, and only with
  `max.in.flight.requests.per.connection <= 5`, which is why that value is pinned
  rather than raised.

### What this librdkafka does when you turn it on

Measured on the installed library by construction, no broker required:

| Configuration | Result |
| --- | --- |
| `enable.idempotence=true` alone | accepted |
| `+ acks=1` | **rejected** — "`acks` must be set to `all` when `enable.idempotence` is true" |
| `+ retries=0` | **rejected** — "`retries` must be set >= 1 when `enable.idempotence` is true" |
| `+ max.in.flight.requests.per.connection=6` | **rejected** — "`max.in.flight` must be set <= 5 when `enable.idempotence` is true" |
| `+ max.in.flight.requests.per.connection=5` | accepted |

This is the good news about the "silent divergence" worry: **this** library
refuses an incompatible combination loudly, at construction, rather than
adjusting it behind your back. The bad news is that the loudness is a property of
librdkafka, not of this repo — a different version could differ, and the
`pyproject.toml` floor is `>=2.3`. The values in the config dict are therefore not
redundant with the check: they are the assertion, and the check is only the
belt.

The one thing this does **not** cover: idempotence is a *client-session* property.
Each uvicorn worker constructs its own `KafkaSink` and therefore its own
producer, its own producer id and its own sequence space. Four workers are four
dedup domains, not one. That is correct — the gateway never reorders one user's
events across workers when sticky routing is on — but it means "idempotence is on"
is a statement about each producer instance, not about the gateway as a whole.

## Asking librdkafka what it actually ended up with

The point of this section is the sentence in D11: *"no default may be relied
upon."* A default nobody set is exactly the thing that turns into a silent
assumption, so the client has to be able to be asked what it resolved.

### What works: the `debug=conf` dump

Verified on the installed library, no broker required, works against an
unreachable address:

```python
from confluent_kafka import Producer
Producer({"bootstrap.servers": "localhost:9092", "debug": "conf"}).flush()
```

Output, on the native log (**stderr** in this environment):

```
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]: Client configuration:
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   client.software.name = confluent-kafka-python
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   client.software.version = 2.15.1-rdkafka-2.15.1
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   metadata.broker.list = localhost:9092
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   debug = conf
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   error_cb = 00007FFCEC4460D0
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   opaque = 000002AFA60C04A0
%7|1790918111.231|CONF|rdkafka#producer-1| [thrd:app]:   dr_msg_cb = 00007FFCEC440160
```

That is the whole dump: a header, then one line per property set on the handle.
Nothing is elided. Note that `bootstrap.servers` was typed and
`metadata.broker.list` is what came back.

Two things this is genuinely good for, both demonstrated above:

1. **It shows the names librdkafka resolved to, not the ones you typed.** You
   write `bootstrap.servers`; the dump says `metadata.broker.list`. You write
   `compression.type`; the dump says `compression.codec`. If a name is being
   accepted as an *alias*, this is where you find out.
2. **It is the only way to see the second namespace.** A property set at topic
   level is dumped under a separate `Default topic configuration:` heading, which
   is how `compression.level = 3` was observed to exist at all.

For the shipped dict, the operator procedure is: add `"debug": "conf"` to the
config in a throwaway script, construct the client against a dead address, read
stderr, diff against `kafka_producer_config()`.

### What does not exist: a resolved-value API **[GAP]**

I could not establish an API for asking this client library for its **fully
resolved effective configuration** — every property with its final value,
including the ones nobody set. Specifically:

* `dir(confluent_kafka.Producer)` on the installed 2.15.1 is
  `abort_transaction, begin_transaction, close, commit_transaction, flush,
  init_transactions, list_topics, poll, produce, produce_batch, purge,
  send_offsets_to_transaction, set_sasl_credentials`. There is no `list_config`
  or equivalent. `dir(confluent_kafka.Consumer)` is `assign, assignment, close,
  commit, committed, consume, consumer_group_metadata, get_watermark_offsets,
  incremental_assign, incremental_unassign, list_topics, memberid,
  offsets_for_times, pause, poll, position, resume, seek, set_sasl_credentials,
  store_offsets, subscribe, unassign, unsubscribe` — likewise nothing.
* Searching the installed package for `list_config`, `conf_dump` and
  `effective_config` returns nothing. librdkafka's `rd_kafka_conf_dump()` is not
  exposed through the Python bindings.
* `debug=conf` dumps the properties set **on the handle**, not the full
  resolved set. The three-property config above produces eight property lines;
  the library has several hundred properties.

**So the honest statement is: for a value this repo does not set, the answer is
"librdkafka's built-in default, and this repository has not read it."** If a
default becomes load-bearing, the procedure is to read it out of the
`CONFIGURATION.md` shipped with the *exact* librdkafka build deployed — not from
a Kafka doc, and not from a doc about a different client. **[GAP: the defaults
for `retries`, `batch.size`, `linger.ms`, `queue.buffering.max.messages`,
`max.in.flight.requests.per.connection`, `request.timeout.ms`,
`socket.timeout.ms`, `reconnect.backoff.max.ms`, `enable.idempotence` and `acks`
were not read from the installed build, because no API exposes them and no
broker was available to ask. Any statement in this document about one of those
defaults would be a guess, and there is none.]**

### What does exist, for the broker and the topics

`confluent_kafka.admin.AdminClient.describe_configs` is the one config-reading
call in the installed library, and it reads the **broker's** view, not the
client's. Verified API surface:

```python
from confluent_kafka.admin import AdminClient, ConfigResource

AdminClient({"bootstrap.servers": "kafka:9092"}).describe_configs(
    [ConfigResource(ConfigResource.Type.BROKER, "1")]        # or .TOPIC, "career.events.raw"
)
# -> {ConfigResource: Future[list[ConfigEntry]]}
#    ConfigEntry(name, value, source, is_read_only, is_default, is_sensitive)
```

`ConfigResource.Type` offers `ANY, BROKER, GROUP, TOPIC, TRANSACTIONAL_ID,
UNKNOWN`, and `ConfigEntry.source` is a `ConfigSource` that distinguishes
`STATIC_BROKER_CONFIG`, `DYNAMIC_TOPIC_CONFIG`, `DEFAULT_CONFIG` and the rest —
which is exactly the "where did this value come from" answer D11 wants, for the
server side.

This is the right tool to confirm the topics were created the way
`kafka-init` intends (`partitions`, `replication.factor`, `compression.type`,
`retention.ms`) and to read `num.partitions`,
`default.replication.factor`, `min.insync.replicas` and `message.max.bytes` off
the broker. **[GAP: the *output* of this call was not observed — no broker is
running in this environment, so only the signature and the enum members are
verified, not the values it returns.]**

## The Java client's defaults are not librdkafka's defaults

A Kafka document that quotes defaults is usually quoting
`org.apache.kafka.clients.producer.ProducerConfig`. `confluent-kafka` wraps
librdkafka, and the two do not share a property namespace, a default set, or in
every case a behaviour. `kafka_producer_config`'s docstring is explicit that the
research was done against the Java client: *"confluent-kafka wraps librdkafka
rather than the Java client whose defaults were researched."*

### What I could establish from the installed library

* **The property namespace differs, measurably.** `compression.type` resolves to
  `compression.codec` in this librdkafka — the dump shows the canonical name, and
  `compression.codec` exists as a real property. The Java client has no
  `compression.codec`. So a Java-flavoured doc is describing a name that here is
  only an alias, and `compression.zstd.level` is a name that does not exist at
  all (see the finding above).
* **The timeout property is named differently, and both names exist here.**
  `message.timeout.ms` is the librdkafka spelling (which is why the code pins it
  with the comment `not delivery.timeout.ms: librdkafka name`); the Java client's
  producer equivalent is `delivery.timeout.ms`. Both were accepted by the
  installed library when set.
  **[GAP: with both set to different values, this library accepted the
  configuration without an error, and the effective winner was not determined.
  That is precisely the kind of "silent divergence" D11 exists to prevent, so
  this repo should set one and only one of the two — it does.]**
* **On the one setting that matters, this library is loud.** The three
  idempotence conflicts in the table above are rejected at construction. The Java
  client raises `ConfigException` for some conflicting settings and, for others,
  logs a warning and **silently adjusts the value** rather than failing. **[GAP:
  the Java client is not installed here, so I have not characterised exactly
  which of these three it refuses versus adjusts, and I am not going to guess.
  If you are comparing, check it yourself — that difference is the whole reason
  the values are pinned here one at a time.]**

### What I could not establish

Every *default value*. I have no way to read this librdkafka's defaults (§ above),
so I will not reproduce a table of them, and neither should you from a Java
document. The only defaults in this document are the ones this repository sets
itself.

## The honest durability story

`docker-compose.yml` says it, and it is worth repeating exactly because a `202`
feels like more than it is:

> Replication factor 1 everywhere below is a consequence of the single-broker
> demo, not a production recommendation: a real deployment is 3+ brokers with RF 3
> and `min.insync.replicas=2`. Idempotence (see app.config.kafka_producer_config)
> protects against *client* retries; it cannot protect against losing the only
> replica.

Concretely, the demo stack is `apache/kafka:3.9.0` in KRaft mode, one node
(`KAFKA_NODE_ID: 1`, `KAFKA_PROCESS_ROLES: broker,controller`, no ZooKeeper),
topics at `--replication-factor 1`, `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1`,
`KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1`,
`KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE:
"false"`, and no `min.insync.replicas` set anywhere in the file.

What follows, and what does not:

* **Client-side idempotence is fully in force and does its job.** A producer retry
  after a timeout, a rebalance, or a `ResetLeaderEpoch` does not create a second
  copy. That is what `enable.idempotence=true` is for and it is real.
* **Nothing on the broker side is redundant.** There is one replica of
  `career.events.raw`; the offsets topic and the transaction-state log have one
  replica each. If that node's disk goes, the events are gone, and no client
  setting in `app/config.py` changes that.
* **`acks=all` is currently worth the same as `acks=1` here, and that is a
  property of the demo topology, not of the config.** With replication factor 1
  there is only the leader to ack, and `min.insync.replicas` is left at the
  broker default. `acks=all` becomes load-bearing the moment RF > 1 — which is
  exactly why it is set explicitly now rather than added later, and why changing
  it is not a "clean-up".
* **The un-acked window is real and it is in memory.** `PRODUCER_QUEUE_MAXSIZE`
  is a Python `queue.Queue`. A worker killed with records in it loses them, and
  the caller already holds a `202`. That is what
  `DURABILITY_ACCEPTED_INTO_BUFFER = "accepted-into-buffer"` names, and it is why
  the gateway's `stop_grace_period: 30s` comment says that a shorter one
  "kills the drain, which is exactly the silent loss the bounded buffer exists to
  bound."

## `202` is accepted-into-a-bounded-buffer; `503` is what happens when it fills

**`202 {"accepted": n, "rejected": [...]}`.** The `durability` constant lives on
the internal `IngestResult` object, not on the wire — `to_response()` emits only
`accepted` and `rejected`. So the durability claim is not something a client reads
off the response body; it is in this document, in the code constant, and in the
log line the handler writes:

```
ingest accepted into buffer: accepted=%d rejected=%d topic=career.events.raw
```

`accepted` counts events handed to the sink — that is, put on the bounded queue
and not yet settled by the broker. A process killed in that window loses them.

**`503 {"reason": "..."}` with `Retry-After: 1`.** `SinkUnavailable.retry_after`
is `1`, and `_failure_response` uses `max(1, int(retry_after))`, so the header is
always `Retry-After: 1` — the handler's comment on the reason: "a Kafka producer
queue drains continuously, so there is nothing to wait for beyond a poll." The
reason codes you may see, all defined in code you can read:

| `reason` | Raised where | Means |
| --- | --- | --- |
| `PRODUCER_QUEUE_FULL` | `app/kafka/producer.py` | The 50,000-record Python queue is full. `_enqueue` never blocks: "Never block. A caller that waits here is a caller holding a request thread — and worse, holding one *after it was told nothing." |
| `PRODUCER_CLOSED` | `app/kafka/producer.py` | The sink is shutting down. |
| `SINK_UNAVAILABLE` | `app/ingest/pipeline.py` | The `sink()` for `career.events.raw` raised — a full queue, or an `OSError`. |
| `DLQ_UNAVAILABLE` | `app/ingest/pipeline.py` | The DLQ write failed, so a rejection could not be recorded. The pipeline's comment: "it is not lost — the client gets a 503 and resends the whole batch with the same `id`s — so failing loudly beats acknowledging a rejection we could not record." |

A `503` means the events were **never buffered**, so a resend of the same batch
with the same `id`s is safe and required. `driver/replay.py` treats 429 and 503
as the two retryable statuses (`RETRYABLE_STATUSES`) and resends the same bytes;
everything else is a property of the bytes and earns an identical answer if
resent.

**A run that is refused loudly beats a run that loses events quietly.** That is
the design intent stated at every layer: the queue is bounded rather than
unbounded, the enqueue never blocks, the loss count is a counter and a log line
rather than a silent discard, and the `202` names what it actually means. If you
are tempted to raise `PRODUCER_QUEUE_MAXSIZE` because 503s are showing up in a
demo, the queue is telling you the producer cannot keep up — raising the cap
raises the number of events at risk in a worker crash, and fixes nothing.

### One caveat about the buffer gauge

`gateway_buffer_utilisation` has the HELP text *"Producer queue buffer
utilisation, 0..1"*, and the denominator is `PRODUCER_QUEUE_MAXSIZE` — the
Python queue's cap. The numerator is `KafkaSink.buffered`, which increments on
`sink()` and decrements only in the delivery callback, so it counts records
*anywhere* in the un-acked set, including records already handed to librdkafka
(`app/kafka/test_producer.py::test_buffered_counts_records_still_in_flight`
asserts exactly that, at `1`, for a record already produced and undelivered).
**The series can therefore read above 1.0, and "0..1" in the HELP is a
description of the intent rather than of the value.** Treat it as a directional
signal — "how far from the point where 503s start" — not as a bounded fraction.
There is separately no gauge on librdkafka's own 64 MiB byte queue or on the
1,000-record `_IN_FLIGHT_HIGH_WATER`; the only back-pressure signal is
`len(producer)` inside the owner thread, which is read and not exported. This is
reported here rather than fixed, because `app/metrics.py` is not this task's
file.

## If you are the Queue team, here is what you are building on

**The topic.** `career.events.raw`, 12 partitions, RF 1, `compression.type=zstd`,
6-hour retention. `career.events.dlq`, 3 partitions, RF 1, 24-hour retention.
Created by `kafka-init`, not implicitly — `auto.create.topics.enable` is off,
because a topic created implicitly gets the broker's default partition count and
partition count is a routing decision.

**The record.** One CloudEvents 1.0 event per record, encrypted, and the value
is capped at `MAX_EVENT_BYTES = 64 KiB` **on the encrypted, serialized form** —
base64 expands ciphertext roughly 33% plus a nonce and tag per field, so an event
inside the ingress cap can still be refused. The DLQ record is a different shape
(`app.dlq.envelope.build_dlq_event`) carrying the original topic, the reason code,
the index and the stage — and a rejected event was **never** encrypted, so the
DLQ is holding plaintext candidate data by design.

**The key.** `f"{career_site_id}|{user_id_pseudo}"`, computed by the gateway, and
`partitioner=consistent_random` hashes it. So: **all events for one
`(tenant, user)` pair land on one partition, in produce order.** That is the
entire mechanism behind the ordering claim. It depends on sticky routing being
on (`STICKY_ROUTING=1` in compose) — with it off, one user's events are spread
across workers and across partitions, and `gateway_ordering_violations_total`
rises. If you see that counter non-zero, sticky routing is off; it is not a
consumer problem.

**The trade-offs you are inheriting, stated plainly:**

* *You get at-least-once from the producer and must assume duplicates anyway.*
  `enable.idempotence=true` covers the producer's **own** retries. It does not
  cover a **client** retrying the HTTP request: a `503` means the batch was never
  buffered, the client resends it with the same `id`s, and the gateway does not
  dedup across requests. **Your consumer is the only place `(source, id)`
  dedup can happen** — `contracts.ledger.dedup_key(source, event_id)`, and the
  key is `source` + `id` and not `id` alone, because `id` is only unique within a
  source. If you do not dedup, a single 503 in a demo inflates your counts and
  nothing will tell you.
* *You get ordering for one `(tenant, user)`, not globally and not per tenant.*
  `sequence` is per user. Nothing guarantees ordering across users of a tenant,
  and nothing should.
* *You get a bounded buffer, not a durable queue.* A `202` means
  `accepted-into-buffer`. The loss window is un-acked records in a worker's
  memory. In this demo that window is bounded by `PRODUCER_QUEUE_MAXSIZE`,
  `queue.buffering.max.kbytes` and `_IN_FLIGHT_HIGH_WATER` together.
* *You get RF 1.* One node, one replica, no `min.insync.replicas` override.
  `acks=all` is the right setting and currently buys you the leader alone.
* *You get a produce timeout of 5 seconds, and a `5xx` for a caller that already
  holds a `202`.* `message.timeout.ms=5_000` is the reason the produce-latency
  histogram's top bucket is `5.0`, and the reason a slow broker shows up as failed
  produces rather than as a rising tail.

**The metrics to watch**, all from `app/metrics.py`, all with a `worker` label:
`gateway_events_accepted_total`, `gateway_events_rejected_total` (both by `type`
and `sourcechannel`), `gateway_validation_failures_total` (by `reason` — codes,
never messages, because a reason reaches a Kafka topic),
`gateway_dlq_published_total`, `gateway_ordering_violations_total` (must be 0
with sticky routing on) and `gateway_ordering_unchecked_total` (a zero violation
count is only evidence when this is small), plus the gauges
`gateway_buffer_items` / `gateway_buffer_items_max` /
`gateway_buffer_utilisation` and `gateway_in_flight_batches`.
`gateway_request_latency_seconds` is the gateway's own handler time, excluding
the network, and it is a **fixed-bucket histogram** — a percentile read out of it
is a bucket *bound*, not a value. `bench/load.py` computes its own exact order
statistics client-side for exactly this reason, and says which is which in its
own output.

## The instrument caveat

`docker-compose.yml`, lines 13–16:

> The cpus: limits below sum to ~6, matching `processors=6` on purpose. They are
> not there to be polite: the driver measures the gateway, and a driver that
> shares a core with the gateway spends that core being scheduled out. A
> measurement taken that way is a measurement of the scheduler. Re-derive them
> together, never one without the other.

So: `kafka` 2.0 CPUs, `kafka-init` 0.5, `gateway` 3.0, `driver` 2.0. **If you
produce load against the gateway, run it from the `driver` service, not from a
shell inside the `gateway` container** — and read `machine.cpu_limit` in whatever
receipt you produce, because that is the number that says whether the separation
was actually in force on that run. A load run without those limits measures the
scheduler, and it will look like a gateway result.

## Reproducing the claims in this document

Everything above that is described as measured was measured by construction, with
no broker and no traffic, on the host interpreter (client 2.15.1, librdkafka
2.15.1):

* **Property acceptance/rejection** — construct one `Producer` per key/value from
  `kafka_producer_config(...)`, and per idempotence combination. The script is
  four lines and needs no network.
* **The effective-config dump** — construct a `Producer` with `{"debug": "conf"}`
  against `localhost:9092` and read stderr.
* **The `AdminClient.describe_configs` signature and enum members** — read from
  the installed `confluent_kafka.admin`.
* **The absence of a resolved-config API** — `dir()` on `Producer` and
  `Consumer`, plus a search of the installed package for `list_config`,
  `conf_dump`, `effective_config`.

What was **not** run, and is therefore not asserted here: anything against a live
broker, anything under traffic, and the output of `describe_configs`. Those are
marked **[GAP]** at the point they matter.
