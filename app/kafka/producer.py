"""T3a: the Kafka producer behind the gateway's `Sink`.

**The shape of the thing is not a preference.** The pipeline runs on an asyncio
loop at ~1,000 requests/sec of 500-event batches, and `confluent-kafka` is a
blocking C client: `produce()` is cheap but its delivery callbacks are served by
`poll()`, and a broker round-trip must never land on the loop. So the client
lives on one background thread per worker process, the request path only ever
`put()`s into a bounded queue, and the thread owns every call into the client.

**Bounded means bounded.** `PRODUCER_QUEUE_MAXSIZE` records. When the queue is
full, `sink()` raises `SinkUnavailable` and the handler answers `503`, because
the alternative -- blocking, or an unbounded queue -- means either latency that
grows without limit or memory that grows without limit, and both are read by
the caller as "the gateway accepted it". That is the one promise here that must
never be broken: `202` means *accepted into this buffer*
(`DURABILITY_ACCEPTED_INTO_BUFFER`), not "in Kafka", and the buffer is finite.

**Publishing is at-least-once and the accounting says so.** Every record handed
to `sink()` ends in exactly one of three states -- produced, failed, or still
buffered -- and `stats()` exposes the identity. A record that librdkafka refuses
outright is logged and counted; it is never silently discarded, because a
silently discarded record is a lie the client cannot detect.

**Configuration is not ours to choose.** Every producer value comes from
`app.config.kafka_producer_config()`, whose docstring explains the two that
matter most: `enable.idempotence=True` (with it off, a retry duplicates and
reorders with no client raising anything) and `acks=all`.
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from app.config import PRODUCER_QUEUE_MAXSIZE, TOPIC_DLQ, kafka_producer_config
from app.ingest.pipeline import SinkUnavailable

log = logging.getLogger("app.kafka")

__all__ = ["FakeSink", "KafkaSink", "ProducerClient", "SinkStats", "build_producer"]

#: Reason code for a refused enqueue. A code, never a message: this string can
#: reach a log line and an HTTP body.
REASON_QUEUE_FULL = "PRODUCER_QUEUE_FULL"
REASON_CLOSED = "PRODUCER_CLOSED"

#: How long the owner thread waits in one `poll()` when there is nothing to do.
#: Kept at `linger.ms` (10 ms, in the producer config): the client batches for up
#: to that long anyway, so a longer poll interval here would add its own delay to
#: every record that arrives while the queue is empty, and make the produce-latency
#: histogram measure this loop rather than the broker.
_IDLE_POLL_SECONDS = 0.01

#: Un-acked records we let librdkafka hold before the owner stops taking more
#: from the queue. Two buffers in memory is the thing this bound exists to
#: prevent: without it the Python queue drains into librdkafka's queue (capped
#: at `queue.buffering.max.kbytes`, which is bytes, not records) and the process
#: grows to whatever the broker's failure mode allows.
_IN_FLIGHT_HIGH_WATER = 1_000

#: Upper bound on the drain at shutdown, so a dead broker cannot hang the
#: process forever. What is still queued when it expires is logged, not hidden.
_SHUTDOWN_DRAIN_SECONDS = 10.0

#: Passed to `flush()`. librdkafka's own `message.timeout.ms` is 5s, so anything
#: still outstanding after 5s has already failed; this is the headroom.
_FLUSH_TIMEOUT_SECONDS = 5.0

#: Gauge publication interval. `set_buffer_usage` takes the registry lock and
#: writes three series; doing that per event would spend real CPU at 50k
#: events/sec on a number that is sampled, not continuous.
_USAGE_INTERVAL_SECONDS = 0.25


class ProducerClient(Protocol):
    """The three calls we make on `confluent_kafka.Producer`, and nothing else.

    Named so a test can pass an in-memory stand-in: the real client cannot be
    exercised without a broker, and the properties under test (a bounded queue,
    a poll loop, a drain) are properties of *this* class rather than of librdkafka.
    """

    def produce(self, topic: str, *, key: Any, value: Any, on_delivery: Any) -> None: ...

    def poll(self, timeout: float | None = None) -> int: ...

    def flush(self, timeout: float | None = None) -> int: ...

    def __len__(self) -> int: ...


def build_producer(config: dict) -> ProducerClient:
    """The real client. Imported lazily so `app.kafka` needs no broker library
    to be importable -- which is what lets the whole test suite (and
    `app.main`) run on a machine with no `confluent-kafka` installed."""
    from confluent_kafka import Producer

    return Producer(config)


@dataclass(frozen=True, slots=True)
class SinkStats:
    """`produced + failed + buffered` is the number of records `sink()` accepted.

    Read it, do not compute it: a stat that can disagree with the truth is worse
    than no stat, because it is the number an operator uses to decide whether a
    gateway lost data.
    """

    produced: int
    failed: int
    buffered: int
    capacity: int


@dataclass(frozen=True, slots=True)
class _Record:
    """One queued record. `enqueued_at` is when the request thread put it here,
    not when the broker got it, so the latency histogram measures what a caller
    would call latency."""

    topic: str
    key: str
    value: bytes
    enqueued_at: float


class KafkaSink:
    """`app.ingest.pipeline.Sink` on top of a real `confluent_kafka.Producer`.

    Injected into `build_ingest_router`; the pipeline never imports this module.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        client_id: str = "event-gateway",
        metrics: Any | None = None,
        maxsize: int = PRODUCER_QUEUE_MAXSIZE,
        dlq_topic: str = TOPIC_DLQ,
        producer_factory: Callable[[dict], ProducerClient] | None = None,
        shutdown_drain_seconds: float = _SHUTDOWN_DRAIN_SECONDS,
        flush_timeout: float = _FLUSH_TIMEOUT_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if maxsize < 1:
            raise ValueError(f"producer queue must hold at least one record, got {maxsize!r}")
        self._config = kafka_producer_config(bootstrap_servers, client_id)
        # Read the module global rather than defaulting the argument to it, so a
        # test can substitute a client without a broker and without reaching into
        # `KafkaSink.__init__.__defaults__`.
        factory = build_producer if producer_factory is None else producer_factory
        self._producer = factory(self._config)
        self._queue: queue.Queue[_Record] = queue.Queue(maxsize)
        self._metrics = metrics
        self._maxsize = maxsize
        self._dlq_topic = dlq_topic
        self._clock = clock
        self._shutdown_drain_seconds = shutdown_drain_seconds
        self._flush_timeout = flush_timeout

        self._lock = threading.Lock()
        self._buffered = 0
        self._produced = 0
        self._failed = 0
        self._closed = False
        self._last_usage_at = 0.0

        self._stop = threading.Event()
        # Daemon, so a process that never reaches its shutdown hook still exits:
        # an exit that hangs is a worse failure than the buffer loss the daemon
        # allows, and `close()` (wired to the app's lifespan) drains properly on
        # the normal SIGTERM path.
        self._thread = threading.Thread(
            target=self._run, name=f"kafka-producer-{os.getpid()}", daemon=True
        )
        self._thread.start()

    # --- Sink protocol ------------------------------------------------------

    def sink(self, topic: str, key: str, value: bytes) -> None:
        """Enqueue one record. Raises `SinkUnavailable` when the queue is full."""
        self._enqueue(topic, key, value)

    def sink_dlq(self, key: str, value: bytes) -> None:
        """Enqueue one DLQ record. The topic is the producer's, not the caller's."""
        self._enqueue(self._dlq_topic, key, value)

    def _enqueue(self, topic: str, key: str, value: bytes) -> None:
        if self._closed:
            raise SinkUnavailable(REASON_CLOSED)
        try:
            self._queue.put_nowait(_Record(topic, key, value, self._clock()))
        except queue.Full:
            # Never block. A caller that waits here is a caller holding a request
            # thread -- and worse, holding one *after* it was told nothing. 503
            # plus the same `id`s on the retry is the answer the pipeline expects.
            log.warning("producer queue full: capacity=%d topic=%s", self._maxsize, topic)
            self._publish_usage(force=True)
            raise SinkUnavailable(REASON_QUEUE_FULL) from None
        with self._lock:
            self._buffered += 1
        self._publish_usage()

    # --- the owner thread ---------------------------------------------------

    def _run(self) -> None:
        producer = self._producer
        deadline: float | None = None
        try:
            while True:
                if self._stop.is_set():
                    if deadline is None:
                        deadline = self._clock() + self._shutdown_drain_seconds
                    elif self._clock() > deadline:
                        # The broker is not draining. Say so rather than exiting
                        # quietly: these records are the difference between a
                        # retry and a lost event, and the count is what the
                        # operator needs.
                        log.error(
                            "shutdown: %d record(s) still queued after %.1fs, giving up",
                            self._queue.qsize(),
                            self._shutdown_drain_seconds,
                        )
                        break
                    elif self._queue.empty():
                        break
                record = self._take(producer)
                if record is None:
                    # Idle, or waiting for the broker: `poll()` is what serves
                    # delivery callbacks, so the loop must never spin without it.
                    producer.poll(_IDLE_POLL_SECONDS)
                    continue
                self._deliver(record)
                # Non-blocking poll after every hand-off. Without it the callbacks
                # pile up for as long as the queue stays non-empty, and the three
                # things they drive -- `buffered`, the produce-latency histogram
                # and the `len(producer)` back-pressure signal -- all read stale
                # exactly when the queue is busiest.
                producer.poll(0)
            outstanding = producer.flush(self._flush_timeout)
            if outstanding:
                log.error(
                    "shutdown: %d record(s) unacknowledged by the broker after %.1fs",
                    outstanding,
                    self._flush_timeout,
                )
        except Exception:  # pragma: no cover - the thread must never die silently
            log.exception("producer thread stopped unexpectedly")
        finally:
            self._publish_usage(force=True)

    def _take(self, producer: ProducerClient) -> _Record | None:
        if len(producer) >= _IN_FLIGHT_HIGH_WATER:
            # Back-pressure, not a block: leave the record in the queue (which is
            # bounded and counted, and turns into a 503 if it fills) rather than
            # growing a second buffer in librdkafka.
            return None
        try:
            return self._queue.get_nowait()
        except queue.Empty:
            return None

    def _deliver(self, record: _Record) -> None:
        try:
            self._producer.produce(
                record.topic,
                key=record.key,
                value=record.value,
                on_delivery=self._callback_for(record),
            )
        except Exception as exc:
            # `BufferError` is librdkafka's own queue being full (a byte cap we
            # cannot see); `KafkaException` covers the rest. Either way the record
            # is accounted for and logged -- the client already has its 202, so
            # the only honest thing left is a counter and a line that names the
            # error and nothing else.
            self._failed_record(record.topic, exc)

    def _callback_for(self, record: _Record) -> Callable[[Any, Any], None]:
        # One closure per record, because librdkafka hands the callback back to
        # us with no context and we need `record` to settle it. Unavoidable, and
        # cheap next to producing the record.
        def on_delivery(error: Any, _message: Any) -> None:
            if error is None:
                self._succeeded(record)
            else:
                self._failed_record(record.topic, error)

        return on_delivery

    def _succeeded(self, record: _Record) -> None:
        with self._lock:
            self._buffered -= 1
            self._produced += 1
        metrics = self._metrics
        if metrics is not None:
            metrics.observe_kafka_produce_latency(self._clock() - record.enqueued_at)
            if record.topic == self._dlq_topic:
                metrics.record_dlq_published()

    def _failed_record(self, topic: str, error: Exception) -> None:
        with self._lock:
            self._buffered -= 1
            self._failed += 1
        # Topic and error only. The key is a user pseudonym and the value is the
        # event, so neither may appear in a log line.
        log.error("produce failed: topic=%s error=%s", topic, error)

    # --- observation --------------------------------------------------------

    def _publish_usage(self, *, force: bool = False) -> None:
        metrics = self._metrics
        if metrics is None:
            return
        now = self._clock()
        if not force and now - self._last_usage_at < _USAGE_INTERVAL_SECONDS:
            return
        self._last_usage_at = now
        metrics.set_buffer_usage(self.buffered, self._maxsize)

    @property
    def buffered(self) -> int:
        """Records accepted by `sink()` and not yet settled by the broker."""
        return self._buffered

    @property
    def capacity(self) -> int:
        return self._maxsize

    def stats(self) -> SinkStats:
        with self._lock:
            return SinkStats(
                produced=self._produced,
                failed=self._failed,
                buffered=self._buffered,
                capacity=self._maxsize,
            )

    # --- lifecycle ----------------------------------------------------------

    def close(self) -> None:
        """Stop the owner thread and drain. Idempotent, and safe to call twice."""
        self._closed = True
        self._stop.set()
        self._thread.join(self._shutdown_drain_seconds + self._flush_timeout + _IDLE_POLL_SECONDS)
        if self._thread.is_alive():  # pragma: no cover - only on a wedged client
            log.error("producer thread did not stop within the shutdown budget")
        stats = self.stats()
        if stats.failed:
            log.error("shutdown: %d record(s) failed to publish", stats.failed)


class FakeSink:
    """An in-memory `Sink`: same protocol, no broker, no thread.

    Exists so `app.main` can be tested end to end with no Kafka, and so the
    driver can run against a gateway that is not there. Appends synchronously,
    which is the one behaviour a real sink does not have -- everything else
    (the topic table, the `503` on a full queue, `close()`) matches, so a test
    written against this one keeps its meaning when it moves to a broker.
    """

    def __init__(self, *, maxsize: int | None = None, error: Exception | None = None) -> None:
        self.records: list[tuple[str, str, bytes]] = []
        self.dlq_records: list[tuple[str, str, bytes]] = []
        self._maxsize = maxsize
        self._error = error
        self.closed = False

    def sink(self, topic: str, key: str, value: bytes) -> None:
        if self._error is not None:
            raise self._error
        if self._maxsize is not None and len(self.records) >= self._maxsize:
            raise SinkUnavailable(REASON_QUEUE_FULL)
        self.records.append((topic, key, value))

    def sink_dlq(self, key: str, value: bytes) -> None:
        if self._error is not None:
            raise self._error
        if self._maxsize is not None and len(self.dlq_records) >= self._maxsize:
            raise SinkUnavailable(REASON_QUEUE_FULL)
        self.dlq_records.append((TOPIC_DLQ, key, value))

    def close(self) -> None:
        self.closed = True

    @property
    def buffered(self) -> int:
        return len(self.records) + len(self.dlq_records)
