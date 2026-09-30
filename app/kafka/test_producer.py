"""T3a: the Kafka sink. RED before implementation.

What is pinned here, and why each one is a test rather than a comment:

1. **The producer config is `app.config.kafka_producer_config()` verbatim.** The
   config is not something this module gets to choose: `enable.idempotence`
   being off would produce duplicates and reorderings on retry with no client
   raising anything, which is the one failure mode the ordering and dedup claims
   in the plan cannot survive.
2. **A dedicated thread owns the client and calls `poll()`.** The async request
   path must only enqueue, or a broker round-trip lands on the event loop.
3. **A full queue is a `503`, not a block and not a drop.** The record that did
   not fit must still be absent afterwards, and the five that did fit must all
   reach the broker.
4. **Every enqueued record ends up in exactly one of three buckets**: produced,
   failed, still buffered. `produced + failed + buffered` is the number handed
   out by `sink()`, and a test asserts the identity.
5. **Shutdown drains.** SIGTERM is the normal way this process ends.
6. **A fake sink is a drop-in**, so the app can be tested with no broker.

Nothing here needs a broker: the fake client below is a `confluent_kafka`
stand-in that keeps the same three calls we actually make -- `produce`,
`poll`, `flush`.
"""

from __future__ import annotations

import ast
import inspect
import logging
import pathlib
import threading
import time
from typing import Any

import pytest

from app.config import TOPIC_DLQ, TOPIC_RAW, kafka_producer_config
from app.ingest.pipeline import Sink, SinkUnavailable
from app.kafka.producer import FakeSink, KafkaSink

# --- the fake librdkafka client ----------------------------------------------


class FakeProducer:
    """Records what was produced; delivery happens on `poll`/`flush`.

    Deliberately not a mock: it stores `(topic, key, value)` and only hands the
    delivery callbacks back when the owner polls for them, which is what makes
    "the request path did not produce synchronously" observable.
    """

    def __init__(self, config: dict) -> None:
        self.config = config
        self.produced: list[tuple[str, Any, Any]] = []
        self.threads: list[str] = []
        self.polls = 0
        self.flushes = 0
        self.pending: list[tuple[Any, Any]] = []
        #: Values starting with this are failed on delivery.
        self.fail_prefix = b""

    def produce(self, topic: str, *, key: Any = None, value: Any = None, on_delivery: Any = None) -> None:
        self.threads.append(threading.current_thread().name)
        self.produced.append((topic, key, value))
        self.pending.append((on_delivery, value))

    def poll(self, timeout: float | None = None) -> int:
        self.polls += 1
        delivered = self._deliver()
        if not delivered and timeout:
            # Faithful to librdkafka: `poll(t)` blocks up to `t` seconds when there
            # is nothing to serve. Without this the fake returns instantly and the
            # owner thread spins -- which would hide whether it polls per record or
            # only when idle, and that is exactly the property under test.
            time.sleep(timeout)
        return delivered

    def flush(self, timeout: float | None = None) -> int:
        self.flushes += 1
        self._deliver()
        return 0

    def _deliver(self) -> int:
        callbacks, self.pending = self.pending, []
        for callback, value in callbacks:
            error = None
            if self.fail_prefix and value.startswith(self.fail_prefix):
                error = _FakeError("broker refused the record")
            callback(error, None)
        return 0

    def __len__(self) -> int:
        return len(self.pending)


class _FakeError(Exception):
    """Stands in for `KafkaError`; only `str()` of it is ever read."""


class GatedProducer(FakeProducer):
    """`produce` blocks until the test lets it through.

    `entered` is set on entry, so a test can wait until the owner thread has
    definitely taken the record it was working on. Without that wait "is the
    queue full?" is a race with the drain thread, and a flaky test that only
    fails when the suite runs slowly is worse than no test.
    """

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.gate = threading.Event()
        self.entered = threading.Event()

    def produce(self, topic: str, *, key: Any = None, value: Any = None, on_delivery: Any = None) -> None:
        self.entered.set()
        assert self.gate.wait(5.0), "test never released the producer"
        super().produce(topic, key=key, value=value, on_delivery=on_delivery)


class RecordingMetrics:
    """The two methods `KafkaSink` is allowed to call."""

    def __init__(self) -> None:
        self.produce_latencies: list[float] = []
        self.usage: list[tuple[int, int]] = []

    def observe_kafka_produce_latency(self, seconds: float) -> None:
        self.produce_latencies.append(seconds)

    def set_buffer_usage(self, used: int, capacity: int) -> None:
        self.usage.append((used, capacity))


# --- fixtures -----------------------------------------------------------------


@pytest.fixture
def producers() -> list[FakeProducer]:
    return []


@pytest.fixture
def build(producers):
    def _build(factory=FakeProducer, **kwargs) -> KafkaSink:
        def factory_factory(config: dict) -> FakeProducer:
            producer = factory(config)
            producers.append(producer)
            return producer

        kwargs.setdefault("bootstrap_servers", "broker:9092")
        kwargs.setdefault("client_id", "test-client")
        sink = KafkaSink(producer_factory=factory_factory, **kwargs)
        return sink

    return _build


# =============================================================================
# 1. the config is app.config's, not ours
# =============================================================================


def test_the_producer_config_is_the_one_app_config_publishes(build, producers):
    sink = build()
    try:
        assert producers[0].config == kafka_producer_config("broker:9092", "test-client")
    finally:
        sink.close()


def test_idempotence_is_on_and_nothing_is_left_to_a_default(build, producers):
    """`retries` unbounded + `acks=all` + idempotence is what makes a retry safe.

    Without idempotence, a retried produce duplicates the record and can reorder
    it, and *no client raises* -- so the duplicate is invisible to us and to the
    client. That is why the flag is asserted rather than trusted.
    """
    sink = build()
    try:
        config = producers[0].config
        assert config["enable.idempotence"] is True
        assert config["acks"] == "all"
        assert config["max.in.flight.requests.per.connection"] == 5
        assert config["queuing.strategy"] == "fifo"
    finally:
        sink.close()


# =============================================================================
# 2. a dedicated thread owns the client
# =============================================================================


def test_producing_happens_on_our_own_thread_not_the_caller_s(build, producers):
    sink = build()
    try:
        sink.sink(TOPIC_RAW, "k", b"v")
        deadline = time.monotonic() + 5.0
        while not producers[0].produced and time.monotonic() < deadline:
            time.sleep(0.005)

        assert producers[0].produced == [(TOPIC_RAW, "k", b"v")]
        assert producers[0].threads == [producers[0].threads[0]]
        assert threading.current_thread().name != producers[0].threads[0]
    finally:
        sink.close()


def test_the_async_path_only_enqueues(build, producers):
    """`sink()` returns before the client is touched.

    This is the property that keeps a broker round-trip off the event loop: the
    request thread hands bytes to a bounded queue and returns.
    """
    sink = build(factory=GatedProducer)
    try:
        sink.sink(TOPIC_RAW, "k", b"v")
        assert producers[0].produced == []  # the worker thread is blocked in produce()
    finally:
        producers[0].gate.set()
        sink.close()


def test_the_owner_thread_polls_the_client(build, producers):
    sink = build()
    try:
        deadline = time.monotonic() + 5.0
        while producers[0].polls == 0 and time.monotonic() < deadline:
            time.sleep(0.005)
        assert producers[0].polls > 0
    finally:
        sink.close()


# =============================================================================
# 3. the queue is bounded, and full is a 503
# =============================================================================


def test_a_full_queue_raises_sink_unavailable_rather_than_blocking_or_dropping(build, producers):
    sink = build(factory=GatedProducer, maxsize=4)
    try:
        sink.sink(TOPIC_RAW, "k0", b"v")
        assert producers[0].entered.wait(5.0)  # the owner took record #0
        for i in range(1, 5):  # these four fill the queue exactly
            sink.sink(TOPIC_RAW, f"k{i}", b"v")

        with pytest.raises(SinkUnavailable):
            sink.sink(TOPIC_RAW, "k5", b"never-accepted")

        assert sink.buffered == 5
    finally:
        producers[0].gate.set()
        sink.close()

    # Nothing was dropped: the five we accepted all reached the client, and the
    # refused sixth is absent from the client's view of the world.
    assert [value for _topic, _key, value in producers[0].produced] == [b"v"] * 5


def test_the_queue_is_the_configured_size(build):
    sink = build(maxsize=7)
    try:
        assert sink.capacity == 7
        assert sink._queue.maxsize == 7
    finally:
        sink.close()


def test_a_refused_record_does_not_leak_into_the_buffer(build, producers):
    sink = build(factory=GatedProducer, maxsize=1)
    try:
        sink.sink(TOPIC_RAW, "a", b"1")
        assert producers[0].entered.wait(5.0)
        sink.sink(TOPIC_RAW, "b", b"2")
        with pytest.raises(SinkUnavailable):
            sink.sink(TOPIC_RAW, "c", b"3")
        assert sink.buffered == 2
    finally:
        producers[0].gate.set()
        sink.close()
    assert [key for _topic, key, _value in producers[0].produced] == ["a", "b"]


# =============================================================================
# 4. every record is accounted for
# =============================================================================


def test_every_record_is_produced_failed_or_still_buffered(build, producers):
    sink = build()
    handed_out = 200
    for i in range(handed_out):
        sink.sink(TOPIC_RAW, f"k{i}", b"v")

    sink.close()

    stats = sink.stats()
    assert stats.produced == handed_out
    assert stats.failed == 0
    assert stats.buffered == 0
    assert stats.produced + stats.failed + stats.buffered == handed_out


def test_a_failed_delivery_is_counted_and_logged_without_its_payload(build, producers, caplog):
    sink = build()
    producers[0].fail_prefix = b"reject"
    with caplog.at_level(logging.ERROR, logger="app.kafka"):
        sink.sink(TOPIC_RAW, "k", b"reject-me")
        sink.sink(TOPIC_RAW, "k", b"accept-me")
        sink.close()

    stats = sink.stats()
    assert (stats.produced, stats.failed, stats.buffered) == (1, 1, 0)
    # The value is PII (ciphertext or not) and the key is a user pseudonym. Neither
    # may reach a log line.
    assert "reject-me" not in caplog.text


def test_buffered_counts_records_still_in_flight(build, producers):
    sink = build()
    sink.sink(TOPIC_RAW, "k", b"v")
    deadline = time.monotonic() + 5.0
    while not sink.buffered and time.monotonic() < deadline:
        time.sleep(0.005)
    assert sink.buffered == 1
    sink.close()
    assert sink.buffered == 0


def test_deliveries_are_served_while_records_flow_not_only_when_the_queue_runs_dry(build, producers):
    """The owner thread polls as it hands off, not just when it has nothing to do.

    Under sustained load the queue is never empty, so a loop that only polled when
    idle would serve every delivery callback after the load stopped -- and the three
    things callbacks drive would be stale exactly when they matter: `buffered` would
    read a full queue while the broker had long since acked everything, the
    produce-latency histogram would measure queue depth instead of broker latency,
    and the DLQ counter would trail. This is a measurement bug, not a performance one.

    `polls >= produced` is the assertion: an idle loop polls at ~100/sec and this
    burst lasts milliseconds, so idle polls cannot account for the difference.
    """
    sink = build(maxsize=5_000)
    produced = 300  # below the in-flight high-water, so the back-pressure path cannot poll for us
    try:
        for i in range(produced):
            sink.sink(TOPIC_RAW, f"k{i}", b"v")

        deadline = time.monotonic() + 5.0
        while sink.buffered and time.monotonic() < deadline:
            time.sleep(0.005)

        assert sink.buffered == 0
        assert sink.stats().produced == produced
        assert producers[0].polls >= produced, (
            f"{producers[0].polls} poll(s) for {produced} records: callbacks are batched "
            "to the end of the burst"
        )
    finally:
        sink.close()


def test_buffer_usage_is_published_for_the_gauge(build, producers):
    metrics = RecordingMetrics()
    sink = build(metrics=metrics, maxsize=32)
    for i in range(3):
        sink.sink(TOPIC_RAW, f"k{i}", b"v")
    sink.close()

    assert metrics.usage, "set_buffer_usage was never called"
    assert all(capacity == 32 for _used, capacity in metrics.usage)
    assert max(used for used, _capacity in metrics.usage) > 0
    # A full queue is the signal the 503 is about to happen, so the last reading
    # before the drain is the one that has to be visible.
    assert metrics.usage[-1][0] == 0


def test_produce_latency_is_observed_per_record(build, producers):
    metrics = RecordingMetrics()
    sink = build(metrics=metrics)
    for i in range(5):
        sink.sink(TOPIC_RAW, f"k{i}", b"v")
    sink.close()

    assert len(metrics.produce_latencies) == 5
    assert all(value >= 0.0 for value in metrics.produce_latencies)


# =============================================================================
# 5. shutdown drains
# =============================================================================


def test_shutdown_drains_the_queue(build, producers):
    sink = build()
    for i in range(500):
        sink.sink(TOPIC_RAW, f"k{i}", b"payload")
    sink.close()

    assert len(producers[0].produced) == 500
    assert sink.stats().buffered == 0
    assert producers[0].flushes >= 1, "shutdown did not flush what was already produced"


def test_close_is_idempotent(build):
    sink = build()
    sink.sink(TOPIC_RAW, "k", b"v")
    sink.close()
    sink.close()
    assert sink.stats().buffered == 0


def test_a_sink_after_close_refuses_rather_than_accepting_into_the_void(build):
    sink = build()
    sink.close()
    with pytest.raises(SinkUnavailable):
        sink.sink(TOPIC_RAW, "k", b"v")


# =============================================================================
# 6. the DLQ topic is the producer's business
# =============================================================================


def test_sink_dlq_publishes_to_the_dlq_topic(build, producers):
    sink = build()
    sink.sink_dlq("/careers/acme_8921", b"{}")
    sink.close()

    assert [topic for topic, _key, _value in producers[0].produced] == [TOPIC_DLQ]


def test_sink_and_sink_dlq_share_one_bounded_queue(build, producers):
    """One bound, not two: a DLQ flood must be able to cause a 503 too."""
    sink = build(factory=GatedProducer, maxsize=2)
    try:
        sink.sink_dlq("k0", b"0")
        assert producers[0].entered.wait(5.0)
        sink.sink_dlq("k1", b"1")
        sink.sink_dlq("k2", b"2")
        with pytest.raises(SinkUnavailable):
            sink.sink_dlq("k3", b"3")
    finally:
        producers[0].gate.set()
        sink.close()


# =============================================================================
# 7. the fake sink is a drop-in
# =============================================================================


def test_the_fake_sink_implements_the_protocol_exactly():
    for name in ("sink", "sink_dlq"):
        assert inspect.signature(getattr(FakeSink, name)) == inspect.signature(
            getattr(Sink, name)
        )


def test_the_real_sink_implements_the_protocol_exactly():
    for name in ("sink", "sink_dlq"):
        assert inspect.signature(getattr(KafkaSink, name)) == inspect.signature(
            getattr(Sink, name)
        )


def test_the_fake_sink_keeps_what_it_was_given():
    sink = FakeSink()
    sink.sink(TOPIC_RAW, "k", b"v")
    sink.sink_dlq("d", b"w")

    assert sink.records == [(TOPIC_RAW, "k", b"v")]
    assert sink.dlq_records == [(TOPIC_DLQ, "d", b"w")]
    assert sink.buffered == 2


def test_the_fake_sink_can_be_made_to_fail_like_a_full_queue():
    sink = FakeSink(maxsize=1)
    sink.sink(TOPIC_RAW, "k", b"v")
    with pytest.raises(SinkUnavailable):
        sink.sink(TOPIC_RAW, "k", b"v")


def test_the_fake_sink_close_is_a_no_op():
    sink = FakeSink()
    sink.sink(TOPIC_RAW, "k", b"v")
    sink.close()
    assert sink.records  # closing a fake loses nothing


# =============================================================================
# 8. the dependency arrow points one way
# =============================================================================


def test_the_ingest_package_never_reaches_the_kafka_package():
    """`app.kafka` may import `app.ingest` (that is where `SinkUnavailable`
    lives); the reverse would be a cycle and would merge two owners."""
    root = pathlib.Path(__file__).resolve().parents[1] / "ingest"
    for path in sorted(root.glob("*.py")):
        if path.name.startswith("test_"):
            continue
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.ImportFrom):
                module = node.module or ""
            elif isinstance(node, ast.Import):
                module = node.names[0].name
            else:
                continue
            assert not module.startswith("app.kafka"), f"{path.name} imports {module}"
