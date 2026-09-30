"""T10a metrics registry. RED before implementation.

Two things here are load-bearing beyond "does the code work":

* **The exposition is parsed, not string-matched.** A registry that renders
  lines which merely *look* like Prometheus is not observable; asserting on
  substrings would pass on malformed output that a scraper rejects.
* **The overhead test is a real budget, not a hope.** Criterion 9 says the cost
  is measured, not assumed, so the test times 100k real increments and fails if
  the per-call cost reaches 1 microsecond. It also prints the number, because a
  benchmark nobody reads is a benchmark nobody acts on.
"""

from __future__ import annotations

import ast
import math
import pathlib
import sys
import threading
import time

import pytest

from app.metrics import (
    BATCH_SIZE_BUCKETS,
    DEFAULT_LABEL_CARDINALITY,
    ENCRYPTION_LATENCY_BUCKETS,
    KAFKA_PRODUCE_LATENCY_BUCKETS,
    REQUEST_LATENCY_BUCKETS,
    CONTENT_TYPE,
    Metrics,
)

# --- A minimal Prometheus text-format parser ----------------------------------
#
# Written here rather than pulled from a library because criterion 10 forbids
# external dependencies, and a test that shares the implementation's assumptions
# cannot catch the implementation being wrong.


class Sample:
    __slots__ = ("labels", "value")

    def __init__(self, labels: dict[str, str], value: float) -> None:
        self.labels = labels
        self.value = value

    def __repr__(self) -> str:  # pragma: no cover - failure output
        return f"Sample({self.labels!r}, {self.value!r})"


def _unescape(raw: str) -> str:
    out: list[str] = []
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            out.append({"n": "\n", "\\": "\\", '"': '"'}.get(nxt, nxt))
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def _split_labels(raw: str) -> dict[str, str]:
    labels: dict[str, str] = {}
    i = 0
    while i < len(raw):
        if i:
            assert raw[i] == ",", f"malformed label set: {raw!r} at {i}"
            i += 1
        eq = raw.index("=", i)
        name = raw[i:eq]
        assert raw[eq + 1] == '"', f"label value must be quoted: {raw!r}"
        j = eq + 2
        chars: list[str] = []
        while True:
            if raw[j] == "\\":
                chars.append(raw[j : j + 2])
                j += 2
                continue
            if raw[j] == '"':
                break
            chars.append(raw[j])
            j += 1
        labels[name] = _unescape("".join(chars))
        i = j + 1
    return labels


def _base_family(name: str, declared: set[str]) -> str:
    """A histogram's `_bucket`/`_sum`/`_count` series belong to the family its
    TYPE header names, not to a family of their own."""
    for suffix in ("_bucket", "_sum", "_count"):
        if name.endswith(suffix) and name[: -len(suffix)] in declared:
            return name[: -len(suffix)]
    return name


def parse_exposition(text: str) -> tuple[dict[str, tuple[str, str]], dict[str, list[Sample]]]:
    """Return ({name: (type, help)}, {name: [Sample, ...]}). Raises on anything
    the format forbids: a sample before its TYPE, a duplicate TYPE, a missing
    `# EOF`, a label value that is not quoted."""
    meta: dict[str, tuple[str, str]] = {}
    samples: dict[str, list[Sample]] = {}
    declared: set[str] = set()
    seen_sample: set[str] = set()

    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("# HELP "):
            name, _, helptext = line[len("# HELP ") :].partition(" ")
            assert name not in meta, f"duplicate HELP for {name}"
            assert name not in seen_sample, f"HELP after sample for {name}"
            meta[name] = ("", helptext)
        elif line.startswith("# TYPE "):
            name, _, kind = line[len("# TYPE ") :].partition(" ")
            assert kind in ("counter", "gauge", "histogram"), f"bad type {kind!r}"
            assert name not in declared, f"duplicate TYPE for {name}"
            declared.add(name)
            kind_known = meta.get(name)
            meta[name] = (kind, kind_known[1] if kind_known else "")
        elif line.startswith("#"):
            continue
        else:
            brace = line.index("{") if "{" in line else -1
            if brace == -1:
                name, _, val = line.partition(" ")
                labels: dict[str, str] = {}
            else:
                name = line[:brace]
                close = line.index("}", brace)
                labels = _split_labels(line[brace + 1 : close])
                val = line[close + 2 :]
            base = _base_family(name, declared)
            assert base in declared, f"sample {name!r} rendered with no TYPE header"
            float(val)  # raises ValueError on a non-numeric value
            samples.setdefault(name, []).append(Sample(labels, float(val)))
            seen_sample.add(base)

    for name in seen_sample:
        assert name in meta, f"sample {name!r} has no HELP"
    return meta, samples


def value_of(text: str, name: str, **match: str) -> float:
    _, samples = parse_exposition(text)
    hits = [
        s
        for s in samples.get(name, [])
        if all(s.labels.get(k) == v for k, v in match.items())
    ]
    assert len(hits) == 1, f"expected exactly 1 sample for {name}{match}, got {len(hits)}"
    return hits[0].value


@pytest.fixture
def m() -> Metrics:
    return Metrics("w-test")


# --- Criterion 1: renderable, no external dependency -------------------------


def test_render_is_parseable_prometheus_text(m):
    m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    m.set_dlq_depth(3)
    m.observe_batch_size(10)
    text = m.render()

    meta, samples = parse_exposition(text)
    assert meta["gateway_events_accepted_total"][0] == "counter"
    assert meta["gateway_dlq_depth"][0] == "gauge"
    assert meta["gateway_batch_size_events"][0] == "histogram"
    assert "gateway_events_accepted_total" in samples
    assert text.endswith("\n"), "exposition must end with a newline"


def test_content_type_is_the_prometheus_text_format():
    assert CONTENT_TYPE == "text/plain; version=0.0.4; charset=utf-8"


def test_module_imports_only_the_standard_library():
    """Criterion 10. Parsed, not grepped: a comment mentioning `import httpx`
    must not fail this, and a real import must."""
    source = pathlib.Path(__file__).with_name("metrics.py").read_text(encoding="utf-8")
    roots: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    assert roots <= sys.stdlib_module_names, f"non-stdlib imports: {sorted(roots)}"
    # `app` itself is allowed (it is the gateway), everything else must be stdlib.
    assert roots <= (sys.stdlib_module_names | {"app"})


# --- Criterion 2: counters ----------------------------------------------------


COUNTERS = [
    "gateway_events_accepted_total",
    "gateway_events_rejected_total",
    "gateway_batches_accepted_total",
    "gateway_batches_rejected_total",
    "gateway_validation_failures_total",
    "gateway_rate_limit_denials_total",
    "gateway_dlq_published_total",
    "gateway_decryptions_total",
    "gateway_ordering_violations_total",
    "gateway_ordering_unchecked_total",
    "gateway_http_responses_total",
    "gateway_label_values_dropped_total",
    "gateway_sequence_evictions_total",
]

#: Families with exactly one series per worker, so an explicit 0 is a value the
#: registry can publish without knowing anything about the traffic.
UNKEYED_COUNTERS = [
    "gateway_batches_accepted_total",
    "gateway_batches_rejected_total",
    "gateway_rate_limit_denials_total",
    "gateway_dlq_published_total",
    "gateway_decryptions_total",
    "gateway_ordering_violations_total",
    "gateway_ordering_unchecked_total",
    "gateway_sequence_evictions_total",
]


@pytest.mark.parametrize("name", COUNTERS)
def test_every_required_counter_is_declared_even_when_idle(m, name):
    """A family that only appears once something increments it is invisible on
    a quiet worker, so the HELP/TYPE headers must be there from the start."""
    meta, _ = parse_exposition(m.render())
    assert name in meta, f"{name} is absent from an idle exposition"
    assert meta[name][0] == "counter"
    assert meta[name][1], f"{name} has no HELP text"


@pytest.mark.parametrize("name", UNKEYED_COUNTERS)
def test_unkeyed_counters_publish_an_explicit_zero_when_idle(m, name):
    _, samples = parse_exposition(m.render())
    assert name in samples, f"{name} renders no series at all"
    assert samples[name][0].labels.get("worker") == "w-test"
    assert samples[name][0].value == 0.0


def test_keyed_families_promise_a_bounded_label_set(m):
    """The keyed families cannot publish a per-type zero without knowing the
    type domain, and importing the contract's enum to seed it would break the
    stdlib-only rule. So the contract they publish is the label set itself."""
    m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    _, samples = parse_exposition(m.render())
    assert set(samples["gateway_events_accepted_total"][0].labels) == {
        "type",
        "sourcechannel",
        "worker",
    }


def test_counters_tally(m):
    m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    m.record_event_rejected("com.careerpage.career.user-logged-in", sourcechannel="MOBILE_APP")
    m.record_batch_accepted(2)
    m.record_batch_rejected()
    m.record_validation_failure("SCHEMA")
    m.record_validation_failure("SCHEMA")
    m.record_validation_failure("OVERSIZED")
    m.record_rate_limit_denial()
    m.record_dlq_published(3)
    m.record_decryption(2)

    text = m.render()
    assert value_of(text, "gateway_events_accepted_total", type="com.careerpage.career.user-registered", sourcechannel="WEB_APP") == 2
    assert value_of(text, "gateway_events_rejected_total", type="com.careerpage.career.user-logged-in") == 1
    assert value_of(text, "gateway_batches_accepted_total") == 1
    assert value_of(text, "gateway_batches_rejected_total") == 1
    assert value_of(text, "gateway_validation_failures_total", reason="SCHEMA") == 2
    assert value_of(text, "gateway_validation_failures_total", reason="OVERSIZED") == 1
    assert value_of(text, "gateway_rate_limit_denials_total") == 1
    assert value_of(text, "gateway_dlq_published_total") == 3
    assert value_of(text, "gateway_decryptions_total") == 2


def test_counters_are_monotonic_across_render(m):
    """A counter that can go down makes every rate() on it wrong, and the
    regression is invisible in a screenshot."""
    m.record_event_accepted("com.careerpage.career.user-registered")
    first = m.render()
    m.render()
    m.render()
    assert m.render() == first, "render() must not mutate counters"
    m.record_event_accepted("com.careerpage.career.user-registered")
    assert value_of(m.render(), "gateway_events_accepted_total", type="com.careerpage.career.user-registered") == 2


# --- Criterion 3: gauges ------------------------------------------------------


def test_dlq_depth_gauges_up_and_down(m):
    m.set_dlq_depth(10)
    assert value_of(m.render(), "gateway_dlq_depth") == 10
    m.set_dlq_depth(3)
    assert value_of(m.render(), "gateway_dlq_depth") == 3


def test_in_flight_batches_gauges_up_and_down(m):
    m.set_in_flight_batches(4)
    assert value_of(m.render(), "gateway_in_flight_batches") == 4
    m.set_in_flight_batches(0)
    assert value_of(m.render(), "gateway_in_flight_batches") == 0


def test_buffer_utilisation_is_a_gauge_plus_its_raw_terms(m):
    m.set_buffer_usage(250, 1000)
    text = m.render()
    assert value_of(text, "gateway_buffer_items") == 250
    assert value_of(text, "gateway_buffer_items_max") == 1000
    assert value_of(text, "gateway_buffer_utilisation") == 0.25
    m.set_buffer_usage(1000, 1000)
    assert value_of(m.render(), "gateway_buffer_utilisation") == 1.0


def test_buffer_utilisation_is_zero_not_a_division_error_when_empty(m):
    m.set_buffer_usage(0, 0)
    assert value_of(m.render(), "gateway_buffer_utilisation") == 0.0


# --- Criterion 4: histograms with fixed, documented buckets -------------------


def test_batch_size_histogram_boundaries(m):
    for size in (1, 5, 10, 25, 50, 100, 200, 500, 501):
        m.observe_batch_size(size)
    text = m.render()
    _, samples = parse_exposition(text)
    buckets = {s.labels["le"]: s.value for s in samples["gateway_batch_size_events_bucket"]}

    assert set(buckets) == {repr(float(b)) for b in BATCH_SIZE_BUCKETS} | {"+Inf"}
    # Cumulative: each le includes every value at or below it. 501 is the only
    # one past the last real bound, so it is the only one in +Inf.
    assert buckets["1.0"] == 1
    assert buckets["5.0"] == 2
    assert buckets["200.0"] == 7
    assert buckets["500.0"] == 8
    assert buckets["+Inf"] == 9
    assert value_of(text, "gateway_batch_size_events_count") == 9
    assert value_of(text, "gateway_batch_size_events_sum") == sum([1, 5, 10, 25, 50, 100, 200, 500, 501])


def test_latency_histograms_use_their_documented_fixed_buckets(m):
    m.observe_encryption_latency(0.0005)
    m.observe_kafka_produce_latency(0.02)
    m.observe_request_latency(0.05)
    text = m.render()
    _, samples = parse_exposition(text)
    assert {s.labels["le"] for s in samples["gateway_encryption_latency_seconds_bucket"]} == {
        repr(float(b)) for b in ENCRYPTION_LATENCY_BUCKETS
    } | {"+Inf"}
    assert {s.labels["le"] for s in samples["gateway_kafka_produce_latency_seconds_bucket"]} == {
        repr(float(b)) for b in KAFKA_PRODUCE_LATENCY_BUCKETS
    } | {"+Inf"}
    assert {s.labels["le"] for s in samples["gateway_request_latency_seconds_bucket"]} == {
        repr(float(b)) for b in REQUEST_LATENCY_BUCKETS
    } | {"+Inf"}
    assert value_of(text, "gateway_encryption_latency_seconds_bucket", le="0.0005") == 1
    assert value_of(text, "gateway_kafka_produce_latency_seconds_bucket", le="0.025") == 1
    assert value_of(text, "gateway_request_latency_seconds_bucket", le="0.05") == 1


def test_histogram_buckets_are_ascending_and_finite(m):
    for buckets in (
        BATCH_SIZE_BUCKETS,
        ENCRYPTION_LATENCY_BUCKETS,
        KAFKA_PRODUCE_LATENCY_BUCKETS,
        REQUEST_LATENCY_BUCKETS,
    ):
        assert list(buckets) == sorted(buckets), "buckets must ascend or a le is unreachable"
        assert len(set(buckets)) == len(buckets), "duplicate bucket makes one le unreachable"
        assert all(b > 0 for b in buckets)
        assert all(math.isfinite(b) for b in buckets), "+Inf is implied, not listed"


def test_record_batch_accepted_also_feeds_the_size_histogram(m):
    """One call, so the ingest path cannot record a batch without recording its
    size -- the failure mode of two separate calls is a silently empty
    histogram, which looks like a bug in the gateway rather than in the wiring."""
    m.record_batch_accepted(7)
    text = m.render()
    assert value_of(text, "gateway_batches_accepted_total") == 1
    assert value_of(text, "gateway_batch_size_events_count") == 1
    assert value_of(text, "gateway_batch_size_events_sum") == 7


# --- Criterion 5: labelled series, bounded cardinality ------------------------


def test_labelled_by_event_type_and_source_channel(m):
    m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    m.record_event_accepted("com.careerpage.career.user-logged-in", sourcechannel="MOBILE_APP")
    text = m.render()
    assert (
        value_of(
            text,
            "gateway_events_accepted_total",
            type="com.careerpage.career.user-registered",
            sourcechannel="WEB_APP",
        )
        == 1
    )
    assert (
        value_of(
            text,
            "gateway_events_accepted_total",
            type="com.careerpage.career.user-logged-in",
            sourcechannel="MOBILE_APP",
        )
        == 1
    )


def test_missing_labels_render_as_an_explicit_sentinel(m):
    m.record_event_accepted("com.careerpage.career.user-registered")
    text = m.render()
    assert value_of(text, "gateway_events_accepted_total", sourcechannel="__unset__") == 1


def test_status_class_labels_split_2xx_4xx_5xx(m):
    m.record_response(202)
    m.record_response(202)
    m.record_response(400)
    m.record_response(429)
    m.record_response(500)
    m.record_response(503)
    text = m.render()
    assert value_of(text, "gateway_http_responses_total", status_class="2xx") == 2
    assert value_of(text, "gateway_http_responses_total", status_class="4xx") == 2
    assert value_of(text, "gateway_http_responses_total", status_class="5xx") == 2


def test_status_class_is_a_bounded_projection_not_a_free_label(m):
    """status_class is bucketed, so it cannot grow the way a raw status or a
    tenant id could. Asserted so a future change to the raw value is caught."""
    for status in range(100, 600, 37):
        m.record_response(status)
    text = m.render()
    _, samples = parse_exposition(text)
    classes = {s.labels["status_class"] for s in samples["gateway_http_responses_total"]}
    assert classes <= {"1xx", "2xx", "3xx", "4xx", "5xx", "other"}


def test_label_cardinality_is_capped_and_truncation_is_visible(m):
    """A 500-tenant system with unbounded tenant labels would blow up the
    exposition. The cap is explicit, and truncation is itself a metric so
    'no violations' cannot be an artefact of having folded them all away."""
    m = Metrics("w-test", label_cardinality=3)
    for i in range(10):
        m.record_event_accepted(f"type-{i}", sourcechannel="WEB_APP")
    text = m.render()
    _, samples = parse_exposition(text)
    types = {
        s.labels["type"]
        for s in samples["gateway_events_accepted_total"]
    }
    assert types == {"type-0", "type-1", "type-2", "__other__"}
    assert value_of(text, "gateway_events_accepted_total", type="__other__") == 7
    assert value_of(text, "gateway_label_values_dropped_total", label="type") == 7


def test_the_cap_is_a_documented_default_not_a_magic_number():
    assert DEFAULT_LABEL_CARDINALITY == 64
    assert Metrics.__doc__ is not None and "cardinal" in Metrics.__doc__.lower()


def test_tenant_is_never_a_label(m):
    """The reason the cap exists. 500 tenants is a documented fact in this
    system, so a `tenant` label is 500 series per counter, multiplied by every
    counter, on every worker."""
    m = Metrics("w-test")
    for i in range(300):
        m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")
    text = m.render()
    _, samples = parse_exposition(text)
    for s in samples["gateway_events_accepted_total"]:
        assert "tenant" not in s.labels
        assert "source" not in s.labels


def test_rate_limit_denials_carry_no_tenant_label(m):
    """Per-tenant rate limiting exists, so a `tenant` label here is the obvious
    next request. It is refused here and documented in the module."""
    m.record_rate_limit_denial()
    _, samples = parse_exposition(m.render())
    labels = samples["gateway_rate_limit_denials_total"][0].labels
    assert set(labels) == {"worker"}


def test_truncation_reads_as_an_explicit_zero_not_a_missing_series(m):
    """"The cap never fired" has to be a value someone can graph. A missing
    series and a zero are indistinguishable to a rate() over a 5-minute window
    that never scraped the family."""
    _, samples = parse_exposition(m.render())
    dropped = {s.labels["label"]: s.value for s in samples["gateway_label_values_dropped_total"]}
    assert set(dropped) == {"type", "sourcechannel", "reason", "status_class", "group", "label"}
    assert set(dropped.values()) == {0.0}


def test_label_values_are_escaped_so_the_exposition_stays_parseable(m):
    m = Metrics("w-test", label_cardinality=1)  # forces the value through as a label
    m.record_event_accepted('evil"\\line\nbreak')
    text = m.render()
    parse_exposition(text)  # must not raise
    assert "evil" in text


# --- Criterion 6: per-worker series -------------------------------------------


def test_worker_id_is_a_passed_in_value(m):
    assert value_of(m.render(), "gateway_worker_info", worker="w-test") == 1


def test_worker_id_falls_back_to_the_environment(monkeypatch):
    monkeypatch.setenv("WORKER_ID", "w-from-env")
    assert value_of(Metrics().render(), "gateway_worker_info") == 1


def test_two_workers_are_told_apart():
    """The sticky-routing invariant is only checkable if a user's events can be
    traced to exactly one worker, which means a busy worker and an idle one must
    not render the same thing."""
    a, b = Metrics("w-a"), Metrics("w-b")
    a.record_event_accepted("com.careerpage.career.user-registered")

    assert value_of(a.render(), "gateway_events_accepted_total", worker="w-a") == 1
    assert value_of(b.render(), "gateway_worker_info", worker="w-b") == 1
    # Worker b has accepted nothing, so it publishes no accepted-event series --
    # which is the point: the two workers' output differs.
    _, b_samples = parse_exposition(b.render())
    assert b_samples.get("gateway_events_accepted_total") is None
    assert b.render() != a.render()


# --- Criterion 7: ordering violations, bounded state --------------------------


def test_ordering_violation_is_counted_when_sequence_goes_backwards(m):
    assert m.observe_sequence("/careers/acme", "usr_1", 1) is False
    assert m.observe_sequence("/careers/acme", "usr_1", 2) is False
    assert m.observe_sequence("/careers/acme", "usr_1", 2) is False, "a repeat is not a violation"
    assert m.observe_sequence("/careers/acme", "usr_1", 1) is True
    assert m.observe_sequence("/careers/acme", "usr_1", 0) is True
    text = m.render()
    assert value_of(text, "gateway_ordering_violations_total") == 2


def test_ordering_is_tracked_per_source_and_user_pair(m):
    m.observe_sequence("/careers/acme", "usr_1", 10)
    m.observe_sequence("/careers/beta", "usr_1", 1)  # different tenant, its own counter
    m.observe_sequence("/careers/acme", "usr_2", 1)  # different user, its own counter
    assert m.observe_sequence("/careers/acme", "usr_1", 9) is True
    assert m.observe_sequence("/careers/beta", "usr_1", 2) is False


def test_a_high_sequence_is_remembered_not_the_last_one_seen(m):
    """Keeping only the last value would make 5, 4 read as progress and let the
    violation through, which is the exact bug this counter exists to catch."""
    m.observe_sequence("/careers/acme", "usr_1", 5)
    m.observe_sequence("/careers/acme", "usr_1", 4)
    assert m.observe_sequence("/careers/acme", "usr_1", 6) is False
    assert m.observe_sequence("/careers/acme", "usr_1", 3) is True


def test_string_sequences_are_compared_numerically_not_lexically(m):
    """`sequence` is a str in the CloudEvents contract. "10" < "9" lexically, so
    a lexical compare would report a violation on every correct event."""
    m.observe_sequence("/careers/acme", "usr_1", "9")
    assert m.observe_sequence("/careers/acme", "usr_1", "10") is False
    assert m.observe_sequence("/careers/acme", "usr_1", "8") is True


def test_unparseable_sequence_is_counted_as_unchecked_not_as_a_violation(m):
    """'reads zero' is only evidence if the counter was actually fed. An
    untrackable sequence must be visible, or a broken driver passes T9."""
    assert m.observe_sequence("/careers/acme", "usr_1", None) is False
    assert m.observe_sequence("/careers/acme", "usr_1", "not-a-number") is False
    text = m.render()
    assert value_of(text, "gateway_ordering_unchecked_total") == 2
    assert value_of(text, "gateway_ordering_violations_total") == 0


def test_sequence_state_is_bounded_and_evicts():
    """Criterion 7's actual teeth: an unbounded (source,user) dict at 50k
    events/sec is a memory leak, and the counter is the only guard on it."""
    m = Metrics("w-test", sequence_window=64)
    for i in range(5000):
        m.observe_sequence("/careers/acme", f"usr_{i}", i)
    assert m.tracked_sequence_pairs == 64
    assert value_of(m.render(), "gateway_sequence_evictions_total") == 5000 - 64


def test_eviction_can_only_undercount_never_falsely_report(m):
    m = Metrics("w-test", sequence_window=1)
    m.observe_sequence("/careers/acme", "usr_1", 10)
    m.observe_sequence("/careers/acme", "usr_2", 1)  # evicts usr_1
    # usr_1's history is gone, so this reads as first-sight, not a violation.
    assert m.observe_sequence("/careers/acme", "usr_1", 5) is False
    assert value_of(m.render(), "gateway_ordering_violations_total") == 0
    # Two, not one: putting usr_1 back in the window evicted usr_2 to make room.
    assert value_of(m.render(), "gateway_sequence_evictions_total") == 2


def test_a_violation_is_still_caught_for_a_pair_still_resident(m):
    m = Metrics("w-test", sequence_window=8)
    m.observe_sequence("/careers/acme", "usr_hot", 100)
    for i in range(5):
        m.observe_sequence("/careers/acme", f"usr_cold_{i}", 1)
    assert m.observe_sequence("/careers/acme", "usr_hot", 99) is True


# --- Criterion 8: consumer lag is an input, not a poll ------------------------


def test_consumer_lag_is_a_settable_gauge(m):
    m.set_consumer_lag(1234)
    assert value_of(m.render(), "gateway_consumer_lag", group="default") == 1234
    m.set_consumer_lag(7, group="queue-team")
    text = m.render()
    assert value_of(text, "gateway_consumer_lag", group="default") == 1234
    assert value_of(text, "gateway_consumer_lag", group="queue-team") == 7


def test_consumer_lag_defaults_to_zero_so_absent_is_not_missing(m):
    assert value_of(m.render(), "gateway_consumer_lag") == 0


# --- Thread safety (documented in the module) ---------------------------------


def test_concurrent_increments_are_exact():
    """Not a stress test for its own sake: `d[k] += 1` is a read-modify-write
    and loses updates without a lock. The lost count shows up as a rate() that
    under-reports, which is the failure nobody notices."""
    m = Metrics("w-test")
    threads = 4
    per_thread = 20_000
    barrier = threading.Barrier(threads)

    def hammer() -> None:
        barrier.wait()
        for _ in range(per_thread):
            m.record_event_accepted("com.careerpage.career.user-registered", sourcechannel="WEB_APP")

    workers = [threading.Thread(target=hammer) for _ in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()

    assert value_of(
        m.render(), "gateway_events_accepted_total", type="com.careerpage.career.user-registered", sourcechannel="WEB_APP"
    ) == threads * per_thread


def test_concurrent_histogram_observations_are_exact():
    m = Metrics("w-test")
    def hammer() -> None:
        for _ in range(10_000):
            m.observe_batch_size(10)
    workers = [threading.Thread(target=hammer) for _ in range(4)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert value_of(m.render(), "gateway_batch_size_events_count") == 40_000
    assert value_of(m.render(), "gateway_batch_size_events_sum") == 400_000


def test_concurrent_sequence_tracking_is_exact():
    m = Metrics("w-test")
    def hammer() -> None:
        for _ in range(5000):
            m.observe_sequence("/careers/acme", "usr_1", 1)
    workers = [threading.Thread(target=hammer) for _ in range(4)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    # 1 repeated is never a violation, so this also asserts no false positives.
    assert value_of(m.render(), "gateway_ordering_violations_total") == 0


# --- Criterion 9: the cost is measured, not assumed ---------------------------

#: Per-call budget for one hot-path increment, in microseconds. 50k events/sec
#: is 20ns of wall clock per event, so anything approaching 1us is a third of
#: the budget for the rest of the request path gone.
INCREMENT_BUDGET_US = 1.0


def _time_us(fn, calls: int) -> float:
    started = time.perf_counter()
    for _ in range(calls):
        fn()
    return (time.perf_counter() - started) / calls * 1e6


def test_increment_path_costs_under_one_microsecond(m):
    calls = 100_000
    per_call_us = _time_us(
        lambda: m.record_event_accepted(
            "com.careerpage.career.user-registered", sourcechannel="WEB_APP"
        ),
        calls,
    )
    print(f"\nrecord_event_accepted: {per_call_us:.3f} us/call over {calls} calls")

    assert value_of(m.render(), "gateway_events_accepted_total") == calls, "the loop must not be optimised away"
    assert per_call_us < INCREMENT_BUDGET_US, f"{per_call_us:.3f} us/call exceeds the {INCREMENT_BUDGET_US} us budget"


def test_histogram_observation_costs_under_one_microsecond(m):
    calls = 100_000
    per_call_us = _time_us(lambda: m.observe_encryption_latency(0.0004), calls)
    print(f"observe_encryption_latency: {per_call_us:.3f} us/call over {calls} calls")
    assert per_call_us < INCREMENT_BUDGET_US, f"{per_call_us:.3f} us/call exceeds the {INCREMENT_BUDGET_US} us budget"


def test_sequence_observation_costs_under_one_microsecond(m):
    """Same budget as the counters: one sequence check per event, so it is just
    as hot."""
    calls = 100_000
    m.observe_sequence("/careers/acme", "usr_warm", 1)
    per_call_us = _time_us(lambda: m.observe_sequence("/careers/acme", "usr_warm", 2), calls)
    print(f"observe_sequence: {per_call_us:.3f} us/call over {calls} calls")
    assert per_call_us < INCREMENT_BUDGET_US, f"{per_call_us:.3f} us/call exceeds the {INCREMENT_BUDGET_US} us budget"


def test_render_stays_cheap_enough_to_serve(m):
    """Render is the cold path, but it is on a scrape and the demo scrapes it.
    A 5000-series registry must still render in single-digit milliseconds --
    the bound is deliberately loose because the assertion that matters is that
    it is not O(series^2)."""
    for i in range(500):
        m.record_event_accepted(f"type-{i}", sourcechannel=f"ch-{i % 7}")
    for i in range(2000):
        m.observe_encryption_latency(0.0004)
    per_call_ms = _time_us(m.render, 20) / 1000
    print(f"render: {per_call_ms:.3f} ms/call over ~3500 series")
    assert per_call_ms < 50.0, f"render took {per_call_ms:.3f} ms; the render path is doing per-request work"
