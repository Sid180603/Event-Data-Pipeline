"""T-R5: the live view, `tools/observe.py`. RED before implementation.

The tests are shaped around the three ways a rate on a screen can be wrong, and
a wrong number is worse than no number:

* **A cumulative total divided by an uptime.** That is the classic fake
  events/sec. Every rate here is asserted from a counter delta over a known
  elapsed time, and one test exists purely to fail if anyone divides by uptime.
* **Per-worker series added up as if they were one.** Every family is labelled
  by `worker`, so an aggregating scraper publishes four series under one name.
  The tool must SUM them and print one number, and the test feeds it two real
  `render()` outputs concatenated -- which is what such a scrape contains.
* **A rate computed across two things that are not comparable.** A counter reset
  after a worker restart, two scrapes answered by two different workers, and two
  scrapes at the same instant all produce a finite number from inputs that do
  not support one. Each must print a caveat and NO rate. All three happen on
  demo day and none of them raises.

The parser is fed `app.metrics.Metrics.render()` rather than a hand-written
sample on purpose: a registry whose output changes shape breaks this tool, and a
fixture written by hand would agree with whatever the parser already expects.
The one exception is a deliberately malformed exposition, because that is the
input the tool has to refuse loudly rather than report as zero.
"""

from __future__ import annotations

import ast
import contextlib
import http.server
import pathlib
import re
import socket
import sys
import threading
from collections.abc import Callable, Iterator, Sequence

import pytest

from app.metrics import Metrics
from tools.observe import (
    DEFAULT_INTERVAL_SECONDS,
    ExpositionError,
    Observer,
    Reading,
    Snapshot,
    Unreachable,
    build_parser,
    main,
    parse_exposition,
    scrape,
)

SOURCE = pathlib.Path(__file__).with_name("observe.py")


# --- the gateway, as the tool will meet it -----------------------------------


def gateway(
    worker: str,
    *,
    accepted: int = 0,
    rejected: int = 0,
    batches: int = 0,
    rate_limited: int = 0,
    dlq: int = 0,
    decryptions: int = 0,
    ordering_violations: int = 0,
    responses: dict[str, int] | None = None,
    latencies: Sequence[float] = (),
    in_flight: int = 0,
    buffer_used: int = 0,
    buffer_capacity: int = 50_000,
    consumer_lag: int = 0,
) -> Metrics:
    """One worker's real registry, configured. The exposition is `render()`."""
    metrics = Metrics(worker_id=worker)
    for _ in range(accepted):
        metrics.record_event_accepted("com.careerpage.career.job-viewed", sourcechannel="WEB_APP")
    for _ in range(rejected):
        metrics.record_event_rejected("com.careerpage.career.job-viewed", sourcechannel="WEB_APP")
    for _ in range(batches):
        metrics.record_batch_accepted(10)
    for _ in range(rate_limited):
        metrics.record_rate_limit_denial()
    for _ in range(dlq):
        metrics.record_dlq_published()
    for _ in range(decryptions):
        metrics.record_decryption()
    for _ in range(ordering_violations):
        metrics.observe_sequence("/careers/acme_8921", "usr", "1")
    for status_class, count in (responses or {}).items():
        for _ in range(count):
            metrics.record_response(int(status_class[0]) * 100)
    for seconds in latencies:
        metrics.observe_request_latency(seconds)
    metrics.set_in_flight_batches(in_flight)
    metrics.set_buffer_usage(buffer_used, buffer_capacity)
    metrics.set_consumer_lag(consumer_lag)
    return metrics


# --- a live socket, so the stdlib client is exercised for real ----------------


class _Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - the stdlib names it this
        self.server.respond(self)  # type: ignore[attr-defined]

    def log_message(self, *args: object) -> None:
        # The base class writes every request to stderr, which would bury a
        # failing assertion in access logs.
        return


@contextlib.contextmanager
def serving_exposition(bodies: Sequence[bytes]) -> Iterator[str]:
    """A `/metrics` endpoint answering `bodies` in order, then the last one."""
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    server.calls = 0  # type: ignore[attr-defined]

    def respond(handler: http.server.BaseHTTPRequestHandler) -> None:
        call = server.calls
        server.calls = call + 1
        body = bodies[min(call, len(bodies) - 1)]
        handler.send_response(200)
        handler.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        handler.send_header("Content-Length", str(len(body)))
        handler.end_headers()
        handler.wfile.write(body)

    server.respond = respond  # type: ignore[attr-defined]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5.0)


@contextlib.contextmanager
def closed_port() -> Iterator[str]:
    """A port nothing is listening on, so the scrape really is refused."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    yield f"http://127.0.0.1:{port}"


class _TruncatingHandler(http.server.BaseHTTPRequestHandler):
    """Promises more bytes than it sends, then hangs up.

    This is what a scrape looks like when the worker is killed while rendering,
    and `http.client` raises `IncompleteRead` for it -- which is neither an
    `OSError` nor a `URLError`, so it is the one refusal most likely to escape
    as a traceback.
    """

    def do_GET(self) -> None:  # noqa: N802 - the stdlib names it this
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", "4096")
        self.end_headers()
        self.wfile.write(b"# HELP gateway_events_accepted_total events\n")
        self.wfile.flush()
        self.close_connection = True

    def log_message(self, *args: object) -> None:
        return


@contextlib.contextmanager
def truncating_endpoint() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _TruncatingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}/metrics"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(5.0)


# --- the observer loop, driven by a scripted clock ----------------------------


def step(values: Sequence[float]) -> Callable[[], float]:
    """A clock that walks `values` and then holds the last one."""
    index = 0

    def clock() -> float:
        nonlocal index
        value = values[min(index, len(values) - 1)]
        index += 1
        return value

    return clock


def run_view(
    bodies: Sequence[str | None],
    lines: list[str],
    *,
    samples: int | None = None,
    times: Sequence[float] | None = None,
    interval: float = 1.0,
    sleeper: Callable[[float], None] | None = None,
    url: str = "http://gateway.invalid/metrics",
) -> int:
    """Drive an `Observer` over a scripted scrape sequence.

    `None` in `bodies` stands for a scrape that fails the way a refused
    connection does, so the "gateway is not there" path is driven through the
    same exception the real scraper raises. The clock steps once per *attempt*,
    as the real one does, so a window that spans a failed scrape spans the time
    that scrape took.
    """
    remaining = list(bodies)
    attempts = len(bodies)
    moments = step(times if times is not None else [index * interval for index in range(attempts)])

    def scraper() -> str:
        head = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        if head is None:
            raise Unreachable(f"{url}: [Errno 111] Connection refused")
        return head

    return Observer(
        url=url,
        interval=interval,
        samples=samples,
        scraper=scraper,
        printer=lines.append,
        clock=moments,
        sleeper=sleeper or (lambda seconds: None),
        stamp=lambda: "00:00:00",
    ).run()


# --- the parser reads the real exposition -------------------------------------


def test_parse_reads_a_labelled_series_from_the_real_exposition():
    text = gateway("w-1", accepted=3, responses={"2xx": 2}).render()

    series = parse_exposition(text)["gateway_events_accepted_total"]

    assert len(series) == 1
    assert series[0].value == 3.0
    assert series[0].labels["type"] == "com.careerpage.career.job-viewed"
    assert series[0].labels["sourcechannel"] == "WEB_APP"
    assert series[0].labels["worker"] == "w-1"


def test_parse_separates_every_series_that_shares_a_name():
    """Four workers publish four series under one name; they must not collapse.

    Every worker has to have recorded something: `app/metrics.py` renders a keyed
    family as "exactly the keys present", so a worker that has accepted nothing
    publishes no `events_accepted` series at all rather than a zero.
    """
    text = "".join(gateway(f"w-{i}", accepted=i + 1).render() for i in range(4))

    series = parse_exposition(text)["gateway_events_accepted_total"]

    assert [s.value for s in series] == [1.0, 2.0, 3.0, 4.0]
    assert [s.labels["worker"] for s in series] == ["w-0", "w-1", "w-2", "w-3"]


def test_parse_reads_a_gauge_that_carries_no_labels():
    text = gateway("w-1", in_flight=7).render()

    assert parse_exposition(text)["gateway_in_flight_batches"][0].value == 7.0


def test_parse_reads_a_histogram_bucket_sum_and_count():
    families = parse_exposition(gateway("w-1", latencies=[0.001, 0.001, 0.02]).render())

    buckets = {
        sample.labels["le"]: sample.value
        for sample in families["gateway_request_latency_seconds_bucket"]
    }
    assert buckets["0.005"] == 2.0
    assert buckets["0.025"] == 3.0
    assert buckets["+Inf"] == 3.0
    assert families["gateway_request_latency_seconds_count"][0].value == 3.0
    assert families["gateway_request_latency_seconds_sum"][0].value == pytest.approx(0.022)


def test_parse_ignores_help_and_type_lines():
    families = parse_exposition(gateway("w-1", accepted=1).render())

    assert "gateway_events_accepted_total" in families
    assert not any(name.startswith("#") for name in families)


def test_a_malformed_exposition_is_refused_rather_than_reported_as_zero():
    """A body this parser cannot read must not become `accepted = 0`.

    Zero is a claim, and on the demo screen it is a claim that the gateway
    accepted nothing. An unreadable scrape has to say so instead.
    """
    broken = (
        "# TYPE gateway_events_accepted_total counter\n"
        "gateway_events_accepted_total not-a-number\n"
    )

    with pytest.raises(ExpositionError) as excinfo:
        parse_exposition(broken)

    assert "gateway_events_accepted_total" in str(excinfo.value)


# --- a rate is a delta over time ----------------------------------------------


def test_events_per_second_is_a_delta_divided_by_elapsed_time():
    """200 events in 4 seconds is 50/sec. Dividing the cumulative total by the
    uptime is the number this test exists to keep out of the tool."""
    before = Snapshot.from_exposition(gateway("w-1", accepted=1_000).render(), 100.0)
    after = Snapshot.from_exposition(gateway("w-1", accepted=1_200).render(), 104.0)

    reading = Reading(before, after)

    assert reading.events_per_sec == pytest.approx(50.0)
    assert reading.events_per_sec != pytest.approx(1_200 / 104.0)


def test_a_constant_rate_reads_the_same_on_every_scrape():
    """200 per scrape at a 1s interval, twice. The second reading is not 100/sec
    just because the total grew: that is the whole difference between a rate and
    a total."""
    first = Snapshot.from_exposition(gateway("w-1", accepted=200).render(), 0.0)
    second = Snapshot.from_exposition(gateway("w-1", accepted=400).render(), 1.0)
    third = Snapshot.from_exposition(gateway("w-1", accepted=600).render(), 2.0)

    assert Reading(first, second).events_per_sec == pytest.approx(200.0)
    assert Reading(second, third).events_per_sec == pytest.approx(200.0)


def test_rejected_events_and_dlq_publication_are_rated_too():
    before = Snapshot.from_exposition(gateway("w-1", rejected=10, dlq=5).render(), 0.0)
    after = Snapshot.from_exposition(gateway("w-1", rejected=30, dlq=9).render(), 2.0)

    reading = Reading(before, after)

    assert reading.rejected_per_sec == pytest.approx(10.0)
    assert reading.dlq_per_sec == pytest.approx(2.0)


def test_error_rate_is_errors_over_responses_on_the_gateway_paths():
    before = Snapshot.from_exposition(
        gateway("w-1", responses={"2xx": 100}).render(), 0.0
    )
    after = Snapshot.from_exposition(
        gateway("w-1", responses={"2xx": 190, "4xx": 9, "5xx": 1}).render(), 1.0
    )

    reading = Reading(before, after)

    assert reading.errors == 10.0
    assert reading.responses == 100.0
    assert reading.error_rate == pytest.approx(0.10)


def test_error_rate_is_none_when_no_request_arrived_in_the_window():
    """Not 0.0%. "No traffic" and "no errors" are different claims."""
    before = Snapshot.from_exposition(gateway("w-1", responses={"2xx": 5}).render(), 0.0)
    after = Snapshot.from_exposition(gateway("w-1", responses={"2xx": 5}).render(), 1.0)

    assert Reading(before, after).error_rate is None


# --- one number, summed across workers ---------------------------------------


def test_every_worker_series_is_summed_into_one_accepted_total():
    """Four workers, and the tool shows one number -- not four, and not the
    first worker's share mistaken for the whole gateway."""
    text = "".join(gateway(f"w-{i}", accepted=1_000).render() for i in range(4))

    snapshot = Snapshot.from_exposition(text, 0.0)

    assert snapshot.accepted == 4_000.0
    assert snapshot.workers == 4


def test_the_accepted_rate_sums_workers_rather_than_averaging_them():
    first = "".join(gateway(f"w-{i}", accepted=1_000).render() for i in range(4))
    second = "".join(gateway(f"w-{i}", accepted=1_200).render() for i in range(4))

    reading = Reading(
        Snapshot.from_exposition(first, 0.0), Snapshot.from_exposition(second, 1.0)
    )

    assert reading.events_per_sec == pytest.approx(800.0)


def test_gauges_are_summed_because_they_are_occupancy_not_rates():
    text = "".join(gateway(f"w-{i}", in_flight=3).render() for i in range(4))

    assert Snapshot.from_exposition(text, 0.0).in_flight == 12.0


def test_buffer_utilisation_is_the_busiest_worker_not_a_sum():
    """Four workers at 30% add up to 120%, which is not a utilisation. The gauge
    that predicts 503s is the fullest worker, so that is the one shown."""
    text = "".join(
        gateway(f"w-{i}", buffer_used=i * 10, buffer_capacity=100).render() for i in range(4)
    )

    snapshot = Snapshot.from_exposition(text, 0.0)

    assert snapshot.buffer_utilisation == pytest.approx(0.3)
    assert snapshot.buffer_items == 60.0


# --- the ways a rate can be a lie --------------------------------------------


def test_a_counter_reset_prints_no_rate():
    """`make chaos` kills the gateway. The workers come back with zeroed
    counters, and a delta across that restart is negative nonsense."""
    before = Snapshot.from_exposition(gateway("w-1", accepted=5_000).render(), 0.0)
    after = Snapshot.from_exposition(gateway("w-1", accepted=0).render(), 1.0)

    reading = Reading(before, after)

    assert reading.events_per_sec is None
    assert "reset" in (reading.caveat or "")


def test_two_scrapes_answered_by_two_different_workers_are_not_rated():
    """uvicorn hands a scrape to one of its workers, so consecutive scrapes can
    come from different processes whose counters were never comparable."""
    before = Snapshot.from_exposition(gateway("w-1", accepted=1_000).render(), 0.0)
    after = Snapshot.from_exposition(gateway("w-2", accepted=1_100).render(), 1.0)

    reading = Reading(before, after)

    assert reading.events_per_sec is None
    assert "w-1" in (reading.caveat or "") and "w-2" in (reading.caveat or "")


def test_an_unchanged_worker_set_keeps_the_rate_measurable():
    before = "".join(gateway(f"w-{i}", accepted=1_000).render() for i in range(2))
    after = "".join(gateway(f"w-{i}", accepted=1_050).render() for i in range(2))

    reading = Reading(
        Snapshot.from_exposition(before, 0.0), Snapshot.from_exposition(after, 1.0)
    )

    assert reading.events_per_sec == pytest.approx(100.0)
    assert reading.caveat is None


def test_two_scrapes_at_the_same_instant_are_not_divided_by_zero():
    before = Snapshot.from_exposition(gateway("w-1", accepted=1).render(), 5.0)
    after = Snapshot.from_exposition(gateway("w-1", accepted=2).render(), 5.0)

    reading = Reading(before, after)

    assert reading.events_per_sec is None
    assert reading.caveat


# --- the histogram is real data, or it is not shown ---------------------------


def test_p50_and_p99_are_interpolated_inside_the_bucket_they_fall_in():
    text = gateway("w-1", latencies=[0.001] * 4 + [0.02] * 4 + [0.2] * 2).render()

    latency = Snapshot.from_exposition(text, 0.0).request_latency

    # 4 observations <= 0.005, 8 <= 0.025, 10 <= 0.25.
    assert latency.quantile(0.5) == pytest.approx(0.005 + 0.020 * (5 - 4) / (8 - 4))
    assert latency.quantile(0.99) == pytest.approx(0.025 + 0.225 * (9.9 - 8) / (10 - 8))


def test_a_quantile_past_the_last_bucket_is_not_invented():
    """Everything above the top edge sits in `+Inf`, which has no upper bound,
    so a p99 there is unanswerable. It prints as `-`, not as 10.0."""
    text = gateway("w-1", latencies=[30.0] * 5).render()

    assert Snapshot.from_exposition(text, 0.0).request_latency.quantile(0.99) is None


def test_a_quantile_over_no_observations_is_not_zero():
    text = gateway("w-1").render()

    assert Snapshot.from_exposition(text, 0.0).request_latency.quantile(0.5) is None


def test_histograms_are_summed_across_workers_before_a_quantile():
    text = "".join(gateway(f"w-{i}", latencies=[0.001] * 5).render() for i in range(4))

    latency = Snapshot.from_exposition(text, 0.0).request_latency

    assert latency.count == 20.0
    # Twenty observations all inside the `le=0.005` bucket, so the median is
    # halfway from the floor (0) to that bound -- 2.5ms, not the 5ms bucket edge.
    # Reporting the edge would claim more precision than the histogram has.
    assert latency.quantile(0.5) == pytest.approx(0.0025)


# --- the observer loop --------------------------------------------------------


def test_the_first_scrape_is_a_baseline_and_reports_no_rate():
    lines: list[str] = []

    code = run_view([gateway("w-1", accepted=5_000).render()], lines, samples=1)

    body = "\n".join(lines)
    assert code == 0
    assert "baseline" in body
    assert "events/s" not in body, "a counter reading is not a rate"
    assert "5,000" in body, "the baseline still shows what the counters read"


def test_a_run_reports_one_rate_line_per_scrape_after_the_baseline():
    lines: list[str] = []
    bodies = [
        gateway("w-1", accepted=0).render(),
        gateway("w-1", accepted=1_000).render(),
        gateway("w-1", accepted=1_200).render(),
    ]

    run_view(bodies, lines, samples=3, interval=2.0)

    rates = [line for line in lines if "events/s" in line]
    assert len(rates) == 2
    assert "500.0 events/s" in rates[0]
    assert "100.0 events/s" in rates[1]


def test_the_rate_line_carries_the_error_rate_and_the_latencies():
    lines: list[str] = []
    bodies = [
        gateway("w-1", accepted=100, responses={"2xx": 10}).render(),
        gateway(
            "w-1",
            accepted=200,
            # 18 more 2xx and 2 new 5xx in the window: 2 errors over 20 requests.
            responses={"2xx": 28, "5xx": 2},
            latencies=[0.001] * 4 + [0.02] * 4 + [0.2] * 2,
        ).render(),
    ]

    run_view(bodies, lines, samples=2)

    line = next(line for line in lines if "events/s" in line)
    assert "100.0 events/s" in line
    assert "10.0% err (2.0/s)" in line
    assert "p50 10.0ms" in line
    assert "p99 238.8ms" in line


def test_an_unreachable_gateway_prints_one_line_and_keeps_going():
    """Connection refused mid-demo must not be a traceback in front of an
    audience, and must not end the run either."""
    lines: list[str] = []

    code = run_view(
        [None, gateway("w-1", accepted=100).render()], lines, samples=3
    )

    body = "\n".join(lines)
    assert code == 0
    assert "Connection refused" in body
    assert "Traceback" not in body
    assert "baseline" in body, "it recovered and took a baseline"


def test_a_gap_in_the_scrape_still_rates_the_whole_elapsed_time():
    """A failed scrape must not reset the baseline: the counter did not reset
    just because the socket did."""
    lines: list[str] = []
    bodies = [
        gateway("w-1", accepted=1_000).render(),
        None,
        gateway("w-1", accepted=1_400).render(),
    ]

    run_view(bodies, lines, samples=3, times=[0.0, 1.0, 2.0])

    rated = [line for line in lines if "events/s" in line]
    assert rated, "the third scrape must still be rated against the first"
    # 400 events over 2.0s: the failed attempt's second is inside the window,
    # not a window of its own.
    assert "200.0 events/s" in rated[0]


def test_an_unreadable_exposition_keeps_the_previous_baseline():
    lines: list[str] = []
    bodies = [gateway("w-1", accepted=1_000).render(), "gateway_broken_total nan-nope"]

    code = run_view(bodies, lines, samples=2)

    body = "\n".join(lines)
    assert code == 0
    assert "baseline" in body
    assert "Traceback" not in body
    # Named by series and line, and the garbage itself is NOT echoed: a proxy
    # answering with HTML would otherwise put a screenful of markup into the
    # middle of the demo's output.
    assert "unreadable exposition" in body
    assert "line 1" in body
    assert "nan-nope" not in body


def test_the_run_sleeps_between_scrapes_and_not_after_the_last():
    slept: list[float] = []
    bodies = [gateway("w-1", accepted=i * 100).render() for i in range(3)]

    run_view(bodies, [], samples=3, sleeper=slept.append)

    assert slept == [1.0, 1.0], "a sleep after the last scrape would hang the run"


def test_an_interrupt_ends_the_run_cleanly(capsys):
    """Ctrl-C is how this program is meant to be stopped."""

    def scraper() -> str:
        raise KeyboardInterrupt

    code = Observer(
        url="http://gateway.invalid/metrics",
        interval=0.0,
        samples=None,
        scraper=scraper,
        printer=lambda line: None,
        clock=step([0.0]),
        sleeper=lambda seconds: None,
        stamp=lambda: "00:00:00",
    ).run()

    assert code == 0


# --- the CLI, against a real socket -------------------------------------------


def test_main_scrapes_a_real_endpoint_and_rates_it(capsys):
    bodies = [
        gateway("w-1", accepted=1_000).render().encode(),
        gateway("w-1", accepted=1_500).render().encode(),
    ]
    # 0.1s, not less: `time.monotonic()` on Windows has a ~15.6ms tick, so a
    # shorter interval can put both scrapes inside one tick -- and then the tool
    # is right to refuse the rate (`test_two_scrapes_at_the_same_instant_...`).
    with serving_exposition(bodies) as base:
        code = main(["--url", f"{base}/metrics", "--samples", "2", "--interval", "0.1"])

    out = capsys.readouterr().out
    assert code == 0
    assert "baseline" in out
    assert f"{base}/metrics" in out, "the header must name the URL being scraped"
    # The arithmetic is pinned by the clock-driven tests above; this one is about
    # the real socket, so it checks the shape of the number rather than its
    # value -- the real elapsed time between two scrapes is not ours to predict.
    assert re.search(r"[\d,]+\.\d events/s", out), out


def test_main_says_so_and_keeps_going_when_the_gateway_is_not_there(capsys):
    with closed_port() as base:
        code = main(["--url", f"{base}/metrics", "--samples", "1", "--interval", "0.01"])

    out = capsys.readouterr().out
    assert code == 0, "one unreachable scrape is a message, not a failed run"
    assert base in out
    assert "Traceback" not in out


def test_a_response_cut_off_mid_body_is_unreachable_not_a_traceback():
    """A worker killed while rendering `/metrics` looks exactly like this."""
    with truncating_endpoint() as url:
        with pytest.raises(Unreachable):
            scrape(url)


def test_main_survives_a_response_cut_off_mid_body(capsys):
    with truncating_endpoint() as url:
        code = main(["--url", url, "--samples", "1", "--interval", "0.01"])

    out = capsys.readouterr().out
    assert code == 0
    assert "unreachable" in out
    assert "Traceback" not in out


def test_main_refuses_a_nonsense_interval(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(["--interval", "0"])

    assert excinfo.value.code == 2
    assert "interval" in capsys.readouterr().err


def test_the_default_url_is_the_gateway_metrics_endpoint():
    args = build_parser().parse_args([])

    assert args.url.endswith("/metrics")
    assert args.interval == DEFAULT_INTERVAL_SECONDS
    assert args.samples == 0, "0 means until interrupted"


# --- the Makefile -------------------------------------------------------------
#
# `make demo` cannot be run in CI (it needs Docker, and the machine running the
# suite is not the machine the demo runs on), so these check the things that
# would break it quietly instead. They live in this file because the Makefile's
# whole job is to be the front door to `tools/observe.py`.


MAKEFILE = pathlib.Path(__file__).with_name("..") / "Makefile"
MAKEFILE_TEXT = MAKEFILE.read_text(encoding="utf-8")

#: Written by the R3/R4 tasks, in parallel with this one. Named here so the
#: "every path in a recipe exists" test below fails on a typo and not on the two
#: files that are legitimately not in the tree yet.
PENDING = (
    "scripts/chaos/kill_gateway.sh",
    "scripts/chaos/broker_down.sh",
    "scripts/chaos/bad_events.sh",
    "scripts/chaos/tenant_flood.sh",
    "tools/verify.py",
)


def recipes() -> dict[str, list[str]]:
    """`{target: [recipe line, ...]}`, with make's leading tab stripped.

    Conditionals (`ifeq`) and variable assignments are not targets, and neither
    line starts with a tab, so the tab test alone is enough to tell them apart --
    which is also why the tab test below is worth having.
    """
    out: dict[str, list[str]] = {}
    target = ""
    for line in MAKEFILE_TEXT.splitlines():
        if line.startswith("\t"):
            out.setdefault(target, []).append(line[1:])
        elif line and not line.startswith(("#", " ")) and ":" in line and not line.startswith("."):
            target = line.split(":", 1)[0].strip()
    return out


def test_every_target_the_plan_names_is_declared_and_phony():
    required = {"up", "demo", "observe", "verify", "chaos", "test"}
    phony = set(
        next(line for line in MAKEFILE_TEXT.splitlines() if line.startswith(".PHONY:")).split()[1:]
    )

    assert required <= set(recipes()), sorted(required - set(recipes()))
    assert required <= phony, "a target that also names a file would be skipped by make"


def test_a_bare_make_runs_the_demo_and_the_shell_fails_loudly():
    """`.DEFAULT_GOAL` because a bare `make` should be the demo, and
    `.SHELLFLAGS` because a recipe that swallows a failure is a demo that lies.
    """
    assert re.search(r"^\.DEFAULT_GOAL\s*:=\s*demo\s*$", MAKEFILE_TEXT, re.M)
    assert re.search(r"^SHELL\s*:=\s*bash\s*$", MAKEFILE_TEXT, re.M)
    assert re.search(r"^\.SHELLFLAGS\s*:=\s*-eu\b.*pipefail", MAKEFILE_TEXT, re.M)
    # Comments excluded, because this file explains that nothing pipes into
    # `|| true` and the check has to be able to say so out loud.
    code = "\n".join(
        line for line in MAKEFILE_TEXT.splitlines() if not line.lstrip().startswith("#")
    )
    assert not re.search(r"\|\|\s*(true|:)\b", code), "a swallowed failure"


def test_every_recipe_line_is_indented_with_a_tab():
    """A space-indented recipe is not a recipe. make reports it as a missing
    target and moves on, which is the worst possible failure mode for a file
    whose job is to be the demo's front door."""
    indented = [line for line in MAKEFILE_TEXT.splitlines() if line and line[0].isspace()]

    assert indented, "the file has no recipe lines at all"
    for line in indented:
        assert line.startswith("\t") or line.lstrip().startswith("#"), repr(line)


def test_the_driver_target_runs_the_load_before_it_checks_the_ledger():
    """As a prerequisite the check would run FIRST, and `make driver` would
    inspect the last run's ledger without running anything."""
    recipe = recipes()["driver"]

    assert recipe[0].strip() == "$(DRIVER)"
    assert "_ledger" in recipe[-1]


def test_every_repository_path_a_recipe_names_exists():
    """A mistyped module or script path is the one thing in a Makefile that
    cannot be caught by reading it. The five entries in PENDING are the
    exception, and naming them here is what keeps that exception from quietly
    growing."""
    code = "\n".join(
        line for lines in recipes().values() for line in lines
    )
    referenced = {
        token.lstrip("./")
        for line in code.splitlines()
        for token in re.findall(r"[\w./-]*[\w-]+\.(?:sh|py|jsonl|log|yml)", line)
    }

    assert referenced, "no paths found: the regex, not the Makefile, is broken"
    missing = {
        path for path in referenced if not (MAKEFILE.parent / path).exists() and path not in PENDING
    }
    assert not missing, f"recipes name paths that are not in the tree: {sorted(missing)}"


def test_the_chaos_target_names_exactly_the_four_beats():
    lines = " ".join(recipes()["chaos"])

    for script in PENDING[:4]:
        assert f"bash {script}" in lines
    assert "scripts/" in lines
    assert lines.count(".sh") == 4, "one more script here is one nobody reviewed"


def test_the_only_address_the_demo_can_publish_is_loopback():
    """The gateway's listener speaks plaintext and carries bearer tokens and
    plaintext PII, so `0.0.0.0` must not appear anywhere near a port mapping."""
    mappings = re.findall(r"[\d.]+:\d+:\d+", MAKEFILE_TEXT)

    assert mappings, "the expected 127.0.0.1:8000:8000 instruction is missing"
    assert set(mappings) == {"127.0.0.1:8000:8000"}
    assert "0.0.0.0" not in " ".join(
        line for line in MAKEFILE_TEXT.splitlines() if not line.lstrip().startswith("#")
    )


def test_the_default_target_sequence_is_up_load_observe_verify():
    """The order is the deliverable: a live view that starts after the load run
    finishes reports zero events/sec, which is true and useless. Matched on the
    variable names rather than the expanded commands -- reimplementing make's
    expansion in a test would be a worse way to check a Makefile than reading it.
    """
    recipe = " ".join(recipes()["demo"])
    steps = ["$(MAKE) --no-print-directory up", "$(DRIVER)", "observe", "verify"]

    positions = [recipe.index(step) for step in steps]
    assert positions == sorted(positions), recipe


def test_the_test_target_runs_the_documented_pytest_command():
    assert recipes()["test"] == ["$(PYTEST)"]
    assert re.search(r"^PYTEST \?= \$\(PYTHON\) -m pytest -q -p no:cacheprovider$", MAKEFILE_TEXT, re.M)


# --- zero dependencies --------------------------------------------------------


def test_observe_imports_only_the_standard_library():
    """Parsed, not grepped: a comment mentioning `import httpx` must not fail
    this, and a real import must. This tool gets pointed at a URL on someone
    else's machine mid-demo, and the fewer things that can fail to install, the
    better."""
    roots: set[str] = set()
    for node in ast.walk(ast.parse(SOURCE.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            roots.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            roots.add(node.module.split(".")[0])
    assert roots <= sys.stdlib_module_names, f"non-stdlib imports: {sorted(roots)}"
