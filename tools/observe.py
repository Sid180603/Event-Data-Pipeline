"""T-R5: the live view. `python -m tools.observe`.

The screen the demo is watched on, so it is built around one question: **what is
this number claiming?** Every number it prints is a delta between two scrapes
divided by the elapsed time between them, and every case where that arithmetic
would be meaningless produces a stated caveat and no number at all.

Four things it will not do, each of which is a way a demo screen ends up lying:

* **Divide a cumulative counter by an uptime.** `events_accepted / process
  uptime` is a plausible-looking number that is not a rate, and it reads as one.
  The first scrape is therefore a *baseline* and reports counters, explicitly
  labelled as not-a-rate, because a rate needs two points and the tool starts
  with one.
* **Print one worker's share and call it the gateway.** Every family is
  labelled `worker`, so a `--workers 4` run publishes four series under one
  name. All of them are summed, and the header says so. A single URL is
  answered by ONE uvicorn worker, so on a multi-worker gateway this is that
  worker's share -- which is why the header says that too, rather than letting a
  viewer assume a cluster total.
* **Rate across a counter that went backwards.** `make chaos` kills the
  gateway, the workers come back with zeroed counters, and the delta across the
  restart is negative nonsense. A falling counter prints a caveat instead.
* **Rate across two scrapes answered by two different workers.** uvicorn hands
  a scrape to one of its children, so consecutive scrapes can come from
  different processes whose counters were never comparable. The `worker` label
  set is compared and a change is a caveat.

**Zero dependencies, deliberately.** `urllib.request` and `re` only -- no httpx,
no requests, no `prometheus_client`. This is a thing somebody points at a URL on
a laptop, mid-demo, while the thing that must work is the gateway; every
installed package is another thing that can be missing from a demo machine. So
the exposition is parsed here, against the format `app/metrics.py` publishes
(`CONTENT_TYPE`, version 0.0.4), and the histogram's `_sum`/`_count` are used
rather than guessed at.

**A gateway that is not there is not an error worth a traceback.** Connection
refused, a timeout, an HTTP 503 from a proxy, an exposition this parser cannot
read: each prints one line naming the URL and keeps going, and the previous
baseline is *kept* -- the counters did not restart just because the socket did.
Ctrl-C is how this program is meant to end, so it exits 0.
"""

from __future__ import annotations

import argparse
import http.client
import os
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Sequence
from dataclasses import dataclass

__all__ = [
    "DEFAULT_GATEWAY_URL",
    "DEFAULT_INTERVAL_SECONDS",
    "DEFAULT_TIMEOUT_SECONDS",
    "ERROR_CLASSES",
    "METRICS_PATH",
    "ExpositionError",
    "Histogram",
    "Observer",
    "Reading",
    "Sample",
    "Snapshot",
    "Unreachable",
    "baseline_line",
    "build_parser",
    "format_line",
    "main",
    "metrics_url",
    "parse_exposition",
    "scrape",
]

#: Where the gateway is when nothing says otherwise. `docker-compose.yml` sets
#: `GATEWAY_URL` for the driver, so the local run and the compose run differ in
#: one environment variable.
DEFAULT_GATEWAY_URL = "http://localhost:8000"
METRICS_PATH = "/metrics"

#: One second between scrapes. Fast enough that a rate is readable and slow
#: enough that `/metrics` does not become a measurable share of the request
#: rate the same screen is reporting -- `app/main.py` excludes `/metrics` from
#: the request metrics for exactly that reason.
DEFAULT_INTERVAL_SECONDS = 1.0

#: A scrape that hangs longer than this is a gateway that is not answering, and
#: the line to print is "unreachable", not "waiting". A long default would
#: freeze the screen in front of an audience with no explanation.
DEFAULT_TIMEOUT_SECONDS = 5.0

#: The status classes counted as errors, per `app/metrics.py::_status_class`.
#: `4xx` includes the 429 the limiter returns, and that is deliberate: a shed
#: batch is a request the gateway refused, and hiding it inside a separate line
#: would make the error rate flatter than the experience. The rate limiter's
#: share is printed separately so the two can be told apart.
ERROR_CLASSES = ("4xx", "5xx", "other")

# --- the series this view is built from ---------------------------------------
#
# Named literally rather than interpolated from a namespace constant: the names
# are the contract between this tool and `app/metrics.py`, and a scraper that
# silently started summing a renamed family is worse than one that fails.

EVENTS_ACCEPTED = "gateway_events_accepted_total"
EVENTS_REJECTED = "gateway_events_rejected_total"
RATE_LIMITED = "gateway_rate_limit_denials_total"
DLQ_PUBLISHED = "gateway_dlq_published_total"
ORDERING_VIOLATIONS = "gateway_ordering_violations_total"
HTTP_RESPONSES = "gateway_http_responses_total"
IN_FLIGHT = "gateway_in_flight_batches"
BUFFER_ITEMS = "gateway_buffer_items"
BUFFER_UTILISATION = "gateway_buffer_utilisation"
CONSUMER_LAG = "gateway_consumer_lag"
REQUEST_LATENCY = "gateway_request_latency_seconds"

#: The `Snapshot` fields that only ever go up, so a fall in any of them means a
#: counter reset rather than a measurement. Checked individually rather than
#: through a flag on the snapshot: the field that fell is the thing to name in
#: the caveat, because "a counter went backwards" without saying which one is a
#: shrug.
COUNTER_FIELDS = (
    "accepted",
    "rejected",
    "rate_limited",
    "dlq",
    "ordering_violations",
)

_INFINITY = float("inf")


class Unreachable(Exception):
    """A scrape that never produced a body. One line, then carry on."""


class ExpositionError(Exception):
    """A body this parser cannot read. Loud, because silence would be zero."""


# --- the parser ----------------------------------------------------------------

_LABEL = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"')
_ESCAPE = re.compile(r"\\(.)")
_ESCAPES = {"\\": "\\", '"': '"', "n": "\n"}


@dataclass(frozen=True, slots=True)
class Sample:
    """One series' labels and value. Several may share `name` -- that is the
    per-worker case, not a parse error."""

    name: str
    labels: dict[str, str]
    value: float


def _unescape(value: str) -> str:
    return _ESCAPE.sub(lambda match: _ESCAPES.get(match.group(1), match.group(0)), value)


def _parse_labels(raw: str, number: int) -> dict[str, str]:
    labels: dict[str, str] = {}
    position = 0
    while position < len(raw):
        match = _LABEL.match(raw, position)
        if match is None:
            raise ExpositionError(f"unreadable exposition: line {number}: label set {raw!r}")
        labels[match.group(1)] = _unescape(match.group(2))
        position = match.end()
        if raw[position : position + 1] == ",":
            position += 1
    return labels


def _parse_line(line: str, number: int) -> Sample:
    brace = line.find("{")
    if brace < 0:
        name, _, value_text = line.partition(" ")
        labels: dict[str, str] = {}
    else:
        name = line[:brace]
        close = line.find("}", brace)
        if close < 0:
            raise ExpositionError(f"unreadable exposition: line {number}: unterminated label set")
        labels = _parse_labels(line[brace + 1 : close], number)
        value_text = line[close + 1 :]
    name = name.strip()
    if not name:
        raise ExpositionError(f"unreadable exposition: line {number}: a sample has no metric name")
    try:
        value = float(value_text.strip())
    except ValueError:
        # The offending text is deliberately not echoed: a proxy answering with
        # an HTML error page would put a screenful of markup into the middle of
        # the demo's output.
        raise ExpositionError(f"unreadable exposition: line {number}: {name}: value is not a number") from None
    return Sample(name, labels, value)


def parse_exposition(text: str) -> dict[str, tuple[Sample, ...]]:
    """Prometheus text exposition 0.0.4 -> `{series name: (Sample, ...)}`.

    Every series is kept, including the several that share a name because they
    belong to different workers. Comments are skipped without being validated:
    `# HELP` is prose and this tool reads no prose.

    A line it cannot read raises rather than being skipped, because the two
    failures look identical to a viewer and only one of them is a lie: a
    genuinely absent family reads 0, and a family this parser mangled would too.
    """
    families: dict[str, list[Sample]] = {}
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        sample = _parse_line(line, number)
        families.setdefault(sample.name, []).append(sample)
    return {name: tuple(samples) for name, samples in families.items()}


# --- histograms ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Histogram:
    """Cumulative bucket counts summed over every worker, plus `_sum`/`_count`.

    Only cumulative counts are kept, because that is the only form a quantile is
    computed from -- the per-bucket counts `app/metrics.py` holds internally are
    cumulated at render time and are not published.
    """

    #: `(le, cumulative count)` ascending by bound; the last bound is `+Inf`.
    buckets: tuple[tuple[float, float], ...]
    count: float
    total: float

    def quantile(self, q: float) -> float | None:
        """The `q` quantile, linearly interpolated inside its bucket.

        `None` when it is unanswerable -- no observations at all, or the rank
        landing in `+Inf`, which has no upper bound to interpolate towards. Both
        cases print as `-`: reporting the top real bucket edge there would be a
        number shaped like a measurement and less than one.
        """
        if self.count <= 0.0:
            return None
        rank = q * self.count
        previous_bound = 0.0
        previous_count = 0.0
        for bound, cumulative in self.buckets:
            if cumulative < rank:
                if cumulative > previous_count:
                    # Only a bucket that actually took observations moves the
                    # floor. Advancing the bound past an EMPTY bucket would
                    # interpolate inside a bucket the data never entered, and
                    # the fixed bucket list has a lot of those.
                    previous_bound, previous_count = bound, cumulative
                continue
            if bound == _INFINITY:
                return None
            if cumulative <= previous_count:
                # A zero-width bucket that still matched the rank: the
                # interpolation would divide by zero, and the bucket's own upper
                # bound is the only defensible answer.
                return bound
            return previous_bound + (bound - previous_bound) * (rank - previous_count) / (
                cumulative - previous_count
            )
        return None


def _histogram(families: dict[str, tuple[Sample, ...]], name: str) -> Histogram:
    """One histogram family, its buckets added up across every worker.

    A bound the scrape does not carry counts as zero rather than failing: the
    registry publishes every bucket of a histogram it publishes, so a missing
    one means no observation fell in it. A missing family entirely lands here
    with no buckets, and `quantile` answers `None` -- "nothing measured" is
    answerable, unlike "measured, unreadable".
    """
    edges: dict[float, float] = {}
    for sample in families.get(f"{name}_bucket", ()):
        bound = _as_bound(sample.labels.get("le", ""))
        if bound is None:
            continue
        edges[bound] = edges.get(bound, 0.0) + sample.value
    return Histogram(
        buckets=tuple(sorted(edges.items())),
        count=_total(families, f"{name}_count"),
        total=_total(families, f"{name}_sum"),
    )


def _as_bound(text: str) -> float | None:
    if text in ("+Inf", "Inf", "inf"):
        return _INFINITY
    try:
        return float(text)
    except ValueError:
        return None


# --- one scrape ---------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Every series this view reports, already summed over the `worker` label.

    `at` is a monotonic timestamp, never a wall clock: a rate divides by the
    difference between two of these, and a clock that steps backwards mid-demo
    (NTP, a laptop resuming) would make that division meaningless.
    """

    at: float
    accepted: float
    rejected: float
    rate_limited: float
    dlq: float
    #: Carried but not printed: the view's own claim is that this counter reads
    #: 0 under load, and a restart resets it, so a fall in it has to be able to
    #: void a rate like any other.
    ordering_violations: float
    responses: dict[str, float]
    in_flight: float
    buffer_items: float
    buffer_utilisation: float
    consumer_lag: float
    #: Every `worker` label value in the scrape, sorted. Kept rather than just
    #: counted because a change to this set is what makes two consecutive
    #: scrapes incomparable, and the caveat has to name them.
    worker_ids: tuple[str, ...]
    request_latency: Histogram

    @property
    def workers(self) -> int:
        return len(self.worker_ids)

    @classmethod
    def from_exposition(cls, text: str, at: float) -> Snapshot:
        families = parse_exposition(text)
        return cls(
            at=at,
            accepted=_total(families, EVENTS_ACCEPTED),
            rejected=_total(families, EVENTS_REJECTED),
            rate_limited=_total(families, RATE_LIMITED),
            dlq=_total(families, DLQ_PUBLISHED),
            ordering_violations=_total(families, ORDERING_VIOLATIONS),
            responses=_by_label(families, HTTP_RESPONSES, "status_class"),
            in_flight=_total(families, IN_FLIGHT),
            buffer_items=_total(families, BUFFER_ITEMS),
            # NOT a sum. `buffer_utilisation` is 0..1 per worker, so four of them
            # add to a number that is not a utilisation, and the one that
            # predicts 503s is the fullest worker. The sum of the gauges it is
            # derived from is still printed as `buffer`, because occupancy in
            # records is the other half of that question.
            buffer_utilisation=_max(families, BUFFER_UTILISATION),
            consumer_lag=_total(families, CONSUMER_LAG),
            worker_ids=_worker_ids(families),
            request_latency=_histogram(families, REQUEST_LATENCY),
        )


def _total(families: dict[str, tuple[Sample, ...]], name: str) -> float:
    """Every series of `name` added up. The whole point, for per-worker families."""
    return sum(sample.value for sample in families.get(name, ()))


def _max(families: dict[str, tuple[Sample, ...]], name: str) -> float:
    return max((sample.value for sample in families.get(name, ())), default=0.0)


def _by_label(families: dict[str, tuple[Sample, ...]], name: str, label: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for sample in families.get(name, ()):
        key = sample.labels.get(label, "")
        out[key] = out.get(key, 0.0) + sample.value
    return out


def _worker_ids(families: dict[str, tuple[Sample, ...]]) -> tuple[str, ...]:
    """Every `worker` label value in the scrape, sorted for a stable message."""
    return tuple(
        sorted(
            {
                sample.labels["worker"]
                for samples in families.values()
                for sample in samples
                if "worker" in sample.labels
            }
        )
    )


# --- two scrapes --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Reading:
    """The window between two scrapes. Every rate here is a delta over `elapsed`.

    A rate is `None` -- and a `caveat` is set -- whenever the two snapshots
    cannot support one. All three cases happen on demo day and none of them
    raises an exception, which is exactly why they are handled here rather than
    left to the arithmetic.
    """

    previous: Snapshot
    current: Snapshot

    @property
    def elapsed(self) -> float:
        return self.current.at - self.previous.at

    @property
    def fell(self) -> str | None:
        """The first counter that went backwards, or None.

        A counter that falls has been reset, not decremented: nothing in the
        gateway decrements one. The usual cause is a restarted worker.
        """
        for name in COUNTER_FIELDS:
            if getattr(self.current, name) < getattr(self.previous, name):
                return name
        for status_class in set(self.previous.responses) | set(self.current.responses):
            if self.current.responses.get(status_class, 0.0) < self.previous.responses.get(
                status_class, 0.0
            ):
                return f"{HTTP_RESPONSES}{{{status_class}}}"
        return None

    @property
    def workers_changed(self) -> bool:
        return self.previous.worker_ids != self.current.worker_ids

    @property
    def caveat(self) -> str | None:
        """Why no rate is printed, or None when the window supports one."""
        if self.elapsed <= 0.0:
            return "the two scrapes carry the same timestamp, so there is no interval"
        if self.workers_changed:
            return (
                f"the two scrapes were answered by different workers "
                f"({', '.join(self.previous.worker_ids)} -> {', '.join(self.current.worker_ids)}), "
                f"so their counters are not comparable"
            )
        fell = self.fell
        if fell is not None:
            return f"a counter reset ({fell} went backwards): a restarted worker, not a rate"
        return None

    @property
    def delta(self) -> float | None:
        """Counter change over the window, or None when there is no window."""
        if self.caveat is not None:
            return None
        return self.current.accepted - self.previous.accepted

    @property
    def events_per_sec(self) -> float | None:
        delta = self.delta
        return None if delta is None else delta / self.elapsed

    @property
    def _window(self) -> float | None:
        return None if self.caveat is not None else self.elapsed

    def _per_sec(self, name: str) -> float | None:
        window = self._window
        if window is None:
            return None
        return (getattr(self.current, name) - getattr(self.previous, name)) / window

    @property
    def rejected_per_sec(self) -> float | None:
        return self._per_sec("rejected")

    @property
    def dlq_per_sec(self) -> float | None:
        return self._per_sec("dlq")

    @property
    def rate_limited(self) -> float | None:
        return self._per_sec("rate_limited")

    @property
    def responses(self) -> float:
        """Requests on the gateway's own paths during the window."""
        return sum(self.current.responses.values()) - sum(self.previous.responses.values())

    @property
    def errors(self) -> float:
        return sum(
            self.current.responses.get(status_class, 0.0)
            - self.previous.responses.get(status_class, 0.0)
            for status_class in ERROR_CLASSES
        )

    @property
    def errors_per_sec(self) -> float | None:
        window = self._window
        return None if window is None else self.errors / window

    @property
    def error_rate(self) -> float | None:
        """Errors over requests in the window, or None when no request arrived.

        None and not 0.0: "no traffic" and "no errors" are different claims, and
        printing 0.0% for a window in which nothing was sent is the first one
        wearing the second one's clothes.
        """
        if self._window is None:
            return None
        responses = self.responses
        if responses <= 0.0:
            return None
        return self.errors / responses


# --- the two lines ------------------------------------------------------------


def _latency(seconds: float | None) -> str:
    return f"{seconds * 1_000:.1f}ms" if seconds is not None else "-"


def _rate(value: float | None) -> str:
    return f"{value:,.1f}/s" if value is not None else "-"


def baseline_line(snapshot: Snapshot, stamp: str) -> str:
    """The first scrape. Counters, explicitly not a rate.

    A rate needs two points. Printing `0.0 events/s` here, or dividing the total
    by the time since the process started, is the single most common way a live
    view starts making things up.
    """
    return (
        f"  {stamp}  baseline (first scrape: counters, not a rate)"
        f"  accepted={snapshot.accepted:,.0f}  rejected={snapshot.rejected:,.0f}"
        f"  dlq={snapshot.dlq:,.0f}  shed={snapshot.rate_limited:,.0f}"
        f"  workers {snapshot.workers}"
    )


def format_line(reading: Reading, stamp: str) -> str:
    """One screen line. Rates in the window, occupancy levels as they are."""
    current = reading.current
    parts = [f"  {stamp}"]
    caveat = reading.caveat
    if caveat is not None:
        parts.append(f"rate unknown -- {caveat}")
    else:
        rate = reading.events_per_sec
        assert rate is not None  # a window with no caveat always has a delta
        parts.append(f"{rate:,.1f} events/s")
        error_rate = reading.error_rate
        parts.append(
            f"{error_rate * 100:.1f}% err ({_rate(reading.errors_per_sec)})"
            if error_rate is not None
            else "err - (no requests)"
        )
        parts.append(f"{_rate(reading.rejected_per_sec)} rej")
        parts.append(f"{_rate(reading.dlq_per_sec)} dlq")
        parts.append(f"{_rate(reading.rate_limited)} shed")
    # Occupancy below here is a LEVEL, not a rate over the window: it reads the
    # same whatever the last scrape did, and a baseline-free number needs no
    # comparison to be true. The `|` separators keep the two kinds apart on a
    # line someone is reading at a glance, because `4 in flight` next to a rate
    # invites being read as part of it.
    parts.append(
        f"| {current.in_flight:,.0f} in flight"
        f"  buffer {current.buffer_utilisation * 100:.1f}% ({current.buffer_items:,.0f})"
        f"  lag {current.consumer_lag:,.0f}"
    )
    parts.append(
        f"p50 {_latency(current.request_latency.quantile(0.5))}"
        f" p99 {_latency(current.request_latency.quantile(0.99))}"
        f" | workers {current.workers}"
    )
    return "  ".join(parts)


# --- the scrape ---------------------------------------------------------------


def metrics_url(base_url: str) -> str:
    """`GATEWAY_URL` -> the metrics endpoint on it."""
    return base_url.rstrip("/") + METRICS_PATH


def scrape(url: str, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> str:
    """The exposition body, or `Unreachable`.

    `urllib` because this tool carries no dependencies: the gateway being
    observed is the thing that has to work on the demo machine, and a scrape that
    needs nothing installed is a scrape that cannot be the reason the demo stops.
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        # Before URLError, which it subclasses: a proxy answering 503 is a
        # different thing to say on screen than a refused connection.
        raise Unreachable(f"unreachable: {url} answered HTTP {exc.code}") from None
    except (urllib.error.URLError, http.client.HTTPException, OSError, ValueError) as exc:
        # `http.client.HTTPException` is the one that is easy to miss: a response
        # cut off mid-body -- a worker killed while rendering, a proxy timing
        # out -- raises `IncompleteRead`, which is NOT an `OSError` and NOT a
        # `URLError`, so without this clause the one failure a demo is most
        # likely to see is the one that prints a traceback.
        reason = getattr(exc, "reason", None) or exc
        raise Unreachable(f"unreachable: {url} -- {reason}") from None
    return body.decode("utf-8", "replace")


# --- the loop -----------------------------------------------------------------


class Observer:
    """Scrape, compare, print, repeat.

    Every collaborator is injected -- the scraper, the clock, the sleeper, the
    printer, the timestamp -- because a live view is the one program in this
    repository whose behaviour is *timing*, and a loop that can only be tested
    against a wall clock cannot be tested at all. The production wiring is the
    five arguments `main` passes.
    """

    def __init__(
        self,
        *,
        url: str,
        interval: float,
        scraper: Callable[[], str],
        printer: Callable[[str], None],
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        stamp: Callable[[], str] | None = None,
        samples: int | None = None,
    ) -> None:
        self.url = url
        self.interval = interval
        self.samples = samples
        self._scraper = scraper
        self._printer = printer
        self._clock = clock
        self._sleeper = sleeper
        self._stamp = stamp if stamp is not None else _wall_stamp

    def header(self) -> str:
        return (
            f"gateway  {self.url}  every {self.interval:.1f}s\n"
            f"  one number per line, summed over every `worker` label set in the scrape.\n"
            f"  a uvicorn scrape is answered by ONE worker, so on --workers 4 this is\n"
            f"  that worker's share, not the whole gateway. Ctrl-C to stop."
        )

    def run(self) -> int:
        """Print until `--samples` is reached or someone interrupts. 0 is success.

        A failed scrape prints one line and keeps the previous baseline: the
        counters did not restart just because the socket did, so dropping the
        baseline would silently understate the next window.
        """
        self._printer(self.header())
        previous: Snapshot | None = None
        taken = 0
        try:
            while self.samples is None or self.samples <= 0 or taken < self.samples:
                # Timed once per attempt, before the scrape, so a window that
                # spans a failed scrape spans the time that failed scrape took.
                at = self._clock()
                stamp = self._stamp()
                try:
                    snapshot = Snapshot.from_exposition(self._scraper(), at)
                except (Unreachable, ExpositionError) as exc:
                    self._printer(f"  {stamp}  {exc}")
                else:
                    self._printer(
                        baseline_line(snapshot, stamp)
                        if previous is None
                        else format_line(Reading(previous, snapshot), stamp)
                    )
                    previous = snapshot
                taken += 1
                if self.samples and taken >= self.samples:
                    break
                self._sleeper(self.interval)
        except KeyboardInterrupt:
            # How this program is meant to be stopped, so it is not a failure and
            # a trailing newline keeps the shell prompt off the last line.
            self._printer("")
            return 0
        return 0


def _wall_stamp() -> str:
    return time.strftime("%H:%M:%S")


# --- the CLI ------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.observe",
        description="Live events/sec and error rate from a gateway's /metrics, "
        "one line per scrape, summed over every worker in the scrape.",
    )
    parser.add_argument(
        "--url",
        default=metrics_url(os.getenv("GATEWAY_URL", DEFAULT_GATEWAY_URL)),
        help="metrics endpoint to scrape. Default: $GATEWAY_URL + /metrics, else "
        f"{DEFAULT_GATEWAY_URL}{METRICS_PATH}",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_INTERVAL_SECONDS,
        help="seconds between scrapes. Default: %(default)s",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=0,
        help="how many scrapes to take before exiting; 0 runs until interrupted. "
        "The first scrape is a baseline and reports no rate. Default: %(default)s",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="per-scrape timeout, seconds. Default: %(default)s",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.interval <= 0:
        parser.error(f"--interval must be positive, got {args.interval}")
    if args.timeout <= 0:
        parser.error(f"--timeout must be positive, got {args.timeout}")
    if args.samples < 0:
        parser.error(f"--samples must be zero (until interrupted) or positive, got {args.samples}")
    return Observer(
        url=args.url,
        interval=args.interval,
        samples=args.samples,
        scraper=lambda: scrape(args.url, args.timeout),
        printer=print,
        clock=time.monotonic,
        sleeper=time.sleep,
        stamp=_wall_stamp,
    ).run()
