"""T1r: the load client. `python -m bench.load`.

**This is not a benchmark, and the module says so before it says anything else.**
Q1 closed with "no": the demo does not demonstrate or assert 50,000 events/sec,
and plan r4.1 cancelled the T1 throughput benchmark outright in favour of "a
modest load run to show it holds". So this tool does one thing -- it replays a
corpus at a rate the operator chose, so that somebody watching a demo can see
the gateway holding that rate on a screen -- and it refuses to answer the
question it is not being asked. Nothing here is pushed until it breaks, the run
is short and single-threaded, and the receipt says which machine it came from.

## The three decisions that are not obvious

**The transport is `driver/replay.py`'s, and it is used rather than copied.**
`ReplayDriver` already does keep-alive, the 500-event / 4 MiB caps, one tenant
and one channel per request, a body encoded once so every retry carries the same
`id`s, bounded retries with `Retry-After`, and the ledger. Re-implementing any of
that here would be a second HTTP client with its own idea of the contract. So the
only new pieces are the two the replay driver does not have: **the pacing** and
**the receipt**.

**The pacing is a deadline schedule, not a sleep.** Request N is due at
`start + events_before_it / rate`. A pacer that slept a fixed gap between
requests would fall behind as soon as the gateway took longer than the gap and
would then run permanently slow, reporting a rate nobody asked for; a pacer that
let the debt accumulate would release it as a burst, and a burst is a rate nobody
asked for either. Both are reported here as what they are: an achieved rate below
the requested one (`achieved.held_requested_rate`).

**The latencies are client-side, and they are exact order statistics.**
`TimedClient` times each attempt from handing the request to the HTTP client
until the response is read, so the samples include the network and the driver's
own wait -- which is what a caller experiences. The gateway's own
`gateway_request_latency_seconds` (`app/metrics.py`) is the opposite: it excludes
the network, and it is a fixed-bucket histogram, so a percentile read out of it
is a bucket *bound*, not a value. Mixing the two without saying which is which is
how two runs end up being compared on two different scales, so `to_dict()` names
the difference in the block itself. Nothing here is interpolated: a percentile is
always a sample that was actually observed, which is the same discipline the
histogram keeps by storing counts rather than sums.

## The receipt is part of the claim

`machine_spec()` attaches the CPU model, the cores the process may actually use,
the total RAM, the OS, the Python build, whether it ran inside WSL2, whether it
ran inside a container, and the cgroup CPU limit -- because `docker-compose.yml`
gives the driver its own cores *precisely so the instrument is not sharing a core
with what it measures*, and a number without that is a number nobody can argue
with. Probes that fail say `unknown`; they never report a zero, because a zero
core count is a fabrication wearing a number.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import httpx

from driver.main import ConfiguredCatalog, tenants_from_env
from driver.replay import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_EVENTS_PER_BATCH,
    ReplayDriver,
    ReplayReport,
    TokenMinter,
    plan_batches,
)
from driver.skew import SkewedCorpus
from driver.tenants import DEFAULT_USERS_PER_TENANT, TenantCatalog

# --- what this tool is not ----------------------------------------------------

#: The first field of the receipt, and the module's first sentence. It is a
#: string in the JSON because the JSON is what gets pasted into a slide, and a
#: number that travels without its disclaimer is the failure this whole file
#: exists to prevent.
NOT_A_BENCHMARK = (
    "NOT A BENCHMARK. This tool replays a corpus at a rate the operator chose, "
    "so a live demo can show the gateway holding that rate. It does not find a "
    "limit: nothing is pushed until something breaks, the run is short and "
    "single-threaded, and no figure here says what the gateway could do on "
    "better hardware, over a longer run, or with a different number of workers."
)

#: Standing caveats, carried on every run. Each one is a way this receipt has
#: been misread before, in this project or in the conversation it is dropped
#: into; none of them is advice for the reader, they are descriptions of the
#: measurement.
CAVEATS = (
    "Every figure describes THIS run on THIS machine. Read the `machine` block "
    "with it -- a rate without the machine it was measured on is not a claim "
    "anybody can argue with.",
    "The requested rate is what this client imposed on itself. Comparing the "
    "two says the gateway kept up on this machine for these seconds. It is not a "
    "capacity figure, and it says nothing about what a longer run or a different "
    "number of workers would do.",
    "`elapsed_seconds` is wall clock for the whole run, so it includes the "
    "pacer's own sleeps, the Retry-After waits, event encoding and token "
    "minting -- everything the run spent, not only the gateway's share. A rate "
    "computed over the gateway's share alone would flatter the tool.",
    "p50/p99 are client-side: from handing the request to the HTTP client until "
    "the response was read, so they include the network, with one sample per "
    "ATTEMPT and retries included -- `samples` says how many there were. They "
    "are exact order statistics over the samples this run took, never "
    "interpolated between two of them.",
    "`gateway_request_latency_seconds` is not the same measurement: it is the "
    "gateway's own handler time, excluding the network, and it is a "
    "fixed-bucket histogram, so a percentile read out of it is a bucket BOUND "
    "rather than a value. Do not put the two side by side without saying which "
    "is which.",
    "If this ran inside WSL2 or a container, the CPU limit in `machine` is what "
    "kept the driver off the gateway's cores. docker-compose.yml sets those "
    "limits because a measuring instrument sharing a core with what it measures "
    "reports the scheduler; a run without them measured the scheduler.",
    "A shortfall is in `errors`, never hidden inside the rate. Batches that "
    "ended without a 202 are counted under the gateway's own reason code, and "
    "events no request ever accounted for are in `events_unaccepted`.",
)

# --- defaults -----------------------------------------------------------------

#: Where the gateway is when nothing says otherwise. `docker-compose.yml` sets
#: `GATEWAY_URL`, so the local run and the compose run differ in one variable.
DEFAULT_GATEWAY_URL = "http://localhost:8000"

#: The compose header writes the private key next to `.env`, which is the working
#: directory the driver service runs in.
DEFAULT_SIGNING_KEY = "driver-signing-key.pem"

#: The value `app.config.Settings.jwt_audience` falls back to, read at
#: parser-build time from both sides so a deployment that changes one changes
#: both -- a token signed for the wrong audience is a 401 on every request.
DEFAULT_AUDIENCE = "career-api"

#: Events per second the client tries to impose. Deliberately modest and
#: deliberately arbitrary: it is an operating point somebody picked for a demo,
#: not a target the gateway is measured against. At `DEFAULT_MAX_EVENTS_PER_BATCH`
#: this is one request roughly every 100 ms, which is a rate a laptop driver can
#: actually schedule -- a number high enough to be worth watching and low enough
#: that the achieved rate is about the gateway rather than about the pacer.
DEFAULT_RATE_EVENTS_PER_SEC = 2_000.0

#: Wall-clock budget for a run, seconds. Short on purpose, and stated in the
#: receipt, because a rate held for thirty seconds is not evidence about a rate
#: held for thirty minutes.
DEFAULT_DURATION_SECONDS = 30.0

#: This run's own ledger. NOT `ledger.jsonl`: that path is the demo's ground
#: truth, `tools/verify.py` reconciles against it, and a load run overwriting it
#: to make its own numbers look tidier would destroy the demo's receipt.
DEFAULT_LEDGER = Path("load-ledger.jsonl")

#: A load run pushes multi-MiB bodies, and httpx's 5-second default is not
#: obviously generous for one -- and it is not settable per request, so it is set
#: once, here, for the whole run.
DEFAULT_TIMEOUT_SECONDS = 30.0

#: How far the achieved rate may sit below the requested one and still count as
#: having held it. A run's first request is due immediately and the last one is
#: not, so a perfect run is already a few percent short of the requested figure;
#: without a tolerance the boolean would be an artefact of the schedule's own
#: rounding rather than a statement about the gateway.
RATE_TOLERANCE = 0.05


# --- the machine the number came from -----------------------------------------


def _read_text(path: str) -> str:
    """File contents, or `""` when it cannot be read. Never raises."""
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""


def _read_int(path: str) -> int | None:
    """First integer in a file, or `None`. Never raises."""
    text = _read_text(path).strip()
    try:
        return int(text)
    except ValueError:
        return None


def _windows_total_ram() -> int | None:
    """Total physical bytes via `GlobalMemoryStatusEx`, or `None`.

    `ctypes` rather than a subprocess, so probing the machine costs no process
    spawn and works on a demo laptop mid-presentation. The broad `except` is the
    point: a probe that raises must never be the reason a load run does not
    print its receipt.
    """
    if sys.platform != "win32":
        return None
    try:
        import ctypes

        class _Status(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = _Status()
        status.dwLength = ctypes.sizeof(_Status)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return None
        return int(status.ullTotalPhys)
    except Exception:
        return None


@dataclass(frozen=True, slots=True)
class MachineFacts:
    """The raw text and numbers a machine spec is derived from.

    Split from the derivation so the derivation is a pure function of its input,
    which is what lets a test read a WSL2-under-Docker cgroup and a bare Windows
    box from a machine that is neither. `""` and `None` mean *the probe could not
    read it*; they never mean zero.
    """

    platform: str = ""
    processor_identifier: str = ""
    wsl_distro: str = ""
    cpu_count: int | None = None
    affinity: int | None = None
    cpuinfo: str = ""
    meminfo: str = ""
    windows_ram_bytes: int | None = None
    cgroup_cpu_max: str = ""
    cgroup_v1_quota_us: int | None = None
    cgroup_v1_period_us: int | None = None
    cgroup_self: str = ""
    dockerenv: bool = False
    python_implementation: str = ""
    python_version: str = ""

    @classmethod
    def read(cls) -> MachineFacts:
        """This process's machine, as far as the standard library can see it."""
        return cls(
            platform=platform.platform(),
            # The one CPU source on Windows that is actually populated:
            # `platform.processor()` is routinely empty there, and it is a CPUID
            # string rather than a marketing name, so it is quoted verbatim.
            processor_identifier=os.environ.get("PROCESSOR_IDENTIFIER", ""),
            wsl_distro=os.environ.get("WSL_DISTRO_NAME", ""),
            cpu_count=os.cpu_count(),
            # The cores this process may actually be scheduled on, which is what
            # docker-compose's `cpus:` limit really bounds -- not the cores on
            # the box. Reporting the logical count alone would describe a
            # machine this run never touched.
            affinity=(
                len(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None
            ),
            cpuinfo=_read_text("/proc/cpuinfo"),
            meminfo=_read_text("/proc/meminfo"),
            windows_ram_bytes=_windows_total_ram(),
            cgroup_cpu_max=_read_text("/sys/fs/cgroup/cpu.max"),
            cgroup_v1_quota_us=_read_int("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"),
            cgroup_v1_period_us=_read_int("/sys/fs/cgroup/cpu/cpu.cfs_period_us"),
            cgroup_self=_read_text("/proc/self/cgroup"),
            dockerenv=Path("/.dockerenv").exists(),
            python_implementation=platform.python_implementation(),
            python_version=platform.python_version(),
        )


@dataclass(frozen=True, slots=True)
class MachineSpec:
    """What a reader needs in order to argue with a rate.

    Every field is either a value or an explicit "unknown". `cpu_limit_source`
    exists so `cpu_limit_cpus is None` is not ambiguous between *unlimited* and
    *could not be read*, which are different claims about a demo machine.
    """

    os: str
    cpu_model: str
    cpu_cores_logical: int | None
    cpu_cores_available: int | None
    ram_bytes: int | None
    python: str
    in_wsl: bool
    container_runtime: str
    cpu_limit_cpus: float | None
    cpu_limit_source: str

    @property
    def cpu_limit(self) -> str:
        """The limit as a sentence, because `null` in a receipt reads as a bug."""
        if self.cpu_limit_cpus is not None:
            return f"{self.cpu_limit_cpus:.2f} CPUs ({self.cpu_limit_source})"
        if self.cpu_limit_source == "unlimited":
            return "no CPU limit here (the cgroup quota is unlimited)"
        return "unknown (no cgroup CPU limit readable from this process)"

    def to_dict(self) -> dict:
        return {
            "os": self.os,
            "cpu_model": self.cpu_model,
            "cpu_cores_logical": self.cpu_cores_logical,
            "cpu_cores_available": self.cpu_cores_available,
            "ram_bytes": self.ram_bytes,
            "ram": _human_bytes(self.ram_bytes),
            "python": self.python,
            "in_wsl": self.in_wsl,
            "container_runtime": self.container_runtime,
            "cpu_limit_cpus": self.cpu_limit_cpus,
            "cpu_limit": self.cpu_limit,
        }


def machine_spec(facts: MachineFacts | None = None) -> MachineSpec:
    """The spec for `facts`, or for this machine when no facts are supplied."""
    facts = MachineFacts.read() if facts is None else facts
    return MachineSpec(
        os=facts.platform or "unknown",
        cpu_model=_cpu_model(facts),
        cpu_cores_logical=facts.cpu_count,
        cpu_cores_available=facts.affinity,
        ram_bytes=_total_ram(facts),
        python=(
            f"{facts.python_implementation} {facts.python_version}".strip() or "unknown"
        ),
        in_wsl=_in_wsl(facts),
        container_runtime=_container_runtime(facts),
        cpu_limit_cpus=_cpu_limit_cpus(facts),
        cpu_limit_source=_cpu_limit_source(facts),
    )


#: The `/proc/cpuinfo` keys that name a CPU. `processor` is deliberately absent:
#: on x86 that key is the *index*, and matching it would report the model of a
#: laptop as "0".
_CPU_MODEL_KEYS = frozenset({"model name", "hardware", "cpu model", "model"})

_CONTAINER_MARKERS = ("docker", "containerd", "kubepods", "podman", "libpod")


def _cpu_model(facts: MachineFacts) -> str:
    for line in facts.cpuinfo.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() in _CPU_MODEL_KEYS and value.strip():
            return value.strip()
    return facts.processor_identifier.strip() or (
        "unknown -- this machine reported no CPU model to the standard library"
    )


def _total_ram(facts: MachineFacts) -> int | None:
    for line in facts.meminfo.splitlines():
        if line.startswith("MemTotal:"):
            fields = line.split()
            if len(fields) >= 2 and fields[1].isdigit():
                # kB, and the k in MemTotal is 1024 bytes -- not 1000.
                return int(fields[1]) * 1024
    return facts.windows_ram_bytes


def _in_wsl(facts: MachineFacts) -> bool:
    haystack = f"{facts.platform} {facts.wsl_distro}".lower()
    return "microsoft" in haystack or "wsl" in haystack


def _container_runtime(facts: MachineFacts) -> str:
    if facts.dockerenv:
        return "docker"
    for marker in _CONTAINER_MARKERS:
        if marker in facts.cgroup_self.lower():
            return marker
    return "none"


def _cpu_limit_cpus(facts: MachineFacts) -> float | None:
    if facts.cgroup_cpu_max:
        fields = facts.cgroup_cpu_max.split()
        if len(fields) == 2 and fields[0] != "max":
            quota, period = float(fields[0]), float(fields[1])
            return quota / period if period > 0 else None
        return None
    quota, period = facts.cgroup_v1_quota_us, facts.cgroup_v1_period_us
    if quota is not None and period:
        # A negative v1 quota is how cgroup v1 spells "unlimited".
        return quota / period if quota > 0 else None
    return None


def _cpu_limit_source(facts: MachineFacts) -> str:
    if facts.cgroup_cpu_max:
        return "unlimited" if _cpu_limit_cpus(facts) is None else "cgroup v2 cpu.max"
    quota = facts.cgroup_v1_quota_us
    if quota is not None and facts.cgroup_v1_period_us:
        return "unlimited" if quota <= 0 else "cgroup v1 cpu.cfs_quota_us"
    return "unknown"


def _human_bytes(value: int | None) -> str:
    if value is None:
        return "unknown"
    for unit, size in (("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)):
        if value >= size:
            return f"{value / size:.1f} {unit}"
    return f"{value} B"


# --- the measurements ----------------------------------------------------------


@dataclass(slots=True)
class Latencies:
    """Every attempt's round-trip time, kept whole.

    No reservoir, no downsampling, no histogram: the percentiles below are exact
    order statistics over the attempts this run actually made, which is only a
    claim about this run and stops being one the moment the sample is thinned. A
    demo run is bounded by `--duration`, and the sample count is printed with the
    percentiles so nobody has to guess how much of the run they describe.
    """

    samples: list[float] = field(default_factory=list)

    def observe(self, seconds: float) -> None:
        self.samples.append(seconds)

    def __len__(self) -> int:
        return len(self.samples)

    def slowest_seconds(self) -> float | None:
        """The slowest attempt this run made, or `None` if there was none.

        Named `slowest` rather than `max` throughout, so the receipt carries no
        field beginning `max_`: a reader skimming a JSON block does not read the
        other fields' qualifiers, and `max_events_per_sec` beside `sent` reads as
        a capacity figure whichever one of them meant it as an observation.
        """
        return max(self.samples) if self.samples else None

    def percentile(self, quantile: float) -> float | None:
        """Nearest-rank: the smallest observed value at or above `quantile`.

        Nearest rank rather than an interpolating estimator because an
        interpolated p99 is a number no request produced. `app/metrics.py` keeps
        bucket counts for the same reason from the other direction -- a histogram
        cannot claim a value, only a bound. `None` for an empty sample rather
        than `0.0`, because zero is a latency somebody could believe.
        """
        if not 0.0 < quantile <= 1.0:
            raise ValueError(f"quantile must be in (0, 1], got {quantile!r}")
        if not self.samples:
            return None
        ordered = sorted(self.samples)
        rank = max(1, math.ceil(quantile * len(ordered)))
        return ordered[min(rank, len(ordered)) - 1]


class TimedClient:
    """A stopwatch wrapped around somebody else's HTTP client.

    Not an `httpx.Client` subclass and deliberately not a second transport: it
    delegates the one method `ReplayDriver` calls, so the connection pool, the
    timeout and the whole request pipeline stay the ones the caller already has.
    (`driver/test_replay.py` passes its `ScriptedClient` in the same way, so the
    duck type is the codebase's own arrangement and not a shortcut.)

    `send` covers the round trip and the network. It does not cover the driver's
    own encoding or its token minting, which happen before `post` is called; those
    are inside the wall-clock denominator and outside the samples, and the
    receipt says so rather than leaving a reader to assume otherwise.

    One sample per ATTEMPT, retries included. There is no way to tell from here
    which attempt was a retry -- the retry counter lives in `ReplayReport` -- so
    the honest thing is to sample every attempt and print `retried` beside the
    percentiles.
    """

    __slots__ = ("_client", "_clock", "_latencies")

    def __init__(
        self,
        client: httpx.Client,
        latencies: Latencies,
        *,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._client = client
        self._latencies = latencies
        self._clock = clock

    def post(self, url: str, *, content: bytes, headers: Mapping[str, str]) -> httpx.Response:
        started = self._clock()
        try:
            return self._client.post(url, content=content, headers=dict(headers))
        finally:
            self._latencies.observe(self._clock() - started)


# --- the pacing ---------------------------------------------------------------


def _one_request_each(
    batches: Iterable[Sequence[dict]], *, max_events: int, max_bytes: int
) -> Iterable[list[dict]]:
    """Re-emit each corpus batch as the exact requests it becomes.

    The schedule counts events per REQUEST, so the pacer has to know how many
    requests the batcher will cut -- and `plan_batches` cuts on the channel
    change as well as on the caps, so a corpus batch of 200 events can be two
    requests. Handing the raw batch to the pacer and letting `ReplayDriver` split
    it afterwards would mean scheduling 200 events at one request's deadline and
    sending two, i.e. asking for a rate and imposing a different one.

    Planning here and letting `ReplayDriver.run` plan again is a double encode,
    paid knowingly: the re-plan is deterministic, and every yielded batch is
    single-channel and within the caps, so it comes back as exactly one
    `PlannedBatch` of the same size with the same bytes. `bench/test_load.py`
    asserts that round trip rather than assuming it.
    """
    for batch in batches:
        for planned in plan_batches(batch, max_events=max_events, max_bytes=max_bytes):
            yield list(planned.events)


def paced_requests(
    batches: Iterable[Sequence[dict]],
    *,
    rate_events_per_sec: float,
    duration_seconds: float | None,
    max_events: int = DEFAULT_MAX_EVENTS_PER_BATCH,
    max_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterable[list[dict]]:
    """The batches `ReplayDriver` will send, spaced to hold `rate_events_per_sec`.

    Request N is due at `start + events_before_it / rate`, so the sleeps between
    requests sum to that offset exactly. When a deadline has already passed the
    request goes out immediately and NO sleep is recorded: the alternative is
    either a permanent slowdown (a fixed gap) or a catch-up burst (an accumulated
    debt), and both report a rate the operator never asked for. A gateway slower
    than the schedule therefore shows up as an achieved rate below the requested
    one, which is the fact the receipt is for.

    `duration_seconds` is a wall-clock budget on the SCHEDULE, not on the work:
    the last request is the first one due at or after the budget, so a one-second
    run at 1000 events/sec in 50-event requests sends 1000 events and stops. The
    corpus ending first also ends the run, and the receipt reports which elapsed
    time it actually got.
    """
    if rate_events_per_sec <= 0:
        raise ValueError(
            f"rate_events_per_sec must be a positive rate, got {rate_events_per_sec!r}: "
            "this tool imposes a rate, and zero of them is not a run"
        )
    if duration_seconds is not None and duration_seconds < 0:
        raise ValueError(
            f"duration_seconds must leave room for a run, got {duration_seconds!r}; pass "
            "None for no budget"
        )
    start = clock()
    scheduled = 0
    for request in _one_request_each(batches, max_events=max_events, max_bytes=max_bytes):
        due = start + scheduled / rate_events_per_sec
        if duration_seconds is not None and due - start >= duration_seconds:
            return
        remaining = due - clock()
        if remaining > 0:
            sleep(remaining)
        yield request
        scheduled += len(request)


# --- the receipt --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LoadReport:
    """`ReplayReport` plus what only a paced run knows: the rate it was asked to
    hold, what it held, how long it ran, and the machine it ran on.

    Held as a value rather than assembled at print time because the CLI's exit
    code and its JSON have to be the same facts, and a receipt computed twice is
    two receipts.
    """

    url: str
    ledger: Path
    requested_events_per_sec: float
    requested_duration_seconds: float | None
    elapsed_seconds: float
    max_events_per_request: int
    max_bytes_per_request: int
    samples: int
    p50_seconds: float | None
    p99_seconds: float | None
    slowest_seconds: float | None
    replay: ReplayReport
    machine: MachineSpec

    @property
    def achieved_events_per_sec(self) -> float:
        """Events the driver sent, over this run's wall clock.

        `sent`, not `accepted`: it is the number of events the client put on the
        wire, and dividing by elapsed time answers "did the client hold the rate",
        which is the question. `accepted` has its own figure beside it.
        """
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.replay.sent / self.elapsed_seconds

    @property
    def achieved_accepted_events_per_sec(self) -> float:
        if self.elapsed_seconds <= 0:
            return 0.0
        return self.replay.accepted / self.elapsed_seconds

    @property
    def held_requested_rate(self) -> bool | None:
        """Whether the gateway kept up with the rate THIS run asked for.

        `None` when the run sent nothing, because a boolean computed from no
        samples is a green light for a run that did not happen.
        """
        if self.replay.sent <= 0 or self.requested_events_per_sec <= 0:
            return None
        return self.achieved_events_per_sec >= self.requested_events_per_sec * (
            1 - RATE_TOLERANCE
        )

    def to_dict(self) -> dict:
        """The whole receipt. The disclaimer is the first key on purpose."""
        return {
            "not_a_benchmark": NOT_A_BENCHMARK,
            "caveats": list(CAVEATS),
            "run": {
                "url": self.url,
                "ledger": str(self.ledger),
                "requested_events_per_sec": self.requested_events_per_sec,
                "requested_duration_seconds": self.requested_duration_seconds,
                "elapsed_seconds": round(self.elapsed_seconds, 6),
                "events_per_request": self.max_events_per_request,
                "bytes_per_request": self.max_bytes_per_request,
            },
            "achieved": {
                "events_sent_per_sec": round(self.achieved_events_per_sec, 3),
                "events_accepted_per_sec": round(self.achieved_accepted_events_per_sec, 3),
                "held_requested_rate": self.held_requested_rate,
                "rate_tolerance": RATE_TOLERANCE,
                "note": "what THIS run did on THIS machine; not a capacity figure",
            },
            "latency_client_side_seconds": {
                "where": "client",
                "measures": "HTTP attempt, from the request being handed to the "
                "client to the response being read; one sample per attempt, "
                "retries included",
                "not_the_same_as": (
                    "gateway_request_latency_seconds, which is the gateway's own "
                    "handler time without the network and is bucketed, so a "
                    "percentile from it is a bucket bound rather than a value"
                ),
                "samples": self.samples,
                "p50_seconds": self.p50_seconds,
                "p99_seconds": self.p99_seconds,
                "slowest_seconds": self.slowest_seconds,
                "method": "nearest rank over the samples; never interpolated",
            },
            "errors": {
                "batches_by_status": dict(sorted(self.replay.by_status.items())),
                "rate_limited_batches": self.replay.rate_limited,
                "sink_unavailable_batches": self.replay.sink_unavailable,
                "retried_attempts": self.replay.retried,
                "events_rejected_by_validation": self.replay.rejected,
                "events_unaccepted": self.replay.unaccepted,
            },
            "ledger": {
                "batches": self.replay.batches,
                "sent": self.replay.sent,
                "accepted": self.replay.accepted,
                "clean": self.replay.clean,
            },
            "machine": self.machine.to_dict(),
        }


# --- the run ------------------------------------------------------------------


def run_load(
    *,
    url: str,
    signer: TokenMinter,
    client: httpx.Client,
    batches: Iterable[Sequence[dict]],
    ledger_path: Path,
    rate_events_per_sec: float,
    duration_seconds: float | None,
    max_events: int = DEFAULT_MAX_EVENTS_PER_BATCH,
    max_bytes: int = DEFAULT_MAX_BATCH_BYTES,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    machine: MachineSpec | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> LoadReport:
    """Replay `batches` at `rate_events_per_sec` and return the receipt.

    `client` is handed in rather than built here for the same reason
    `ReplayDriver` takes one: keep-alive is the pool's, and a client per request
    would make the receipt a measurement of the network. It is wrapped in a
    `TimedClient` on the way through, and that wrapper is the whole of what this
    module adds to the transport.

    `clock` and `sleep` are injected for both the pacer and the replay driver's
    retry waits, so one clock describes the run and a test can drive the whole
    thing without waiting for wall clock to pass. `elapsed_seconds` is measured
    around `ReplayDriver.run`, which is the run's own wall clock: pacing sleeps,
    `Retry-After` waits, encoding and token minting all inside it.

    The `max_events`/`max_bytes` given here are the SAME values the pacer is
    given, and they have to be: the schedule counts events per request, and it
    only knows which requests those are because it planned them under these caps.
    """
    latencies = Latencies()
    started = clock()
    replay = ReplayDriver(
        url=url,
        signer=signer,
        client=TimedClient(client, latencies),
        ledger_path=ledger_path,
        max_attempts=max_attempts,
        max_events=max_events,
        max_bytes=max_bytes,
        sleep=sleep,
    ).run(
        paced_requests(
            batches,
            rate_events_per_sec=rate_events_per_sec,
            duration_seconds=duration_seconds,
            max_events=max_events,
            max_bytes=max_bytes,
            clock=clock,
            sleep=sleep,
        )
    )
    return LoadReport(
        url=url,
        ledger=Path(ledger_path),
        requested_events_per_sec=rate_events_per_sec,
        requested_duration_seconds=duration_seconds,
        elapsed_seconds=clock() - started,
        max_events_per_request=max_events,
        max_bytes_per_request=max_bytes,
        samples=len(latencies),
        p50_seconds=latencies.percentile(0.5),
        p99_seconds=latencies.percentile(0.99),
        slowest_seconds=latencies.slowest_seconds(),
        replay=replay,
        machine=machine_spec() if machine is None else machine,
    )


# --- the CLI ------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m bench.load",
        description="Replay a corpus into the gateway at a rate you choose, and "
        "report what this run did on this machine. NOT a benchmark: it does not "
        "find a limit, it holds the rate it was given, and every figure it prints "
        "comes attached to the machine spec and the standing caveats.",
    )
    parser.add_argument(
        "--url",
        default=os.getenv("GATEWAY_URL", DEFAULT_GATEWAY_URL),
        help="gateway base URL. Default: $GATEWAY_URL, else " + DEFAULT_GATEWAY_URL,
    )
    parser.add_argument(
        "--signing-key",
        type=Path,
        default=Path(os.getenv("DRIVER_SIGNING_KEY", DEFAULT_SIGNING_KEY)),
        help="PEM holding the PRIVATE signing key. Default: $DRIVER_SIGNING_KEY, "
        f"else ./{DEFAULT_SIGNING_KEY}. The gateway must never be able to read this.",
    )
    parser.add_argument(
        "--audience",
        default=os.getenv("JWT_AUD", DEFAULT_AUDIENCE),
        help=f"token audience; must match the gateway's JWT_AUD (default: {DEFAULT_AUDIENCE})",
    )
    parser.add_argument(
        "--tenants",
        type=int,
        default=3,
        help="tenants in the catalog. Ignored when GATEWAY_TENANTS is set, because "
        "the driver has to name tenants the gateway has registered.",
    )
    parser.add_argument(
        "--users-per-tenant", type=int, default=DEFAULT_USERS_PER_TENANT,
        help="users per tenant in the corpus. Default: "
        f"{DEFAULT_USERS_PER_TENANT}, the driver's own default.",
    )
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_RATE_EVENTS_PER_SEC,
        metavar="EVENTS_PER_SEC",
        help="events per second this client tries to impose. An operating point "
        "for a demo, NOT a target the gateway is measured against and not a "
        f"benchmark: default {DEFAULT_RATE_EVENTS_PER_SEC:.0f}.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=DEFAULT_DURATION_SECONDS,
        metavar="SECONDS",
        help="wall-clock budget for the run. Short on purpose: a rate held for "
        f"{DEFAULT_DURATION_SECONDS:.0f}s is not evidence about a longer one. "
        "The corpus is the other bound -- whichever runs out first ends the run.",
    )
    parser.add_argument(
        "--sessions", type=int, default=200_000, help="corpus size, in sessions"
    )
    parser.add_argument("--max-events", type=int, default=DEFAULT_MAX_EVENTS_PER_BATCH)
    parser.add_argument("--max-batch-bytes", type=int, default=DEFAULT_MAX_BATCH_BYTES)
    parser.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=DEFAULT_LEDGER,
        help="this run's own ledger. Default: ./"
        f"{DEFAULT_LEDGER}. Deliberately not ledger.jsonl, which is the demo's "
        "ground truth for tools/verify.py.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.rate <= 0:
        parser.error(
            "--rate must be a positive number of events per second: this tool holds "
            "a rate, and a rate of zero is not a run"
        )
    if args.duration <= 0:
        parser.error("--duration must be positive")
    if args.sessions < 0:
        parser.error("--sessions must be non-negative")
    if args.tenants < 1:
        parser.error("--tenants must be positive")
    if args.max_events < 1:
        parser.error("--max-events must be positive")
    if args.max_batch_bytes < 1:
        parser.error("--max-batch-bytes must be positive")
    if args.max_attempts < 1:
        parser.error("--max-attempts must be positive")
    if args.timeout <= 0:
        parser.error("--timeout must be positive")

    key_path = Path(args.signing_key)
    if not key_path.is_file():
        parser.error(
            f"signing key {key_path} not found. The driver mints the bearer tokens "
            "and so needs the PRIVATE half of the pair; the header of "
            "docker-compose.yml writes it to driver-signing-key.pem beside .env"
        )

    configured = tenants_from_env()
    catalog = (
        ConfiguredCatalog(configured, seed=args.seed, users_per_tenant=args.users_per_tenant)
        if configured
        else TenantCatalog(args.tenants, seed=args.seed, users_per_tenant=args.users_per_tenant)
    )
    corpus = SkewedCorpus(
        catalog=catalog, seed=args.seed, users_per_tenant=args.users_per_tenant
    ).batches(args.sessions, max_events=args.max_events)

    signer = TokenMinter(key_path.read_bytes(), audience=args.audience)
    client = httpx.Client(timeout=args.timeout)
    try:
        report = run_load(
            url=args.url,
            signer=signer,
            client=client,
            batches=corpus,
            ledger_path=args.ledger,
            rate_events_per_sec=args.rate,
            duration_seconds=args.duration,
            max_events=args.max_events,
            max_bytes=args.max_batch_bytes,
            max_attempts=args.max_attempts,
        )
    finally:
        client.close()

    print(json.dumps(report.to_dict(), indent=2))
    # Same checkpoint as the demo's driver, and for the same reason: a run that
    # cannot make the claim has to say so in the exit code as well as the JSON,
    # because a refused batch means this run's reconciliation will not balance.
    return 0 if report.replay.clean else 1


__all__ = [
    "CAVEATS",
    "DEFAULT_AUDIENCE",
    "DEFAULT_DURATION_SECONDS",
    "DEFAULT_GATEWAY_URL",
    "DEFAULT_LEDGER",
    "DEFAULT_MAX_BATCH_BYTES",
    "DEFAULT_MAX_EVENTS_PER_BATCH",
    "DEFAULT_RATE_EVENTS_PER_SEC",
    "DEFAULT_SIGNING_KEY",
    "DEFAULT_TIMEOUT_SECONDS",
    "NOT_A_BENCHMARK",
    "RATE_TOLERANCE",
    "Latencies",
    "LoadReport",
    "MachineFacts",
    "MachineSpec",
    "TimedClient",
    "build_parser",
    "machine_spec",
    "main",
    "paced_requests",
    "run_load",
]


if __name__ == "__main__":
    raise SystemExit(main())