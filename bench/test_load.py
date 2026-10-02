"""T1r: the load client. RED before implementation.

**What is being defended here.** Q1 closed with "no": the demo does not
demonstrate or assert 50,000 events/sec, and plan r4.1 cancelled the T1
throughput benchmark outright. So this file does not test that a number is big.
It tests four things that are checkable without a broker and without any
capacity claim, and the first of them is a constraint rather than a behaviour:

* **The output never reads like a capacity claim.** The summary's first field
  says what the tool is not, and no field is named the way a headline number is
  named. A tool that prints "supports 12,000 events/sec" has already made the
  claim the project refused to make, whatever the number came from.
* **The machine travels with the number.** Every run attaches the CPU model, the
  core count, the RAM, the OS, the Python build, whether it was inside WSL2 and a
  container, and the CPU limit. A rate without the machine it was measured on is
  not a claim anybody can argue with, which is the problem.
* **The rate the operator asked for is the rate that is scheduled, and the
  achieved rate is measured from the report, not asserted.** The pacer spaces
  requests on absolute deadlines, so a gateway slower than the schedule shows up
  as an achieved rate below the requested one instead of as a burst of catch-up
  traffic.
* **The arithmetic is right without a gateway.** Every test below runs against a
  fake clock, the real `ReplayDriver`, and -- where a server is needed at all --
  the real FastAPI app with `FakeSink` or a loopback socket. Nothing here needs a
  broker, and nothing here prints a measured rate.
"""

from __future__ import annotations

import http.server
import json
import threading
from pathlib import Path

import msgspec
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

from app.config import Settings
from app.kafka.producer import FakeSink
from app.main import create_app, credentials_from_env
from app.metrics import Metrics
from app.ratelimit.registry import TenantLimits
from bench.load import (
    CAVEATS,
    DEFAULT_AUDIENCE,
    DEFAULT_LEDGER,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_EVENTS_PER_BATCH,
    NOT_A_BENCHMARK,
    RATE_TOLERANCE,
    Latencies,
    LoadReport,
    MachineFacts,
    MachineSpec,
    build_parser,
    machine_spec,
    main,
    paced_requests,
    run_load,
)
from contracts.ledger import Ledger
from driver.corpus import CorpusBuilder
from driver.replay import ReplayReport, TokenMinter, plan_batches

TENANTS = [f"tenant_{i:04d}" for i in range(1, 4)]

AUDIENCE = DEFAULT_AUDIENCE

#: Matches `app/test_app.py`; the load run never reaches the decrypt path, which
#: is the only consumer of the master secret and the operator key.
DEV_MASTER = b"test-only-master-secret-do-not-use!"
OPERATOR_KEY = b"test-only-operator-key"


# --- helpers ------------------------------------------------------------------


class FakeClock:
    """A monotonic clock the test owns, and the sleeps the pacer asked for.

    Injected rather than slept against, because a test that verifies pacing by
    actually pacing takes as long as the rate is slow -- and the point of the
    injected clock is that the ARITHMETIC is checked, not the scheduler.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        assert seconds >= 0.0, f"the pacer asked to sleep backwards by {seconds!r}s"
        self.sleeps.append(seconds)
        self.now += seconds

    @property
    def slept(self) -> float:
        return sum(self.sleeps)


class DriftingClock(FakeClock):
    """A clock that advances on every READ, standing in for work that took time.

    This is what a gateway slower than the requested rate looks like to a pacer:
    the clock moves on between the deadline being computed and the request going
    out, so the deadline is already in the past when the pacer looks at it.
    """

    def __init__(self, per_read: float) -> None:
        super().__init__()
        self._per_read = per_read

    def __call__(self) -> float:
        self.now += self._per_read
        return self.now


def _corpus_batches(sessions: int = 120, max_events: int = 200) -> list[list[dict]]:
    return list(
        CorpusBuilder(career_site_ids=TENANTS, seed=7, users_per_tenant=10).batches(
            sessions, max_events=max_events
        )
    )


@pytest.fixture(scope="module")
def keypair() -> tuple[bytes, str]:
    key = Ed25519PrivateKey.generate()
    private = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    public = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return private, public.decode()


@pytest.fixture
def minter(keypair) -> TokenMinter:
    return TokenMinter(keypair[0], audience=AUDIENCE)


def _build_gateway(public_pem: str, **kwargs):
    return create_app(
        credentials=credentials_from_env({"GATEWAY_TENANTS": ",".join(TENANTS)}),
        settings=Settings(
            master_secret=DEV_MASTER,
            jwt_public_key_pem=public_pem,
            jwt_algorithm="EdDSA",
            jwt_audience=AUDIENCE,
        ),
        operator_key=OPERATOR_KEY,
        sink=FakeSink(),
        metrics=Metrics(worker_id="test-worker"),
        **kwargs,
    )


@pytest.fixture
def gateway(keypair):
    return _build_gateway(keypair[1])


def _report(**over) -> LoadReport:
    """A `LoadReport` built from explicit numbers, so every assertion below is
    an arithmetic claim about the tool rather than a measurement of anything."""
    fields = dict(
        url="http://gateway:8000",
        ledger=Path("load-ledger.jsonl"),
        requested_events_per_sec=1000.0,
        requested_duration_seconds=1.0,
        elapsed_seconds=1.0,
        max_events_per_request=DEFAULT_MAX_EVENTS_PER_BATCH,
        max_bytes_per_request=DEFAULT_MAX_BATCH_BYTES,
        samples=1,
        p50_seconds=0.01,
        p99_seconds=0.05,
        slowest_seconds=0.2,
        replay=ReplayReport(batches=10, sent=1000, accepted=1000),
        machine=machine_spec(MachineFacts(platform="test", python_version="3.11.0")),
    )
    fields.update(over)
    return LoadReport(**fields)


def _keys(node, prefix: str = "") -> set[str]:
    """Every dotted key in a nested dict, for the naming assertions."""
    out = {prefix}
    for key, value in node.items():
        out |= _keys(value, f"{prefix}.{key}" if prefix else key) if isinstance(
            value, dict
        ) else {f"{prefix}.{key}" if prefix else key}
    return out


# =============================================================================
# 1. the schedule
# =============================================================================


def test_the_pacer_spaces_requests_so_the_run_lands_the_rate_it_was_asked_for():
    """Request N is due at `start + events_before_it / rate`, so the sleeps
    between requests have to sum to exactly that offset. Anything else and the
    number printed at the end is not the number that was scheduled."""
    batches = _corpus_batches(120, max_events=50)
    clock = FakeClock()
    requests = list(
        paced_requests(
            iter(batches),
            rate_events_per_sec=1000.0,
            duration_seconds=None,
            max_events=50,
            clock=clock,
            sleep=clock.sleep,
        )
    )
    assert len(requests) > 3, "not enough requests for the spacing to mean anything"
    sent = sum(len(request) for request in requests)
    assert clock.slept == pytest.approx((sent - len(requests[-1])) / 1000.0, abs=1e-9)
    assert clock.now == pytest.approx(clock.slept)


def test_a_gateway_slower_than_the_schedule_gets_no_catch_up_burst_and_no_backwards_sleep():
    """When the deadline has already passed the pacer sends immediately. What it
    must NOT do is sleep a negative interval, or accumulate a backlog and then
    release it as a burst -- a burst reads as a rate nobody asked for, and the
    latency samples that come with it are the pacer's own debt."""
    batches = _corpus_batches(120, max_events=50)
    before = sum(len(batch) for batch in batches)
    clock = DriftingClock(per_read=0.2)
    requests = list(
        paced_requests(
            iter(batches),
            rate_events_per_sec=1000.0,
            duration_seconds=None,
            max_events=50,
            clock=clock,
            sleep=clock.sleep,
        )
    )
    assert sum(len(request) for request in requests) == before, "the pacer dropped work when it ran late"
    assert clock.sleeps == []


def test_the_pacer_stops_at_the_duration_budget():
    """The budget is wall clock, so at 1000 events/sec for one second the last
    request is scheduled at 0.95s and the 1.0s one is never sent. Anything else
    overshoots the run the operator asked for."""
    clock = FakeClock()
    requests = list(
        paced_requests(
            iter(_corpus_batches(600, max_events=50)),
            rate_events_per_sec=1000.0,
            duration_seconds=1.0,
            max_events=50,
            clock=clock,
            sleep=clock.sleep,
        )
    )
    sent = sum(len(request) for request in requests)
    assert sent == 1000, "one second at 1000 events/sec, not the budget overshot"
    # The last request is due at `start + (sent - its own events)/rate`, which is
    # not 0.95s: the batcher cuts on the channel change, so requests are not all
    # 50 events and the schedule is not a neat multiple of one batch.
    assert clock.now == pytest.approx((sent - len(requests[-1])) / 1000.0, abs=1e-9)
    assert clock.now <= 1.0


def test_every_batch_the_pacer_yields_is_exactly_one_request_the_replay_driver_will_send():
    """The schedule counts EVENTS PER REQUEST, so a yielded batch that
    `plan_batches` splits a second time would make the requested rate a fiction:
    the pacer would believe it sent N events in one request while the gateway
    saw two requests. The re-plan in `ReplayDriver.run` is deterministic, so the
    proof is that re-planning a yielded batch gives back one batch of the same
    size."""
    requests = list(
        paced_requests(
            iter(_corpus_batches(120)),
            rate_events_per_sec=1000.0,
            duration_seconds=None,
        )
    )
    assert len(requests) > 3
    for request in requests:
        assert len({event["sourcechannel"] for event in request}) == 1
        assert len(request) <= DEFAULT_MAX_EVENTS_PER_BATCH
        replanned = list(
            plan_batches(
                request,
                max_events=DEFAULT_MAX_EVENTS_PER_BATCH,
                max_bytes=DEFAULT_MAX_BATCH_BYTES,
            )
        )
        assert len(replanned) == 1, "the replay driver will send more requests than were scheduled"
        assert replanned[0].size == len(request)


def test_a_rate_that_is_not_a_rate_is_refused_rather_than_reinterpreted():
    with pytest.raises(ValueError, match="rate"):
        list(
            paced_requests(
                iter(_corpus_batches(20)),
                rate_events_per_sec=0.0,
                duration_seconds=1.0,
            )
        )
    with pytest.raises(ValueError, match="rate"):
        list(
            paced_requests(
                iter(_corpus_batches(20)),
                rate_events_per_sec=-5.0,
                duration_seconds=1.0,
            )
        )


def test_a_negative_budget_is_refused_rather_than_read_as_unbounded():
    with pytest.raises(ValueError, match="duration"):
        list(
            paced_requests(
                iter(_corpus_batches(20)),
                rate_events_per_sec=100.0,
                duration_seconds=-1.0,
            )
        )


# =============================================================================
# 2. the percentiles
# =============================================================================


def test_the_percentiles_are_exact_order_statistics_over_the_samples_taken():
    latencies = Latencies([0.10, 0.01, 0.05, 0.99] * 25)
    assert len(latencies) == 100
    assert latencies.percentile(0.5) == 0.05
    assert latencies.percentile(0.99) == 0.99
    assert latencies.percentile(1.0) == 0.99


def test_a_reported_percentile_is_a_value_that_was_actually_observed():
    """No interpolation. `app/metrics.py` keeps bucket counts precisely so the
    exposition never claims more precision than a bucket holds; interpolating
    between two client-side samples would invent a number no request produced,
    which is the same failure one level up."""
    latencies = Latencies()
    for value in (0.011, 0.019, 0.023, 0.037):
        latencies.observe(value)
    for quantile in (0.5, 0.9, 0.99, 1.0):
        assert latencies.percentile(quantile) in latencies.samples


def test_a_run_that_measured_nothing_reports_no_percentile_rather_than_zero():
    """Zero is a latency a reader could believe, and a run that sent nothing has
    measured nothing."""
    latencies = Latencies()
    assert latencies.percentile(0.5) is None
    assert latencies.percentile(0.99) is None
    assert latencies.slowest_seconds() is None


def test_a_quantile_outside_zero_to_one_is_refused():
    latencies = Latencies([0.1])
    with pytest.raises(ValueError, match="quantile"):
        latencies.percentile(0.0)
    with pytest.raises(ValueError, match="quantile"):
        latencies.percentile(1.5)


# =============================================================================
# 3. the machine spec
# =============================================================================

#: Trimmed from what `/proc/cpuinfo` reports under WSL2, where the model name is
#: the HOST's -- which is exactly why the caveat about a shared core matters.
WSL_CPUINFO = """processor\t: 0
vendor_id\t: GenuineIntel
model name\t: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
cpu MHz\t\t: 2592.000
"""

MEMINFO = """MemTotal:       16693224 kB
MemFree:         1234567 kB
"""


def test_the_spec_names_the_cpu_the_cores_and_the_ram_the_run_happened_on():
    spec = machine_spec(
        MachineFacts(
            platform="Linux-5.15.90.1-microsoft-standard-WSL2-amd64",
            cpuinfo=WSL_CPUINFO,
            meminfo=MEMINFO,
            cpu_count=12,
            affinity=6,
            python_implementation="CPython",
            python_version="3.11.9",
        )
    )
    assert spec.cpu_model == "Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz"
    assert spec.cpu_cores_logical == 12
    assert spec.cpu_cores_available == 6, "cores the process may actually use, not cores on the box"
    assert spec.ram_bytes == 16693224 * 1024
    assert spec.in_wsl is True
    assert spec.python == "CPython 3.11.9"


def test_the_spec_reports_the_cgroup_cpu_limit_the_demo_actually_runs_under():
    """`docker-compose.yml` gives the driver `cpus: "2.0"` and says in as many
    words that the instrument must not share a core with what it measures. A run
    whose receipt cannot name that limit is a run that cannot be believed to have
    been given it."""
    spec = machine_spec(
        MachineFacts(
            dockerenv=True,
            cgroup_self="0::/docker/1f3a2b9c7d",
            cgroup_cpu_max="200000 100000",
            affinity=2,
        )
    )
    assert spec.container_runtime == "docker"
    assert spec.cpu_limit_cpus == 2.0
    assert spec.cpu_limit_source == "cgroup v2 cpu.max"
    assert "2.00 CPUs" in spec.cpu_limit


def test_a_cgroup_v1_limit_reads_too_and_an_unlimited_quota_is_not_a_number():
    v1 = machine_spec(MachineFacts(cgroup_v1_quota_us=300_000, cgroup_v1_period_us=100_000))
    assert v1.cpu_limit_cpus == 3.0
    assert v1.cpu_limit_source == "cgroup v1 cpu.cfs_quota_us"

    unlimited = machine_spec(MachineFacts(cgroup_cpu_max="max 100000"))
    assert unlimited.cpu_limit_cpus is None
    assert unlimited.cpu_limit_source == "unlimited"

    absent = machine_spec(MachineFacts())
    assert absent.cpu_limit_cpus is None
    assert absent.cpu_limit_source == "unknown"


def test_a_machine_that_will_not_say_what_it_is_reports_unknown_rather_than_guessing():
    """Every probe failed. The honest report is 'unknown' on each field; a zero
    core count or a 0-byte machine would be a fabrication with a number in it."""
    spec = machine_spec(MachineFacts())
    assert spec.cpu_model.startswith("unknown")
    assert spec.cpu_cores_logical is None
    assert spec.cpu_cores_available is None
    assert spec.ram_bytes is None
    assert spec.in_wsl is False
    assert spec.container_runtime == "none"
    assert spec.cpu_limit_cpus is None


def test_the_windows_processor_identifier_is_the_fallback_cpu_model():
    """On Windows `/proc/cpuinfo` does not exist and `platform.processor()` is
    routinely empty; `PROCESSOR_IDENTIFIER` is the one source that is actually
    populated, and it is a CPUID string rather than a marketing name -- which is
    why it is quoted verbatim instead of prettied up."""
    spec = machine_spec(
        MachineFacts(platform="Windows-11-10.0.26100-SP0", processor_identifier="Intel64 Family 6")
    )
    assert spec.cpu_model == "Intel64 Family 6"


def test_the_probe_reads_the_machine_this_test_is_running_on_without_raising():
    """The derived spec on the real box, so a reader can see what a real run
    attaches. Asserts only that every field resolved to something printable --
    no rate, because there is no gateway here to produce one."""
    spec = machine_spec(MachineFacts.read())
    assert spec.os and spec.python
    assert spec.cpu_model
    assert isinstance(spec.in_wsl, bool)
    assert isinstance(spec.to_dict()["cpu_limit"], str)


# =============================================================================
# 4. the receipt
# =============================================================================


def test_the_summary_says_in_its_own_first_field_that_it_is_not_a_benchmark():
    summary = _report().to_dict()
    assert list(summary)[0] == "not_a_benchmark"
    assert "not a benchmark" in summary["not_a_benchmark"].lower()
    assert "not a benchmark" in NOT_A_BENCHMARK.lower()
    assert CAVEATS, "the caveats are the argument; an empty tuple says nothing"


def test_the_summary_carries_no_field_named_the_way_a_capacity_claim_is_named():
    """A field called `capacity` or `supports_events_per_sec` is read as a claim
    before it is read as a measurement, whatever it contains. The names are the
    constraint; the values are somebody else's problem."""
    banned = ("capacity", "supports", "throughput", "sustainable", "headroom", "max_")
    keys = _keys(_report().to_dict())
    for key in keys:
        assert not any(word in key.lower() for word in banned), f"{key} reads like a claim"


def test_the_summary_puts_the_machine_on_every_run_even_when_nothing_was_probed():
    for spec in (machine_spec(MachineFacts()), machine_spec(MachineFacts.read())):
        summary = _report(machine=spec).to_dict()
        assert set(summary["machine"]) == set(MachineSpec(  # same shape either way
            os="", cpu_model="", cpu_cores_logical=None, cpu_cores_available=None,
            ram_bytes=None, python="", in_wsl=False, container_runtime="none",
            cpu_limit_cpus=None, cpu_limit_source="unknown",
        ).to_dict())


def test_the_achieved_rate_is_this_runs_sent_events_over_this_runs_elapsed_seconds():
    report = _report(replay=ReplayReport(sent=2500, accepted=2500), elapsed_seconds=2.0)
    assert report.achieved_events_per_sec == pytest.approx(1250.0)
    assert report.achieved_accepted_events_per_sec == pytest.approx(1250.0)
    summary = report.to_dict()
    assert summary["achieved"]["events_sent_per_sec"] == pytest.approx(1250.0)
    assert summary["run"]["elapsed_seconds"] == 2.0


def test_a_run_that_sent_nothing_reports_a_rate_of_nothing_rather_than_dividing_by_zero():
    report = _report(replay=ReplayReport(), elapsed_seconds=0.0)
    assert report.achieved_events_per_sec == 0.0
    assert report.held_requested_rate is None


def test_holding_the_rate_is_a_comparison_with_the_rate_this_run_was_asked_for():
    asked = 1000.0
    on_the_nose = _report(replay=ReplayReport(sent=int(asked * 1.0)), elapsed_seconds=1.0)
    behind = _report(replay=ReplayReport(sent=500), elapsed_seconds=1.0)
    assert on_the_nose.held_requested_rate is True
    assert behind.held_requested_rate is False
    assert _report().to_dict()["achieved"]["rate_tolerance"] == RATE_TOLERANCE


def test_the_latency_block_says_where_the_numbers_came_from():
    """Client-side and exact, over every attempt. The gateway's own
    `gateway_request_latency_seconds` is bucketed and excludes the network, so
    quoting a percentile from it next to these without saying which is which is
    how two runs end up being compared on two different scales."""
    latency = _report().to_dict()["latency_client_side_seconds"]
    assert latency["where"] == "client"
    assert latency["samples"] == 1
    assert latency["p50_seconds"] == 0.01
    assert latency["p99_seconds"] == 0.05
    assert "gateway_request_latency_seconds" in latency["not_the_same_as"]


def test_the_error_breakdown_keeps_the_gateways_own_reason_codes():
    report = _report(
        replay=ReplayReport(
            batches=10,
            sent=1000,
            accepted=900,
            rejected=50,
            rate_limited=4,
            sink_unavailable=1,
            by_status={"RATE_LIMITED": 4, "PRODUCER_QUEUE_FULL": 1},
        )
    )
    errors = report.to_dict()["errors"]
    assert errors["batches_by_status"] == {"PRODUCER_QUEUE_FULL": 1, "RATE_LIMITED": 4}
    assert errors["rate_limited_batches"] == 4
    assert errors["sink_unavailable_batches"] == 1
    assert errors["events_rejected_by_validation"] == 50
    assert errors["events_unaccepted"] == 50


# =============================================================================
# 5. end to end through the real ingest endpoint, with the FakeSink
# =============================================================================
#
# Nothing here is a stub of the gateway: a real FastAPI app, the real handler and
# pipeline, a real EdDSA token verified by the real `TokenVerifier`, and the real
# `ReplayDriver`. Only the broker is replaced (`FakeSink`) -- the substitution
# `app/test_app.py` already makes, and the reason this file can prove the pacing
# and the receipt with no cluster and therefore no measured rate.


def test_a_load_run_against_the_real_ingest_endpoint_holds_its_rate_and_reports_its_latency(
    gateway, minter, tmp_path
):
    batches = _corpus_batches(240, max_events=50)
    sink = gateway.state.sink.inner
    clock = FakeClock()
    report = run_load(
        url="http://gateway",
        signer=minter,
        client=TestClient(gateway),
        batches=batches,
        ledger_path=tmp_path / "load-ledger.jsonl",
        rate_events_per_sec=2000.0,
        duration_seconds=1.0,
        max_events=50,
        max_bytes=DEFAULT_MAX_BATCH_BYTES,
        clock=clock,
        sleep=clock.sleep,
    )
    # The wall clock here is the fake one, so what is checked is the arithmetic
    # and the counts -- not a measured rate. The latency samples ARE real, because
    # `TimedClient` reads `perf_counter` itself.
    assert report.replay.clean, report.replay.by_status
    assert report.replay.sent == report.replay.accepted == len(sink.records)
    assert report.replay.sent == sum(len(batch) for batch in batches)
    assert report.samples == report.replay.batches, "one timed attempt per request"
    assert report.p50_seconds is not None and report.p50_seconds <= report.slowest_seconds
    assert report.p99_seconds is not None and report.p50_seconds <= report.p99_seconds
    assert report.held_requested_rate is True
    assert report.machine.os, "every run carries the machine it ran on"


def test_the_run_is_one_connection_for_the_whole_load_not_one_per_request(
    gateway, minter, tmp_path
):
    """`docker-compose.yml` puts the driver on separate cores precisely because
    the driver is the instrument. A handshake per request would make the receipt
    a measurement of the network."""
    clock = FakeClock()
    with TestClient(gateway) as client:
        report = run_load(
            url="http://gateway",
            signer=minter,
            client=client,
            batches=_corpus_batches(120, max_events=25),
            ledger_path=tmp_path / "load-ledger.jsonl",
            rate_events_per_sec=5000.0,
            duration_seconds=0.5,
            clock=clock,
            sleep=clock.sleep,
        )
    assert report.replay.batches > 3
    assert report.samples == report.replay.batches, "a retry would show as a second sample"


def test_a_gateway_that_sheds_shows_up_in_the_breakdown_rather_than_as_a_missing_number(
    keypair, minter, tmp_path
):
    shed = _build_gateway(
        keypair[1], tenant_limits={TENANTS[0]: TenantLimits(rate=1.0, burst=1.0)}
    )
    clock = FakeClock()
    with TestClient(shed) as client:
        report = run_load(
            url="http://gateway",
            signer=minter,
            client=client,
            batches=_corpus_batches(120, max_events=25),
            ledger_path=tmp_path / "load-ledger.jsonl",
            rate_events_per_sec=2000.0,
            duration_seconds=0.5,
            max_attempts=1,
            clock=clock,
            sleep=clock.sleep,
        )
    errors = report.to_dict()["errors"]
    assert errors["rate_limited_batches"] > 0
    assert errors["batches_by_status"], "the gateway's own reason code, not a status"
    assert errors["events_unaccepted"] > 0
    assert report.replay.clean is False


def test_the_load_run_writes_its_own_ledger_and_leaves_the_demos_ground_truth_alone(
    gateway, minter, tmp_path, monkeypatch
):
    """`tools/verify.py` reconciles against `ledger.jsonl`. A load run that
    overwrote it would destroy the demo's ground truth to make its own numbers
    look tidier."""
    monkeypatch.chdir(tmp_path)
    clock = FakeClock()
    report = run_load(
        url="http://gateway",
        signer=minter,
        client=TestClient(gateway),
        batches=_corpus_batches(120),
        ledger_path=DEFAULT_LEDGER,
        rate_events_per_sec=2000.0,
        duration_seconds=0.5,
        clock=clock,
        sleep=clock.sleep,
    )
    assert DEFAULT_LEDGER.name != "ledger.jsonl"
    assert len(Ledger(DEFAULT_LEDGER).read_all()) == report.replay.sent
    assert not (tmp_path / "ledger.jsonl").exists()


# =============================================================================
# 6. the CLI, over a real socket
# =============================================================================
#
# `main()` builds its own `httpx.Client`, so it needs something to talk to. A
# loopback server that speaks the 202 CONTRACT.md section 2 specifies is enough,
# and it is why the CLI test is not a transport double: `python -m bench.load`
# under compose is this same code path, minus the container.


class _StubGateway(http.server.ThreadingHTTPServer):
    """A loopback `/v1/ingest` that accepts a batch and counts connections.

    `connections` is the keep-alive evidence: a client opened per request would
    still produce a plausible number, which is exactly why it needs a count.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self) -> None:
        super().__init__(("127.0.0.1", 0), _StubHandler)
        self.requests: list[tuple[str, dict, bytes]] = []
        self.connections = 0
        self.status = 202

    @property
    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def get_request(self):
        self.connections += 1
        return super().get_request()


class _StubHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # persistent, which is the point

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length)
        server: _StubGateway = self.server  # type: ignore[assignment]
        server.requests.append((self.path, dict(self.headers), body))
        if server.status == 202:
            payload = msgspec.json.encode(
                {"accepted": len(msgspec.json.decode(body, type=list)), "rejected": []}
            )
        else:
            payload = msgspec.json.encode({"reason": "PRODUCER_QUEUE_FULL"})
        self.send_response(server.status)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        if server.status != 202:
            self.send_header("Retry-After", "0")
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args) -> None:
        """Silent: the default handler writes every request to stderr, which
        would bury the assertions in this file's pytest output."""


@pytest.fixture
def live_gateway():
    server = _StubGateway()
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True
    )
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def signing_key(tmp_path, keypair) -> str:
    path = tmp_path / "driver-signing-key.pem"
    path.write_bytes(keypair[0])
    return str(path)


@pytest.fixture(autouse=True)
def no_ambient_tenants(monkeypatch):
    """The CLI reads `GATEWAY_TENANTS`; a developer's shell must not decide what
    these tests replay."""
    monkeypatch.delenv("GATEWAY_TENANTS", raising=False)
    monkeypatch.delenv("GATEWAY_URL", raising=False)


def _cli(server, key, ledger: Path, *extra: str) -> list[str]:
    return [
        "--url", server.url,
        "--signing-key", key,
        "--ledger", str(ledger),
        "--tenants", "3",
        *extra,
    ]


def test_the_cli_prints_one_json_receipt_that_declares_itself_and_names_the_machine(
    live_gateway, signing_key, tmp_path, capsys
):
    assert main(_cli(live_gateway, signing_key, tmp_path / "load.jsonl", "--rate", "2000",
                    "--duration", "0.3", "--sessions", "1000", "--max-events", "100")) == 0
    summary = json.loads(capsys.readouterr().out)
    assert "not a benchmark" in summary["not_a_benchmark"].lower()
    assert summary["run"]["requested_events_per_sec"] == 2000.0
    assert summary["achieved"]["events_sent_per_sec"] > 0
    assert summary["ledger"]["sent"] == summary["ledger"]["accepted"] > 0
    assert summary["machine"]["python"]
    assert summary["latency_client_side_seconds"]["samples"] == len(live_gateway.requests)
    assert live_gateway.connections == 1, "the load run opened more than one connection"
    assert list(summary)[0] == "not_a_benchmark"


def test_a_gateway_that_refuses_everything_exits_non_zero_and_says_which_code(
    live_gateway, signing_key, tmp_path, capsys
):
    live_gateway.status = 503
    assert main(_cli(live_gateway, signing_key, tmp_path / "load.jsonl", "--rate", "2000",
                    "--duration", "0.2", "--sessions", "40", "--max-attempts", "1")) == 1
    summary = json.loads(capsys.readouterr().out)
    assert summary["ledger"]["clean"] is False
    assert summary["errors"]["batches_by_status"] == {"PRODUCER_QUEUE_FULL": len(
        live_gateway.requests
    )}


def test_the_cli_refuses_a_rate_of_zero_before_it_sends_anything(
    live_gateway, signing_key, tmp_path, capsys
):
    with pytest.raises(SystemExit):
        main(_cli(live_gateway, signing_key, tmp_path / "load.jsonl", "--rate", "0"))
    assert live_gateway.requests == []
    assert capsys.readouterr().out == ""


def test_the_parser_documents_its_own_refusal_to_be_a_benchmark():
    parser = build_parser()
    description = (parser.description or "") + " ".join(
        action.help or "" for action in parser._actions
    )
    assert "benchmark" in description.lower()
    assert parser.prog == "python -m bench.load"