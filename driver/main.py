"""T8d: the driver's entry point. `python -m driver.main`.

This is the process `docker-compose.yml` runs, and the one command the demo
drives load through. It wires five things together and contains no logic of its
own, which is why each of them lives somewhere else:

* **where the traffic comes from** -- `driver/skew.py` (the 500-tenant Zipfian
  corpus the CLI in `driver/inject.py` also builds), and `driver/inject.py` when
  the run has to carry an exact fraction of malformed events;
* **who signs the tokens** -- `driver/replay.py`, which holds the private half of
  the key pair. The gateway is given only the public one, so this process is the
  only thing in the system that can mint a token naming a tenant;
* **how a batch goes on the wire** -- `driver/replay.py`, including the caps, the
  stable `id` across retries, and the bounded retries;
* **what actually went out** -- the ledger, written by the replay driver because
  it is the component that knows;
* **where the answer is** -- stdout, as one JSON summary, and the exit code.

## The three decisions that are not obvious

**The tenant list is the gateway's, not ours.** `GATEWAY_TENANTS` is the single
source the gateway expands into its registry (`app.main.credentials_from_env`),
so when it is set the driver uses it and `--tenants` is not consulted. A driver
that named tenants the gateway has not provisioned would get a 403 per request
and the run would look like a gateway fault.

**One HTTP client for the whole run**, built here and handed to the driver.
`httpx.Client` keeps the connection alive between requests, and a handshake per
request at this request rate is fatal and gets misdiagnosed as a gateway problem.
It is closed in a `finally` so an exception mid-run does not leak the pool.

**A run that lost a batch exits non-zero.** The checkpoint is `sent == accepted`,
so a run that cannot make that claim has to say so in the exit code as well as in
the summary. A refused batch is not a lost event -- the ledger has it, and
`unaccepted` counts it -- but it is a run whose reconciliation will not balance,
and reporting that as success is how a demo ends up over-claiming.

The summary is the last thing printed and it is the whole receipt: the run's
counts, the ledger path, and whether `sent == accepted` held. Anything else on
stdout would make that harder to read out of `docker compose logs driver`.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path

import httpx

from driver.inject import MalformedInjector, count_events
from driver.replay import (
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_BATCH_BYTES,
    DEFAULT_MAX_EVENTS_PER_BATCH,
    ReplayDriver,
    TokenMinter,
)
from driver.skew import SkewedCorpus
from driver.tenants import DEFAULT_TENANT_COUNT, DEFAULT_USERS_PER_TENANT, TenantCatalog

#: Where the gateway is when nothing says otherwise. `docker-compose.yml` sets
#: `GATEWAY_URL`, so this default is the local run and the compose run differ in
#: one environment variable.
DEFAULT_GATEWAY_URL = "http://localhost:8000"

#: The compose header writes the private key next to `.env`, which is the working
#: directory the driver service runs in.
DEFAULT_SIGNING_KEY = "driver-signing-key.pem"

#: The value `app.config.Settings.jwt_audience` falls back to. `JWT_AUD` is read at
#: parser-build time from both sides, so a deployment that changes one changes both
#: -- a token signed for the wrong audience is a 401 on every request.
DEFAULT_AUDIENCE = "career-api"

#: A load run pushes bodies of a couple of MiB, and httpx's 5-second default is
#: not obviously generous for one -- and it is not settable per request, so it is
#: set once, here, for the whole run.
DEFAULT_TIMEOUT_SECONDS = 30.0

#: A thousand sessions is a few thousand events: enough to exercise every batch
#: path, every channel and every tenant, and quick enough that a wiring mistake is
#: obvious in a second run rather than ten minutes in. A 50k events/sec demo
#: passes a bigger number.
DEFAULT_SESSIONS = 1_000

#: The ledger's default home. In the working directory, which under compose is the
#: repository bind-mount, so `docker compose logs driver` and the file the run wrote
#: are in the same place the runbook can point at.
DEFAULT_LEDGER = Path("ledger.jsonl")


class ConfiguredCatalog(TenantCatalog):
    """A catalog over the gateway's own tenant list rather than a generated one.

    `TenantCatalog` mints `tenant_0001..tenant_0500` from a count, which happens
    to be what the compose `.env` generates -- and "happens to" is the failure
    mode: change the gateway's list and the driver would keep replaying tenants
    that no longer exist. Overriding the two plain attributes `__init__` sets is
    enough, because everything derived (users, shard routing) is computed from
    `self.ids`.
    """

    def __init__(
        self,
        career_site_ids: Sequence[str],
        *,
        seed: int = 7,
        users_per_tenant: int = DEFAULT_USERS_PER_TENANT,
    ) -> None:
        ids = [entry.strip() for entry in career_site_ids if entry.strip()]
        if not ids:
            raise ValueError("career_site_ids must name at least one tenant")
        super().__init__(len(ids), seed=seed, users_per_tenant=users_per_tenant)
        self.ids = ids


def tenants_from_env(environ: Mapping[str, str] | None = None) -> list[str]:
    """`GATEWAY_TENANTS` as a list, or nothing when it is unset.

    Unset is not an error: a local run against a gateway configured some other way
    is legitimate, and `--tenants` covers it.
    """
    env = os.environ if environ is None else environ
    return [entry.strip() for entry in env.get("GATEWAY_TENANTS", "").split(",") if entry.strip()]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m driver.main",
        description="Replay a generated corpus into POST /v1/ingest, single-tenant "
        "batches within the gateway's caps, and write the ground-truth ledger.",
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
        help="PEM holding the PRIVATE signing key. Default: $DRIVER_SIGNING_KEY, else "
        f"./{DEFAULT_SIGNING_KEY}. The gateway must never be able to read this.",
    )
    parser.add_argument(
        "--audience",
        default=os.getenv("JWT_AUD", DEFAULT_AUDIENCE),
        help=f"token audience; must match the gateway's JWT_AUD (default: {DEFAULT_AUDIENCE})",
    )
    parser.add_argument(
        "--tenants",
        type=int,
        default=DEFAULT_TENANT_COUNT,
        help="tenants in the catalog. Ignored when GATEWAY_TENANTS is set, because the "
        "driver has to name tenants the gateway has registered.",
    )
    parser.add_argument("--sessions", type=int, default=DEFAULT_SESSIONS)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--users-per-tenant", type=int, default=DEFAULT_USERS_PER_TENANT)
    parser.add_argument(
        "--max-events",
        type=int,
        default=DEFAULT_MAX_EVENTS_PER_BATCH,
        help="events per request (capped at the gateway's 500). "
        f"Default: {DEFAULT_MAX_EVENTS_PER_BATCH}",
    )
    parser.add_argument(
        "--max-batch-bytes",
        type=int,
        default=DEFAULT_MAX_BATCH_BYTES,
        help="request body ceiling (capped at the gateway's 4 MiB). "
        f"Default: {DEFAULT_MAX_BATCH_BYTES}",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        help=f"attempts per batch on 429/503/connection reset. Default: {DEFAULT_MAX_ATTEMPTS}",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="per-request timeout, seconds",
    )
    parser.add_argument(
        "--ledger",
        type=Path,
        default=DEFAULT_LEDGER,
        help=f"ground-truth ledger to write. Default: {DEFAULT_LEDGER}",
    )
    parser.add_argument(
        "--inject-invalid-rate",
        type=float,
        default=0.0,
        metavar="PCT",
        help="exactly this percentage of events is malformed (0-100), for the DLQ beat. "
        "Default 0: a clean run.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 0.0 <= args.inject_invalid_rate <= 100.0:
        parser.error("--inject-invalid-rate is a percentage and must be between 0 and 100")
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
            f"signing key {key_path} not found. The driver mints the bearer tokens and "
            "so needs the PRIVATE half of the pair; the header of docker-compose.yml "
            "writes it to driver-signing-key.pem beside .env"
        )

    configured = tenants_from_env()
    catalog: TenantCatalog = (
        ConfiguredCatalog(configured, seed=args.seed, users_per_tenant=args.users_per_tenant)
        if configured
        else TenantCatalog(args.tenants, seed=args.seed, users_per_tenant=args.users_per_tenant)
    )

    def corpus() -> SkewedCorpus:
        # A fresh corpus per pass, not one instance asked twice: the generator
        # advances its own RNG, so a second pass over the same instance is a
        # different-sized run and an exact injection count would be a lie. See
        # `count_events`.
        return SkewedCorpus(
            catalog=catalog, seed=args.seed, users_per_tenant=args.users_per_tenant
        )

    injector = MalformedInjector(rate=args.inject_invalid_rate / 100.0, seed=args.seed)
    total = (
        count_events(corpus().batches(args.sessions, max_events=args.max_events))
        if injector.enabled
        else None
    )
    # `MalformedInjector.batches`, never `.build`: the injector writing the ledger
    # as well as the replay driver would put every event in the ground truth
    # twice, and `Ledger.duplicates` is the number reconciliation asserts is zero.
    batches = injector.batches(
        corpus().batches(args.sessions, max_events=args.max_events), total_events=total
    )

    # The minter is built first because it is the step that can fail on a
    # misconfiguration, and it is the step that needs cleaning up after.
    signer = TokenMinter(key_path.read_bytes(), audience=args.audience)
    client = httpx.Client(timeout=args.timeout)
    try:
        report = ReplayDriver(
            url=args.url,
            signer=signer,
            client=client,
            ledger_path=args.ledger,
            max_attempts=args.max_attempts,
            max_events=args.max_events,
            max_bytes=args.max_batch_bytes,
        ).run(batches)
    finally:
        # One client for the whole run is the keep-alive; closing it is the only
        # cleanup there is, and it belongs here so an exception mid-run cannot
        # leave the pool open.
        client.close()

    summary: dict = report.to_dict()
    summary["url"] = args.url
    summary["tenants"] = len(catalog.ids)
    summary["ledger"] = str(args.ledger)
    summary["ledger_rows"] = report.sent
    if injector.enabled:
        summary["injected"] = injector.report.injected
        summary["by_variant"] = dict(sorted(injector.report.by_variant.items()))
    print(json.dumps(summary, indent=2))
    return 0 if report.clean else 1


__all__ = [
    "DEFAULT_AUDIENCE",
    "DEFAULT_GATEWAY_URL",
    "DEFAULT_LEDGER",
    "DEFAULT_SESSIONS",
    "DEFAULT_SIGNING_KEY",
    "DEFAULT_TIMEOUT_SECONDS",
    "ConfiguredCatalog",
    "build_parser",
    "main",
    "tenants_from_env",
]


if __name__ == "__main__":
    raise SystemExit(main())
