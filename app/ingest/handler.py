"""`POST /v1/ingest`.

**No Pydantic request model, on purpose.** FastAPI's validation layer would
reject the whole request before a single event was examined, and that is the
opposite of this endpoint's contract: rejection is per-event, and the answer is
`202 {accepted, rejected}` rather than a `422` listing one field path. So the
handler reads `await request.body()` and hands the bytes to the pipeline, which
parses with msgspec and keeps per-event verdicts.

Every status the contract names is decided before this function returns, by
raising. The mapping below is the whole of the decision logic.
"""

from __future__ import annotations

import logging

import msgspec
from fastapi import APIRouter, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from app.auth import TokenParseBudget, TokenVerifier, TenantRegistry
from app.auth.errors import AuthError
from app.config import TOPIC_RAW
from app.crypto.registry import TenantKeyRegistry
from app.ingest.limits import IngestError, check_declared_length
from app.ingest.pipeline import IngestResult, Sink, ingest_batch
from app.ratelimit.registry import BucketRegistry

log = logging.getLogger("app.ingest")

#: 503 says "try again", and the client is expected to resend the same batch with
#: the same `id`s. One second is the shortest honest value: a Kafka producer
#: queue drains continuously, so there is nothing to wait for beyond a poll.
_RETRY_AFTER_SECONDS = 1

#: 401 must not say WHICH check failed. Telling a prober "the audience was
#: wrong" instead of "the signature was wrong" turns the endpoint into a
#: signature oracle, so the body is a fixed code and the detail goes to the log.
_REASON_UNAUTHORIZED = "UNAUTHORIZED"
_REASON_FORBIDDEN = "FORBIDDEN"


def build_ingest_router(
    *,
    verifier: TokenVerifier,
    tenants: TenantRegistry,
    keys: TenantKeyRegistry,
    buckets: BucketRegistry,
    sink: Sink,
    parse_budget: TokenParseBudget | None = None,
) -> APIRouter:
    """The ingest route, with its collaborators injected.

    Injected rather than imported so that `app/main.py` owns the process wiring
    and this module owns no global state.
    """
    router = APIRouter()

    @router.post("/v1/ingest")
    async def ingest(request: Request) -> Response:
        # Before `await request.body()`: uvicorn has no default request-body
        # limit, so this is the only thing standing between one request and an
        # arbitrary allocation. (The cap inside the pipeline runs after the bytes
        # already exist, which is a correctness check, not a DoS defence.)
        try:
            check_declared_length(request.headers.get("content-length"))
            raw = await request.body()
            result = await run_in_threadpool(
                ingest_batch,
                raw,
                authorization=request.headers.get("authorization"),
                x_source_type=request.headers.get("x-source-type"),
                sink=sink,
                verifier=verifier,
                tenants=tenants,
                keys=keys,
                buckets=buckets,
                parse_budget=parse_budget,
            )
        except AuthError as exc:
            # `exc.reason` may quote a tenant id or a body `source`, so it is
            # logged and not echoed.
            log.warning("ingest refused: status=%s reason=%s", exc.status_code, exc.reason)
            reason = _REASON_UNAUTHORIZED if exc.status_code == 401 else _REASON_FORBIDDEN
            return JSONResponse({"reason": reason}, status_code=exc.status_code)
        except (msgspec.DecodeError, ValueError) as exc:
            # Unreachable in practice -- the pipeline turns its own decode errors
            # into per-event rejections. Kept so a surprise cannot become a 500
            # with a traceback on the hot path.
            log.error("ingest failed: %s", type(exc).__name__)
            return JSONResponse({"reason": "BAD_REQUEST"}, status_code=400)
        except IngestError as exc:
            # 400 / 413 / 429 / 503. Each carries the status and the reason code
            # it must be answered with, so there is nothing left to decide here.
            return _failure_response(exc)

        return _accepted_response(result)

    return router


def _accepted_response(result: IngestResult) -> Response:
    if result.rejected:
        # Codes and counts only. A rejection reason reaches a Kafka topic, so it
        # is already a place a value must never appear, let alone a log line.
        log.info(
            "ingest accepted into buffer: accepted=%d rejected=%d topic=%s",
            result.accepted,
            len(result.rejected),
            TOPIC_RAW,
        )
    return JSONResponse(result.to_response(), status_code=202)


def _failure_response(exc: IngestError) -> Response:
    headers = None
    if exc.retry_after is not None:
        headers = {"Retry-After": str(max(_RETRY_AFTER_SECONDS, int(exc.retry_after)))}
    log.warning("ingest refused: status=%s reason=%s", exc.status_code, exc.reason)
    return JSONResponse({"reason": exc.reason}, status_code=exc.status_code, headers=headers)
