"""T5: the ingestion gateway's request path.

`decode -> auth -> validate -> encrypt -> produce`, and the DLQ carries the
post-encryption event. See `app/ingest/pipeline.py` for why that order is
load-bearing, and `app/ingest/limits.py` for why the caps run before allocation.

The Kafka sink is injected (`Sink`), never imported: `app/kafka` is a separate
owner's module, and importing it here would put two owners in one file.
"""

from __future__ import annotations

from app.ingest.decrypt import (
    DECRYPTABLE_FIELDS,
    OPERATOR_KEY_HEADER,
    DecryptAudit,
    DecryptRequest,
    build_decrypt_router,
)
from app.ingest.handler import build_ingest_router
from app.ingest.limits import (
    REASON_BODY_BYTES,
    REASON_EVENT_BYTES,
    REASON_NOT_A_BATCH,
    REASON_TOO_MANY_EVENTS,
    IngestError,
    NotABatch,
    PayloadTooLarge,
    check_declared_length,
    split_limited_batch,
)
from app.ingest.pipeline import (
    DURABILITY_ACCEPTED_INTO_BUFFER,
    REASON_DLQ_UNAVAILABLE,
    REASON_RATE_LIMITED,
    REASON_SINK_UNAVAILABLE,
    IngestResult,
    IngressCandidate,
    IngressData,
    IngressEvent,
    RateLimited,
    Sink,
    SinkUnavailable,
    ingest_batch,
)

__all__ = [
    "DECRYPTABLE_FIELDS",
    "DURABILITY_ACCEPTED_INTO_BUFFER",
    "DecryptAudit",
    "DecryptRequest",
    "IngestError",
    "IngestResult",
    "IngressCandidate",
    "IngressData",
    "IngressEvent",
    "NotABatch",
    "OPERATOR_KEY_HEADER",
    "PayloadTooLarge",
    "REASON_BODY_BYTES",
    "REASON_DLQ_UNAVAILABLE",
    "REASON_EVENT_BYTES",
    "REASON_NOT_A_BATCH",
    "REASON_RATE_LIMITED",
    "REASON_SINK_UNAVAILABLE",
    "REASON_TOO_MANY_EVENTS",
    "RateLimited",
    "Sink",
    "SinkUnavailable",
    "build_decrypt_router",
    "build_ingest_router",
    "check_declared_length",
    "ingest_batch",
    "split_limited_batch",
]
