"""Request limits, enforced before anything is allocated.

Order matters here, and it is the whole point of the module:

1. **The declared `Content-Length` is checked before the body is read.** This is
   the only pre-allocation lever a FastAPI handler has -- uvicorn has no default
   request-body limit, so without this a single request can make the process
   allocate an arbitrary buffer. (The cap below cannot help here: by the time it
   runs, the bytes already exist.)
2. **The body length is checked before it is parsed.**
3. **The batch is split into `msgspec.Raw` slices, which cost no allocation
   proportional to their contents**, and the element count and the per-event
   size are checked on those raw byte lengths.
4. Only then is a single event materialised as a dict.

Without step 3, "more than 500 events" could only be discovered by building 500
dicts, which is the allocation the count limit exists to prevent. The byte cap is
what actually bounds memory -- the element count is a contract limit, checked
while the allocation is still 500 pointers.
"""

from __future__ import annotations

import msgspec

from app.config import INGRESS_EVENT_BYTES, MAX_BATCH_BYTES, MAX_EVENTS_PER_BATCH

#: Reason CODES, never values: these strings reach logs and an HTTP response
#: body, and a rejection reason that quotes the offending bytes is a way to
#: move plaintext PII out of the request.
REASON_BODY_BYTES = "BODY_TOO_LARGE"
REASON_TOO_MANY_EVENTS = "TOO_MANY_EVENTS"
REASON_EVENT_BYTES = "EVENT_TOO_LARGE"
REASON_NOT_A_BATCH = "BODY_NOT_A_BATCH"


class IngestError(Exception):
    """A whole-request failure, carrying the status the handler must return."""

    status_code = 500
    retry_after: int | None = None

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class PayloadTooLarge(IngestError):
    """413. A contract cap was exceeded, so the request was never parsed."""

    status_code = 413


class NotABatch(IngestError):
    """400. The body is not a CloudEvents JSON array at all."""

    status_code = 400


def check_declared_length(content_length: str | None) -> None:
    """Refuse an over-long or untrustworthy `Content-Length`, before reading.

    An unparseable value is refused rather than ignored: we will not read a body
    whose advertised length we cannot check, and 413 is the conservative answer
    because the body may well be the oversized one that lied.
    """
    if content_length is None:
        return
    try:
        declared = int(content_length)
    except ValueError:
        raise PayloadTooLarge(REASON_BODY_BYTES) from None
    if declared > MAX_BATCH_BYTES:
        raise PayloadTooLarge(REASON_BODY_BYTES)


def split_limited_batch(body: bytes) -> list[msgspec.Raw]:
    """The request body as a list of undecoded event byte slices.

    Every cap is checked here, before any event becomes an object. A `Raw` is a
    view over the request buffer, so holding 500 of them costs 500 pointers
    rather than 500 parsed dicts.
    """
    if len(body) > MAX_BATCH_BYTES:
        raise PayloadTooLarge(REASON_BODY_BYTES)

    try:
        items = msgspec.json.decode(body, type=list[msgspec.Raw])
    except msgspec.DecodeError:
        # Not JSON, or not an array. A CloudEvents batch is a JSON array; there
        # are no per-event rejections to report because there are no events.
        raise NotABatch(REASON_NOT_A_BATCH) from None

    if len(items) > MAX_EVENTS_PER_BATCH:
        raise PayloadTooLarge(REASON_TOO_MANY_EVENTS)

    for item in items:
        # The ingress cap is stricter than MAX_EVENT_BYTES on purpose: it is what
        # keeps the ENCRYPTED event (base64 expansion plus nonce+tag per field)
        # under the CloudEvents 64 KiB ceiling. It is a batch-level 413, not a
        # per-event DLQ, because it is a client capacity error.
        if len(item) > INGRESS_EVENT_BYTES:
            raise PayloadTooLarge(REASON_EVENT_BYTES)

    return items
