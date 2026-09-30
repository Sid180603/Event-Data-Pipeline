"""Operator-only decrypt endpoint (M6).

Three properties, and each one is a separate guard rather than a consequence of
the others:

1. **A separate credential.** The operator key is not a tenant JWT and is not
   verified with the tenant verifier; a valid tenant token presented here is
   simply not an operator credential, and is refused. A tenant can therefore
   never ask for someone else's plaintext.
2. **Rate limited, independently of tenant budgets.** Decryption is a key
   operation and a cheap oracle, so it has its own bucket rather than spending a
   tenant's ingest budget.
3. **Audit logged on every attempt**, recording WHO and WHICH `(source, id)` --
   successes, denials, rate limits and failures alike. The record carries no
   plaintext: it is written to an audit sink that outlives the request, and a
   decrypted value sitting in an audit trail is exactly the thing this endpoint
   exists to prevent.

The decrypted value is returned in the response body and nowhere else. It is not
logged, not audited, and the response is marked `no-store`.
"""

from __future__ import annotations

import logging
import secrets
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone

import msgspec
from cryptography.exceptions import InvalidTag
from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse

from app.crypto.registry import TenantKeyRegistry, UnknownKeyVersionError, UnknownTenantError
from app.ratelimit.bucket import TokenBucket
from contracts.cloudevent import CloudEvent

log = logging.getLogger("app.ingest.decrypt")

OPERATOR_KEY_HEADER = "X-Operator-Key"

#: The five ciphertext fields, and nothing else. `user_id_pseudo` and
#: `email_hmac` are HMACs, so they are not decryptable -- but a whitelist is
#: still the right shape: an endpoint that takes a field NAME from its caller
#: should be able to say exactly which names it accepts.
DECRYPTABLE_FIELDS = frozenset(
    {"email_enc", "phone_enc", "alternate_phone_enc", "name_enc", "gender_enc"}
)

#: A denied caller has told us nothing about itself. The audit record still
#: exists, because "someone tried" is the fact worth keeping.
UNIDENTIFIED = "unidentified"

_REASON_UNAUTHORIZED = "UNAUTHORIZED"
_REASON_RATE_LIMITED = "RATE_LIMITED"
_REASON_BAD_REQUEST = "BAD_REQUEST"
_REASON_UNKNOWN_FIELD = "UNKNOWN_FIELD"
_REASON_UNKNOWN_TENANT = "UNKNOWN_TENANT"
#: One code for "wrong key", "wrong event", "wrong tenant pairing" and
#: "malformed ciphertext" alike. Distinguishing them for whoever is probing is
#: exactly what an oracle needs, and the operator learns nothing useful from the
#: distinction -- the request was wrong either way.
_REASON_DECRYPT_FAILED = "DECRYPT_FAILED"


class DecryptRequest(msgspec.Struct, forbid_unknown_fields=True):
    """One event, one field. Not a batch: an operator asking for 500 fields is
    asking for an export, and that is a different, differently-authorised thing."""

    career_site_id: str
    event: CloudEvent
    field: str


@dataclass(frozen=True, slots=True)
class DecryptAudit:
    """WHO decrypted WHICH `(source, id)`. Never the value."""

    actor: str
    career_site_id: str | None
    source: str | None
    event_id: str | None
    field: str | None
    outcome: str
    at: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_decrypt_router(
    *,
    keys: TenantKeyRegistry,
    operator_key: bytes,
    operator_id: str,
    bucket: TokenBucket,
    audit: Callable[[DecryptAudit], None],
) -> APIRouter:
    """The decrypt route. `audit` is the sink the app wires to a durable log;
    this module owns the record, not where records go."""
    if not operator_key:
        # Fail at wiring time rather than at request time: an empty configured key
        # would make the constant-time compare succeed for a caller who sent no
        # header at all, and a fail-open on the one credential that guards
        # plaintext PII is not something to discover in production.
        raise ValueError("operator_key must be non-empty")

    router = APIRouter()

    @router.post("/v1/decrypt")
    async def decrypt(request: Request) -> Response:
        # 1. Credential, before the body is read: the unauthenticated path stays
        #    as cheap as the ingest endpoint's.
        supplied = request.headers.get(OPERATOR_KEY_HEADER.lower(), "")
        if not secrets.compare_digest(supplied.encode(), operator_key):
            return _audit_and_fail(
                audit, None, "denied", _REASON_UNAUTHORIZED, 401, headers={"WWW-Authenticate": "OperatorKey"}
            )

        # 2. Rate limit before the expensive path.
        decision = bucket.acquire()
        if not decision.allowed:
            return _audit_and_fail(
                audit, None, "rate-limited", _REASON_RATE_LIMITED, 429,
                headers={"Retry-After": str(decision.retry_after)},
            )

        raw = await request.body()
        try:
            parsed = msgspec.json.decode(raw, type=DecryptRequest)
        except msgspec.DecodeError:
            return _audit_and_fail(audit, None, "rejected", _REASON_BAD_REQUEST, 400)

        if parsed.field not in DECRYPTABLE_FIELDS:
            return _audit_and_fail(audit, parsed, "rejected", _REASON_UNKNOWN_FIELD, 400)

        event = parsed.event
        token_value = getattr(event.data.candidate, parsed.field, None)
        if not token_value:
            return _audit_and_fail(audit, parsed, "failed", _REASON_DECRYPT_FAILED, 400)

        # The AAD is what makes this safe, not the tenant check: the ciphertext
        # authenticates against `(source, id, type, field, keyversion)`, so
        # pointing tenant A's key at tenant B's event fails authentication rather
        # than returning a plausible wrong value.
        try:
            value = keys.decrypt_field(
                parsed.career_site_id,
                token_value,
                source=event.source,
                event_id=event.id,
                event_type=event.type,
                field_name=parsed.field,
            )
        except UnknownTenantError:
            return _audit_and_fail(audit, parsed, "failed", _REASON_UNKNOWN_TENANT, 404)
        except (UnknownKeyVersionError, InvalidTag, ValueError, KeyError):
            return _audit_and_fail(audit, parsed, "failed", _REASON_DECRYPT_FAILED, 400)

        record = _record(operator_id, parsed, "ok")
        audit(record)
        log.info("decrypt: actor=%s source=%s id=%s field=%s", record.actor, record.source, record.event_id, record.field)
        return JSONResponse(
            {
                "career_site_id": parsed.career_site_id,
                "source": event.source,
                "id": event.id,
                "field": parsed.field,
                "value": value,
            },
            status_code=200,
            # Plaintext PII in a response body must not be cached by anything
            # between here and the operator.
            headers={"Cache-Control": "no-store"},
        )

    return router


def _record(actor: str, parsed: DecryptRequest | None, outcome: str) -> DecryptAudit:
    if parsed is None:
        return DecryptAudit(actor=UNIDENTIFIED, career_site_id=None, source=None, event_id=None, field=None, outcome=outcome, at=_now())
    return DecryptAudit(
        actor=actor,
        career_site_id=parsed.career_site_id,
        source=parsed.event.source,
        event_id=parsed.event.id,
        field=parsed.field,
        outcome=outcome,
        at=_now(),
    )


def _audit_and_fail(
    audit: Callable[[DecryptAudit], None],
    parsed: DecryptRequest | None,
    outcome: str,
    reason: str,
    status_code: int,
    headers: dict[str, str] | None = None,
) -> Response:
    record = _record(UNIDENTIFIED, parsed, outcome)
    audit(record)
    # The log line carries identifiers and an outcome, never a value.
    log.warning("decrypt refused: status=%s actor=%s source=%s id=%s", status_code, record.actor, record.source, record.event_id)
    return JSONResponse({"reason": reason}, status_code=status_code, headers=headers)
