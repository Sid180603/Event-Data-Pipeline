"""Envelope rules msgspec cannot express: naming, RFC 3339, tenant binding.

msgspec validates structure, types and enums. These are the semantic rules the
CloudEvents spec states in prose.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

from contracts.cloudevent import CloudEvent

#: CloudEvents naming conventions: lower-case letters and digits only, SHOULD
#: start with a letter, SHOULD NOT exceed 20 characters, MUST NOT be `data`.
_ATTR_RE = re.compile(r"^[a-z][a-z0-9]{0,19}$")

#: Context/extension attributes permitted in an envelope. Everything else fails
#: the allowlist regardless of its value -- a value-pattern test cannot catch a
#: raw `usr_992182741` (plan H3).
SAFE_CONTEXT_ATTRS = frozenset(
    {
        "specversion",
        "id",
        "source",
        "type",
        "subject",
        "time",
        "dataschema",
        "datacontenttype",
        "keyversion",
        "sequence",
        "sourcechannel",
        "referrertype",
        "completionmethod",
        "partitionkey",
    }
)

#: RFC 3339 date-time. `time` is carried as a string so we control the exact
#: serialisation; msgspec's datetime would reformat the offset.
_RFC3339_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$"
)

#: `source` is the tenant. A URI-reference of the form /careers/<career_site_id>.
_SOURCE_RE = re.compile(r"^/careers/[A-Za-z0-9_-]{1,64}$")


def is_valid_attribute_name(name: str) -> bool:
    return bool(_ATTR_RE.match(name)) and name != "data"


def career_site_id_from_source(source: str) -> str:
    """Extract the tenant from a `source` URI-reference."""
    m = _SOURCE_RE.match(source)
    if not m:
        raise ValueError(f"source must look like /careers/<career_site_id>, got {source!r}")
    return source.split("/", 2)[2]


_PROBLEM_FIELD = {
    "BAD_TIME": "$.time",
    "BAD_SOURCE": "$.source",
    "BAD_ID": "$.id",
    "RAW_SUBJECT": "$.subject",
}


def envelope_problem(
    ev: CloudEvent, *, require_pseudonymous_subject: bool = False
) -> str | None:
    """Return a safe problem CODE for the envelope, or None when it is valid.

    Codes rather than messages: a rejection reason is written to the DLQ, and a
    message built from the value (e.g. "time 'x' is not RFC 3339") would put the
    offending value into a second Kafka topic.

    `require_pseudonymous_subject` is an EGRESS-only rule. At ingress a client
    sends its own raw identifier -- it cannot HMAC anything, the gateway holds
    the secret. Only what we PUBLISH must carry `user_id_pseudo` (plan H3), so
    applying this to an ingress event would refuse every well-formed client.
    """
    if not _SOURCE_RE.match(ev.source):
        return "BAD_SOURCE"
    if not _RFC3339_RE.match(ev.time):
        return "BAD_TIME"
    if not ev.id:
        return "BAD_ID"
    if require_pseudonymous_subject and ev.type.endswith(
        ("user-registered", "user-logged-in")
    ):
        if ev.subject and ev.subject.startswith(("usr_", "user_")):
            return "RAW_SUBJECT"
    return None


def validate_envelope(ev: CloudEvent, *, published: bool = False) -> None:
    """Raise ValueError if the envelope breaks a prose CloudEvents rule.

    The message names the code and the field, never the value. Pass
    `published=True` for an event on its way to Kafka.
    """
    for name in SAFE_CONTEXT_ATTRS:
        if not is_valid_attribute_name(name):  # pragma: no cover - guards our own table
            raise ValueError(f"our attribute name {name!r} is invalid")

    problem = envelope_problem(
        ev, require_pseudonymous_subject=published
    )
    if problem:
        raise ValueError(f"{problem} at {_PROBLEM_FIELD.get(problem, '$')}")


def extra_attributes(raw: dict) -> set[str]:
    """Attribute names present in a raw envelope but not on the allowlist."""
    return set(raw) - SAFE_CONTEXT_ATTRS - {"data"}


def distinct_sources(events: Iterable[CloudEvent]) -> int:
    return len({ev.source for ev in events})


# --- Derived partition key (plan C2) ----------------------------------------


def derive_kafka_key(career_site_id: str, user_id_pseudo: str) -> str:
    """The Kafka message key, computed by the gateway.

    A client-supplied `partitionkey` is never used: a tenant could collide with
    another tenant's key or omit it, which is a tenant-isolation hole one row
    below the JWT binding. The raw user id never appears -- the HMAC
    pseudonymises it, so partitioning is unchanged while the identifier stays
    out of the most-inspected field in the system.
    """
    return f"{career_site_id}|{user_id_pseudo}"


def partitionkey_conflicts(supplied: str | None, career_site_id: str, user_id_pseudo: str) -> bool:
    """True when a client hint disagrees with the derived key (caller returns 403)."""
    if supplied is None:
        return False
    return supplied != derive_kafka_key(career_site_id, user_id_pseudo)
