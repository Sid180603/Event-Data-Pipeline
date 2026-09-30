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


def validate_envelope(ev: CloudEvent) -> None:
    """Raise ValueError if the envelope breaks a prose CloudEvents rule."""
    for name in SAFE_CONTEXT_ATTRS:
        if not is_valid_attribute_name(name):  # pragma: no cover - guards our own table
            raise ValueError(f"our attribute name {name!r} is invalid")

    career_site_id_from_source(ev.source)

    if not _RFC3339_RE.match(ev.time):
        raise ValueError(f"time {ev.time!r} is not RFC 3339")

    if not ev.id:
        raise ValueError("id must be a non-empty string")

    # H3: identity events must carry a pseudonymous subject.
    if ev.type.endswith(("user-registered", "user-logged-in")):
        if ev.subject and ev.subject.startswith(("usr_", "user_")):
            raise ValueError("identity events must use a pseudonymous subject")


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
