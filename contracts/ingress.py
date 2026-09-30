"""Ingress schema: what a CLIENT sends, before encryption.

This is deliberately a different shape from `contracts.cloudevent.CloudEvent`,
which is what lands on the topic. The two must not be conflated:

- A client **cannot** hold a tenant key, so it cannot produce ciphertext. It
  sends plaintext.
- The gateway **must not** publish a ciphertext it did not produce, so a
  client-supplied `email_enc` / `email_hmac` is refused rather than passed
  through. `forbid_unknown_fields` is what enforces that.

Having one struct for both directions is the bug this module exists to prevent:
a single "no plaintext fields, so PII cannot leak" struct is a good egress shape
and a broken ingress shape, because then no client can post anything at all.

The published examples in `contracts/examples/` and everything the driver emits
are in THIS shape, because that is what a client would send.
"""

from __future__ import annotations

import msgspec

from contracts.cloudevent import EventPayload

#: Matches `app.crypto.facade.PII_FIELDS` — the five fields the gateway encrypts.
PLAINTEXT_PII_FIELDS = ("email", "phone", "alternate_phone", "name", "gender")


class IngressCandidate(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """`data.candidate` as a client sends it.

    `user_id` is the client's own opaque per-user identifier, NOT a pseudonym.
    The gateway HMACs it into `user_id_pseudo` during the encrypt stage, because
    the `mac_key` derives from a master secret the gateway alone holds. The two
    fields are deliberately named differently so nobody mistakes the raw value
    for the hashed one.
    """

    user_id: str
    email: str | None = None
    phone: str | None = None
    alternate_phone: str | None = None
    name: str | None = None
    gender: str | None = None
    experience_status: str | None = None
    years_of_experience: float | None = None
    education_degree: str | None = None
    education_branch: str | None = None


class IngressData(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    candidate: IngressCandidate
    #: The published payload struct verbatim. There is nothing to loosen about
    #: it, and a second copy would only be a second thing to keep in sync.
    event_payload: EventPayload


class IngressEvent(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """One CloudEvents batch element as it arrives on the wire.

    The field set is the published allowlist
    (`contracts.attributes.SAFE_CONTEXT_ATTRS`) plus `data`, which a test asserts,
    so the two cannot drift. `forbid_unknown_fields` therefore enforces the
    allowlist here rather than relying on a separate pass over the raw dict.
    """

    specversion: str
    id: str
    source: str
    type: str
    time: str
    data: IngressData
    subject: str | None = None
    dataschema: str | None = None
    datacontenttype: str | None = None
    keyversion: int | None = None
    sequence: str | None = None
    sourcechannel: str | None = None
    referrertype: str | None = None
    completionmethod: str | None = None
    #: Never used for routing. Read only so a hint disagreeing with the derived
    #: key can be refused with 403.
    partitionkey: str | None = None


_DECODER = msgspec.json.Decoder(type=list[IngressEvent])


def decode_ingress(raw: bytes) -> list[IngressEvent]:
    """Decode an ingress batch (`application/cloudevents-batch+json`)."""
    return _DECODER.decode(raw)


def generate_ingress_schema_json() -> str:
    """JSON Schema for the ingress envelope, generated from the structs.

    `msgspec.json.schema_components` returns `(schemas, components)` where the
    top-level schema is a bare `$ref` into `components`. Emitting only `schemas[0]`
    would publish a three-line file pointing at definitions that are not there, so
    the components are spliced in under `$defs` and the real root is returned.
    """
    import json

    schemas, components = msgspec.json.schema_components(
        [IngressEvent], ref_template="#/$defs/{name}"
    )
    document = dict(schemas[0])
    if "$defs" not in document:
        document["$defs"] = components
    return json.dumps(document, indent=2)
