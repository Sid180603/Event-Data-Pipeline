"""T2 contract: CloudEvents 1.0.3 envelope for the career-page pipeline.

This module is the single source of truth. `event.schema.json` is GENERATED from
these structs, so the published contract and the runtime validators cannot drift.

Spec reference: `SPEC.txt:131` requires validation to conform to CloudEvents
standards. CloudEvents is a CNCF Graduated project.
"""

from __future__ import annotations

import json
from typing import Literal, get_args

import msgspec

ORG_PREFIX = "com.careerpage.career."

SpecVersion = Literal["1.0"]

EventType = Literal[
    "com.careerpage.career.job-viewed",
    "com.careerpage.career.job-wishlisted",
    "com.careerpage.career.application-started",
    "com.careerpage.career.application-step-completed",
    "com.careerpage.career.application-draft-saved",
    "com.careerpage.career.application-submitted",
    "com.careerpage.career.application-abandoned",
    "com.careerpage.career.user-registered",
    "com.careerpage.career.user-logged-in",
]

EVENT_TYPES: tuple[str, ...] = get_args(EventType)

SourceChannel = Literal["WEB_APP", "MOBILE_APP", "THIRD_PARTY_SERVICE"]
ReferrerType = Literal["SEARCH", "RECOMMENDATION", "THIRD_PARTY_WEBHOOK", "DIRECT"]
CompletionMethod = Literal["MANUAL", "RESUME_AUTOFILL", "HYBRID"]

#: PII arrives encrypted or pseudonymised. `user_id_pseudo` is an HMAC, never a
#: raw identifier (plan D5). Plaintext fields do not exist in this struct at all,
#: so they cannot be serialised by accident.
#:
#: `forbid_unknown_fields` is load-bearing: without it a client posting a
#: plaintext `email` has it SILENTLY DROPPED at decode, and the event is then
#: accepted carrying no email with nobody aware. Rejecting is the correct
#: behaviour -- a loud failure the client can fix, per the additive-only
#: versioning policy in CONTRACT.md.
class CandidateMetadata(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    user_id_pseudo: str
    email_hmac: str | None = None
    email_enc: str | None = None
    phone_enc: str | None = None
    alternate_phone_enc: str | None = None
    name_enc: str | None = None
    gender_enc: str | None = None
    experience_status: Literal["FRESHER", "EXPERIENCED"] | None = None
    years_of_experience: float | None = None
    education_degree: str | None = None
    education_branch: str | None = None


class EventPayload(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    job_id: str
    session_id: str
    step_number: int | None = None
    step_name: str | None = None
    action: str | None = None
    completion_method: CompletionMethod | None = None
    time_spent_on_step_ms: int | None = None
    total_application_duration_ms: int | None = None
    recommended_job_ids: list[str] = []
    client_metadata: dict = {}


class Data(msgspec.Struct, omit_defaults=True):
    candidate: CandidateMetadata
    event_payload: EventPayload


class CloudEvent(msgspec.Struct, omit_defaults=True, forbid_unknown_fields=True):
    """A CloudEvent. `type` is an enum, so msgspec rejects unknown types.

    Extension attribute names carry no underscores: CloudEvents requires
    lower-case [a-z0-9] only (r3 error E2).
    """

    specversion: SpecVersion
    id: str
    source: str
    type: EventType
    time: str
    data: Data
    subject: str | None = None
    dataschema: str | None = None
    datacontenttype: str = "application/json"
    keyversion: int | None = None
    sequence: str | None = None
    sourcechannel: SourceChannel | None = None
    referrertype: ReferrerType | None = None
    completionmethod: CompletionMethod | None = None
    #: Accepted ONLY so a disagreeing client value can be detected and rejected
    #: with 403. It is NEVER used for routing: the gateway derives the Kafka key
    #: from the JWT and the pseudonymous user id (plan C2). The CloudEvents
    #: partitioning extension itself notes the value "might change, or even be
    #: removed" across hops -- a hint, not a guarantee.
    partitionkey: str | None = None


_DECODER = msgspec.json.Decoder(type=list[CloudEvent])


def decode_batch(raw: bytes | list | dict) -> list[CloudEvent]:
    """Decode a CloudEvents JSON batch (`application/cloudevents-batch+json`).

    One msgspec call for the whole batch: parse and validation in a single
    native pass, which is why msgspec rather than Pydantic or jsonschema.
    """
    if isinstance(raw, (list, dict)):
        raw = msgspec.json.encode(raw)
    return _DECODER.decode(raw)


def generate_schema_json() -> str:
    """JSON Schema for the envelope, generated from the structs.

    msgspec has no pretty-printer for a schema object, and this is a build step
    run by `python -m contracts.gen_schema`, not a hot path -- so stdlib json is
    the right tool here. The hot path is `decode_batch`, which is msgspec.
    """
    schemas, _components = msgspec.json.schema_components(
        [CloudEvent], ref_template="#/$defs/{name}"
    )
    return json.dumps(schemas[0], indent=2)
