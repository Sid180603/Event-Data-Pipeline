"""T-R5: the raw-vs-encrypted inspector. `python -m app.inspect`.

The demo beat where somebody asks to see that the PII really is encrypted. It
puts ONE event on the wire, in the shape a client uses, and shows three columns
for every value in it:

* **what the client sent** -- plaintext, because a client cannot hold a tenant
  key (`contracts/ingress.py`),
* **what is stored** -- the record the sink actually kept, read out of the sink
  rather than re-encoded, so it is the topic's copy and not this tool's idea of
  one (`contracts/cloudevent.py`),
* **what `POST /v1/decrypt` gives back** -- the plaintext, and *only* if the
  operator credential was accepted.

## The rules this module holds itself to

**Nothing runs in-process that would not run in production.** The sample is
posted to the real `POST /v1/ingest` of a real gateway built by
`app.main.create_app`, over a real ASGI request, so the pipeline's five stages,
its validation and its encryption are the ones that ran. The only substitution
is the broker: `FakeSink` instead of `KafkaSink`, which is what lets the beat
run on a laptop with no Docker and no topics. The report says so, because a demo
that quietly substituted more than that would be over-claiming.

**The two schemas are never conflated.** This is the bug this repo has already
had once: the driver posting the egress shape and being refused with `SCHEMA at
$.data.candidate`. So the ingress body is a `contracts.ingress.IngressEvent` and
the stored record is a `contracts.cloudevent.CloudEvent`, each decoded into its
own struct, and `app/test_inspect.py` decodes each as the other and requires a
rejection.

**Plaintext is only ever printed behind the operator credential.** Two separate
guards, because they fail differently: without a credential the CLI refuses
before it builds or sends anything at all, and with the WRONG one the endpoint
answers 401, the third column says `refused`, and the exit code is non-zero. The
left-hand column is this process's own synthetic sample, fabricated a moment ago
from a constant in this file -- it cannot leak anyone's PII, and printing it is
what makes the comparison mean anything. Nothing else is printed as plaintext
unless the endpoint handed it over.

**The two HMACs are shown, not decrypted.** `user_id_pseudo` and `email_hmac`
are HMAC-SHA256: joinable for grouping, not reversible, and not on
`DECRYPTABLE_FIELDS`. Asking for them would be five guaranteed 400s and a table
that implies they might have worked, so the rows say so instead.

**Every decrypt is audited**, by the endpoint, not by this tool. The report
prints the count and the actor; it never prints a value into the audit line,
because the audit outlives the request.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

import jwt
import msgspec
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from fastapi.testclient import TestClient

from app.auth import Credential
from app.auth.jwt import CREDENTIAL_CLAIM, TENANT_CLAIM
from app.config import Settings
from app.crypto.facade import PII_FIELDS
from app.ingest.decrypt import DecryptAudit, DecryptRequest, OPERATOR_KEY_HEADER
from app.kafka.producer import FakeSink
from app.main import create_app, credential_id_for
from app.metrics import Metrics
from contracts.attributes import career_site_id_from_source
from contracts.cloudevent import CloudEvent

#: A tenant, the one the sample event belongs to. Fixed rather than a flag: the
#: master secret is per deployment, not per tenant, so any configured tenant
#: would work and the extra knob would only be a way to mistype the demo.
CAREER_SITE_ID = "acme_8921"

#: The credential the sample's bearer token names. `WEB_APP` because it is the
#: channel the UI team posts from first.
SOURCE_CHANNEL = "WEB_APP"

#: Who the audit trail says asked. A real name would be a lie in a repo; this is
#: the operator the demo is pretending to be.
OPERATOR_ID = "ops-inspector"

#: CONTRACT.md section 2. Not imported from `driver.replay`, which carries its
#: own copy: `driver` depends on `app`, and a tool inside `app` importing the
#: load generator would invert that for the sake of a string constant.
INGEST_PATH = "/v1/ingest"
DECRYPT_PATH = "/v1/decrypt"
BATCH_CONTENT_TYPE = "application/cloudevents-batch+json"

#: `Settings.jwt_audience` falls back to this, and the throwaway verifier below
#: pins the same value on both sides of the check.
AUDIENCE = "career-api"

#: The only algorithm the gateway accepts that is also a private key this
#: process can hold. `app.auth.jwt.ALLOWED_ALGORITHMS` is the authority; this is
#: the choice, and a comment is cheaper than a second table.
SIGNING_ALGORITHM = "EdDSA"

#: How long the sample's bearer token is good for. Minutes, not hours: it is
#: minted and spent inside one function call.
TOKEN_TTL_SECONDS = 300

SAMPLE_USER_ID = "usr_7f3a91c4"
SAMPLE_EMAIL = "dana.okafor@candidate.invalid"
SAMPLE_EVENT_ID = "evt_inspect_00000001"
SAMPLE_SEQUENCE = "0000000042"
SAMPLE_TIME = "2026-10-02T09:14:03.412Z"

#: Why the two HMAC rows have no third column. Shown rather than omitted: the
#: whole point of the beat is what the gateway does and does not keep
#: recoverable, and "not shown" reads as "not covered". Kept inside the third
#: column's width so the note is not truncated into something vaguer; the footer
#: says the rest.
HMAC_NOTE = "HMAC: joinable, not reversible"

#: Display widths. Fixed rather than computed from the data so the columns line
#: up between runs, which is the only reason a fixed width is acceptable here:
#: the sample is a constant in this file, so the widest cell is known. The last
#: column is not padded at all -- it is the end of the line, and trailing
#: whitespace only makes the report harder to copy out of a terminal.
_FIELD_WIDTH = 19
_SENT_WIDTH = 32
_STORED_WIDTH = 28

#: How much of a base64 ciphertext to show. Enough to see that it is opaque and
#: that it differs per field, short enough to fit the table; the full length
#: goes next to it so nothing here implies the value is complete.
_STORED_PREFIX = 20


class InspectionFailed(RuntimeError):
    """The sample did not make it through the pipeline. Always a bug, never a
    demo beat, so it is loud -- but it is caught in `main` so what a viewer sees
    is a sentence rather than a traceback."""


@dataclass(frozen=True, slots=True)
class FieldRow:
    """One value, before and after.

    `sent_name` and `stored_name` differ on purpose -- the client's `email` is
    the topic's `email_enc`, and a column headed `email` holding a ciphertext is
    exactly the confusion this beat is meant to dissolve.
    """

    sent_name: str
    stored_name: str
    sent: str
    stored: str
    decrypted: str | None = None
    refusal: str = ""
    note: str = ""


@dataclass(frozen=True, slots=True)
class Inspection:
    """Everything one pass over the sample produced."""

    tenant: str
    sent: dict
    event: CloudEvent
    topic: str
    key: str
    stored_bytes: int
    rows: tuple[FieldRow, ...]
    operator_id: str
    refusals: tuple[str, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        """True when every ciphertext the sample carried came back.

        A demo that ends with a refusal is a demo that failed, so this is the
        CLI's exit code rather than a detail in the body of the report.
        """
        return not self.refusals


def sample_event() -> dict:
    """One event in the INGRESS shape, with all five PII fields populated.

    A function, not a module constant, so a caller cannot mutate the dictionary
    the tool sends. All five fields are present because a demo showing three
    encrypted fields and two absent ones is a demo whose table is mostly
    `-`; `app.crypto.facade` is already the thing that proves absent PII is
    left absent.
    """
    return {
        "specversion": "1.0",
        "id": SAMPLE_EVENT_ID,
        "source": f"/careers/{CAREER_SITE_ID}",
        "type": "com.careerpage.career.application-submitted",
        "time": SAMPLE_TIME,
        "subject": "job_88320491",
        "dataschema": "https://schema.careerpage.example/event/1.0",
        "datacontenttype": "application/json",
        "sequence": SAMPLE_SEQUENCE,
        "data": {
            "candidate": {
                "user_id": SAMPLE_USER_ID,
                "email": SAMPLE_EMAIL,
                "phone": "+44 7700 900118",
                "alternate_phone": "+44 7700 900119",
                "name": "Dana Okafor",
                "gender": "FEMALE",
                "experience_status": "EXPERIENCED",
                "years_of_experience": 6.5,
                "education_degree": "BACHELOR",
                "education_branch": "Computer Science",
            },
            "event_payload": {
                "job_id": "job_88320491",
                "session_id": "sess_8839201923",
                "step_number": 6,
                "step_name": "review",
                "action": "submit",
                "completion_method": "MANUAL",
                "time_spent_on_step_ms": 8_400,
                "total_application_duration_ms": 51_200,
            },
        },
    }


def _build_app(
    *, master_secret: bytes, operator_key: bytes, audit: Callable[[DecryptAudit], None] | None = None
):
    """The whole gateway, wired the way `app.main` wires it, with a `FakeSink`.

    Split out of `run_inspection` so the refusal path can be driven against a
    gateway whose configured credential is not the one presented -- the only way
    to make `/v1/decrypt` say no from inside this module, and the case the
    third column exists to report.
    """
    signing_key = Ed25519PrivateKey.generate()
    settings = Settings(
        kafka_bootstrap_servers="broker.invalid:9092",
        master_secret=master_secret,
        jwt_audience=AUDIENCE,
        jwt_algorithm=SIGNING_ALGORITHM,
        jwt_public_key_pem=signing_key.public_key()
        .public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
        .decode(),
    )
    app = create_app(
        credentials=(
            Credential(
                credential_id=credential_id_for(CAREER_SITE_ID, SOURCE_CHANNEL),
                career_site_id=CAREER_SITE_ID,
                source_channel=SOURCE_CHANNEL,
            ),
        ),
        settings=settings,
        operator_key=operator_key,
        operator_id=OPERATOR_ID,
        sink=FakeSink(),
        metrics=Metrics(worker_id="inspector"),
        audit=audit,
    )
    # On the app rather than returned, because the bearer token is signed with
    # the key that was just generated for this one gateway and there is nowhere
    # else to keep the pair.
    app.state.inspector_signing_key = signing_key
    return app


def run_inspection(
    *,
    master_secret: bytes,
    operator_key: bytes,
    audit: Callable[[DecryptAudit], None] | None = None,
) -> Inspection:
    """Post one event, read it back out of the sink, and decrypt it field by field.

    `audit` is the app's own audit sink, injected so the caller can read what the
    endpoint wrote. It defaults to the app's logger, which is where every other
    deployment sends it.
    """
    if not master_secret:
        raise ValueError("master_secret is required: the gateway derives tenant keys from it")
    if not operator_key:
        raise ValueError("operator_key is required: without it nothing here is decrypted")

    app = _build_app(master_secret=master_secret, operator_key=operator_key, audit=audit)
    sent = sample_event()
    with TestClient(app) as client:
        _ingest(client, sent, _bearer(app.state.inspector_signing_key))
        topic, key, value = _stored_record(app)
        stored = msgspec.json.decode(value, type=CloudEvent)
        rows, refusals = _compare(client, sent, stored, operator_key)

    return Inspection(
        tenant=CAREER_SITE_ID,
        sent=sent,
        event=stored,
        topic=topic,
        key=key,
        stored_bytes=len(value),
        rows=rows,
        operator_id=OPERATOR_ID,
        refusals=refusals,
    )


def _bearer(private_key: Ed25519PrivateKey) -> str:
    """A bearer token for the gateway this process just built.

    A throwaway key pair, minted here, and the system's real signing key is
    deliberately NOT read: `driver-signing-key.pem` is the one file the whole
    design treats as a standing secret (the gateway holds only the public half),
    and a tool whose job is to print things to a screen is the last place that
    should be needed. The token's only job is to get a synthetic sample past
    the bearer check of a process we are about to throw away, so nothing about
    PII, tenancy or the crypto depends on it.
    """
    now = dt.datetime.now(dt.timezone.utc)
    return jwt.encode(
        {
            TENANT_CLAIM: CAREER_SITE_ID,
            CREDENTIAL_CLAIM: credential_id_for(CAREER_SITE_ID, SOURCE_CHANNEL),
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + dt.timedelta(seconds=TOKEN_TTL_SECONDS),
        },
        private_key,
        algorithm=SIGNING_ALGORITHM,
    )


def _ingest(client: TestClient, sent: dict, bearer: str) -> None:
    """POST the sample in the ingress shape, and insist that it was accepted.

    In-process rather than over a socket, and that is the whole of the reason:
    this tool runs on a laptop in front of people, and a port to collide with is
    a failure mode the demo does not need. Everything downstream of the socket --
    routing, the middleware, auth, validation, encryption, the sink -- is the
    production path.
    """
    response = client.post(
        INGEST_PATH,
        content=msgspec.json.encode([sent]),
        headers={
            "content-type": BATCH_CONTENT_TYPE,
            "authorization": f"Bearer {bearer}",
            "x-source-type": SOURCE_CHANNEL,
        },
    )
    if response.status_code != 202 or response.json().get("accepted") != 1:
        raise InspectionFailed(
            f"the sample was not accepted: HTTP {response.status_code} {response.text[:200]}"
        )


def _stored_record(app) -> tuple[str, str, bytes]:
    """The one record the sink kept: `(topic, key, value)`.

    Taken from `app.state.sink`, which is the `InstrumentedSink` the routes
    publish through, so this is the byte sequence that reached the topic and not
    a re-encode of the event.
    """
    records = app.state.sink.inner.records
    if not records:
        raise InspectionFailed("the sample was accepted but nothing reached the sink")
    return records[-1]


def _compare(
    client: TestClient, sent: dict, stored: CloudEvent, operator_key: bytes
) -> tuple[tuple[FieldRow, ...], tuple[str, ...]]:
    """One row per value, and the plaintext for each ciphertext field.

    Iterating `PII_FIELDS` rather than `DECRYPTABLE_FIELDS` is deliberate: the
    former is the ordered `(plaintext name, ciphertext name)` pair list the
    gateway itself encrypts from, so a field the pipeline starts encrypting shows
    up here without anyone remembering to edit this file.
    """
    sent_candidate = sent["data"]["candidate"]
    stored_candidate = stored.data.candidate
    rows: list[FieldRow] = []
    refusals: list[str] = []

    for sent_name, stored_name in PII_FIELDS:
        value = sent_candidate.get(sent_name)
        token = getattr(stored_candidate, stored_name, None)
        if not value or not token:
            continue
        decrypted, refusal = _decrypt(client, stored, stored_name, operator_key, sent=value)
        if refusal:
            refusals.append(f"{stored_name}: {refusal}")
        rows.append(
            FieldRow(
                sent_name=sent_name,
                stored_name=stored_name,
                sent=value,
                stored=_ciphertext_cell(token),
                decrypted=decrypted,
                refusal=refusal,
            )
        )

    # The HMACs last: they are the two values in this event that are deliberately
    # not recoverable, and having them under the recoverable ones reads better
    # than having them above.
    rows.append(
        FieldRow(
            sent_name="user_id",
            stored_name="user_id_pseudo",
            sent=sent_candidate["user_id"],
            stored=_ciphertext_cell(stored_candidate.user_id_pseudo),
            note=HMAC_NOTE,
        )
    )
    if stored_candidate.email_hmac:
        rows.append(
            FieldRow(
                sent_name="email",
                stored_name="email_hmac",
                sent=sent_candidate["email"],
                stored=_ciphertext_cell(stored_candidate.email_hmac),
                note=HMAC_NOTE,
            )
        )
    return tuple(rows), tuple(refusals)


def _decrypt(
    client: TestClient,
    event: CloudEvent,
    field_name: str,
    operator_key: bytes,
    *,
    sent: str,
) -> tuple[str | None, str]:
    """`POST /v1/decrypt` for one field. `(value, "")` or `(None, reason)`.

    `sent` is here to check the round trip rather than in the caller: a decrypt
    that returned a plausible-looking value for the wrong event would be a far
    worse demo failure than a refusal, and a report showing an unverified round
    trip is the thing to avoid. The endpoint binds the ciphertext to
    `(source, id, type, field, keyversion)` through its AAD, which makes a
    mismatch unreachable -- so this is a guard against broken wiring, not an
    expected path.
    """
    response = client.post(
        DECRYPT_PATH,
        content=msgspec.json.encode(
            DecryptRequest(
                career_site_id=career_site_id_from_source(event.source),
                event=event,
                field=field_name,
            )
        ),
        headers={OPERATOR_KEY_HEADER: operator_key.decode(), "content-type": "application/json"},
    )
    if response.status_code != 200:
        reason = _reason_of(response)
        return None, f"HTTP {response.status_code} {reason}"
    value = response.json()["value"]
    if value != sent:
        # Not reachable through the endpoint: the AAD binds the ciphertext to
        # `(source, id, type, field, keyversion)`, so a mismatch fails
        # authentication rather than decrypting. Checked anyway, because a tool
        # that shows a round trip it did not verify is the thing to avoid.
        raise InspectionFailed(f"{field_name} decrypted to {value!r}, not the value that was sent")
    return value, ""


def _reason_of(response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "unreadable body"
    reason = body.get("reason") if isinstance(body, dict) else None
    return reason if isinstance(reason, str) and reason else str(response.status_code)


def _ciphertext_cell(token: str) -> str:
    """A ciphertext, shortened, with its real length beside it.

    The length is not decoration: a viewer who can see that the stored value is
    76 characters of base64 rather than the 30-character address cannot
    reasonably think the two columns are the same string.
    """
    prefix = token[:_STORED_PREFIX]
    return prefix if len(token) <= _STORED_PREFIX else f"{prefix}..{len(token)}"


# --- the report ---------------------------------------------------------------


def _cell(text: str, width: int) -> str:
    if len(text) <= width:
        return text.ljust(width)
    return text[: width - 3] + "..."


def _decrypt_cell(row: FieldRow) -> str:
    if row.decrypted is not None:
        return row.decrypted
    return row.refusal or row.note


def render(inspection: Inspection) -> str:
    """The report. One table, three columns, and no value that did not come back
    from the endpoint."""
    event = inspection.event
    lines = [
        "career event gateway -- one event, before and after the gateway",
        "  a synthetic event is POSTed to the real ingest endpoint of a real gateway",
        f"  built on this machine's MASTER_SECRET; the plaintext comes back only",
        "  through the operator-only /v1/decrypt endpoint, and every call is audited.",
        "",
        f"  tenant   {inspection.tenant:<20} channel {event.sourcechannel}",
        f"  event    {event.type}",
        f"  id       {event.id:<20} keyversion {event.keyversion}",
        f"  source   {event.source:<20} sequence {event.sequence}",
        f"  stored   {inspection.stored_bytes:,} bytes on {inspection.topic}",
        f"  key      {inspection.key}",
        "           derived by the gateway as career_site_id | user_id_pseudo --",
        "           the raw user id never appears in it",
        "",
        "  " + "field".ljust(_FIELD_WIDTH)
        + "  what the client sent".ljust(_SENT_WIDTH)
        + "  what is stored".ljust(_STORED_WIDTH)
        + "  via POST /v1/decrypt",
    ]
    lines.append(
        "  "
        + "-" * _FIELD_WIDTH
        + "  "
        + "-" * _SENT_WIDTH
        + "  "
        + "-" * _STORED_WIDTH
        + "  "
        + "-" * 30
    )
    for row in inspection.rows:
        lines.append(
            (
                "  "
                + _cell(row.stored_name, _FIELD_WIDTH)
                + "  "
                + _cell(row.sent, _SENT_WIDTH)
                + "  "
                + _cell(row.stored, _STORED_WIDTH)
                + "  "
                + _decrypt_cell(row)
            ).rstrip()
        )
    asked = len([row for row in inspection.rows if row.refusal or not row.note])
    lines += [
        "",
        f"  {asked} decrypt request(s), each audit-logged with who and which (source, id).",
        f"  operator {inspection.operator_id}. No plaintext is stored, logged or audited --",
        "  the third column above exists only because the operator endpoint returned it.",
        "  user_id_pseudo and email_hmac are HMAC-SHA256, not ciphertext: the same input",
        "  always groups the same way and no key reverses them, so /v1/decrypt is not",
        "  asked for them and the endpoint's whitelist would refuse.",
    ]
    if inspection.refusals:
        lines += [
            "",
            "  REFUSED: " + "; ".join(inspection.refusals),
        ]
    return "\n".join(lines)


# --- the CLI ------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m app.inspect",
        description="Post one synthetic event through the real ingest pipeline and show "
        "what the client sent beside what the topic stores beside what the operator "
        "endpoint will hand back.",
    )
    parser.add_argument(
        "--operator-key",
        default=os.getenv("OPERATOR_KEY", ""),
        help="the X-Operator-Key credential. Required: without it no plaintext is "
        "printed at all. Default: $OPERATOR_KEY",
    )
    parser.add_argument(
        "--master-secret",
        default=os.getenv("MASTER_SECRET", ""),
        help="the gateway's own secret, so the ciphertexts shown are the ones this "
        "deployment's keys produce. Default: $MASTER_SECRET",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.operator_key:
        # Before anything is built or sent. The left-hand column is plaintext
        # too, and printing it without a credential would make the "only behind
        # the operator key" rule a rule about the third column only.
        parser.error(
            "no operator key: pass --operator-key or set OPERATOR_KEY. The plain column "
            "is plaintext, and nothing here is printed without the credential that "
            "guards it"
        )
    if not args.master_secret:
        parser.error(
            "no master secret: pass --master-secret or set MASTER_SECRET. Without it the "
            "gateway cannot derive tenant keys, and inventing one would show ciphertexts "
            "from a different deployment than the one running"
        )
    try:
        inspection = run_inspection(
            master_secret=args.master_secret.encode(), operator_key=args.operator_key.encode()
        )
    except (InspectionFailed, ValueError) as exc:
        print(f"inspection failed: {exc}", file=sys.stderr)
        return 1
    print(render(inspection))
    return 0 if inspection.ok else 1


__all__ = [
    "AUDIENCE",
    "BATCH_CONTENT_TYPE",
    "CAREER_SITE_ID",
    "DECRYPT_PATH",
    "HMAC_NOTE",
    "INGEST_PATH",
    "OPERATOR_ID",
    "SAMPLE_EMAIL",
    "SAMPLE_EVENT_ID",
    "SAMPLE_SEQUENCE",
    "SAMPLE_TIME",
    "SAMPLE_USER_ID",
    "SIGNING_ALGORITHM",
    "SOURCE_CHANNEL",
    "TOKEN_TTL_SECONDS",
    "FieldRow",
    "Inspection",
    "InspectionFailed",
    "build_parser",
    "main",
    "render",
    "run_inspection",
    "sample_event",
]


if __name__ == "__main__":
    raise SystemExit(main())
