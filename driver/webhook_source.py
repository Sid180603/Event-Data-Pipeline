"""T8b: the third-party webhook source.

A partner job board does not walk the web funnel. It posts one completed
application per call, with its own attribution fields, and the pipeline has to
carry that provenance through to the dashboard rather than attributing it to a
browser search that never happened.

So the partner's inbound shape is a first-class, separately validated contract
(`PartnerWebhook`, `forbid_unknown_fields`, per the additive-only policy in
CONTRACT.md) and it is translated into the ordinary pipeline envelope instead of
being faked as a web session.
"""

from __future__ import annotations

import random

import msgspec

from driver.fsm import SUBMITTED, SessionConfig, generate_session

THIRD_PARTY_CHANNEL = "THIRD_PARTY_SERVICE"
THIRD_PARTY_REFERRER = "THIRD_PARTY_WEBHOOK"

_PARTNER_HOSTS = ["boards.example.com", "jobs.example.org", "partner-careers.net"]
_PARTNERS = ["jobboard-eu", "talent-net", "indeed-mirror", "referral-co"]
_UTM = [("linkedin", "cpc"), ("google", "cpc"), ("newsletter", "email"), ("indeed", "referral")]


class PartnerApplicant(msgspec.Struct, forbid_unknown_fields=True):
    """The partner's own candidate, before the gateway pseudonymises it."""

    candidate_ref: str
    email_hmac: str | None = None
    email_enc: str | None = None
    phone_enc: str | None = None
    name_enc: str | None = None


class PartnerWebhook(msgspec.Struct, forbid_unknown_fields=True):
    """One application posted by a partner job board.

    The partner's `candidate_ref` is resolved to the tenant's `user_id_pseudo`
    by the gateway's HMAC before anything is partitioned. That matters for
    sticky routing: minting a fresh identity per partner candidate would give a
    hiring drive thousands of single-event partitions and defeat the ordering
    the key exists to provide.
    """

    partner: str
    external_application_id: str
    career_site_id: str
    job_id: str
    referrer_url: str
    utm_source: str
    utm_medium: str
    occurred_at: str
    applicant: PartnerApplicant


_DECODER = msgspec.json.Decoder(PartnerWebhook)


def parse_webhook(raw: bytes | dict) -> PartnerWebhook:
    """Validate a partner's inbound payload. Raises on a bad or unknown field."""
    if isinstance(raw, dict):
        raw = msgspec.json.encode(raw)
    return _DECODER.decode(raw)


def webhook_payload(rng: random.Random, career_site_id: str, job_id: str) -> PartnerWebhook:
    """A plausible partner application, for generating traffic."""
    host = rng.choice(_PARTNER_HOSTS)
    source, medium = rng.choice(_UTM)
    return PartnerWebhook(
        partner=rng.choice(_PARTNERS),
        external_application_id=f"ext_{rng.randrange(1 << 30):08x}",
        career_site_id=career_site_id,
        job_id=job_id,
        referrer_url=f"https://{host}/jobs/{rng.randint(10_000, 99_999)}",
        utm_source=source,
        utm_medium=medium,
        occurred_at="2026-09-30T14:43:09.123Z",
        applicant=PartnerApplicant(
            candidate_ref=f"cand_{rng.randrange(1 << 30):08x}",
            email_hmac=f"{rng.randrange(1 << 32):08x}",
            email_enc="Y2lwaGVydGV4dA==",
            phone_enc="cGhvbmUtY2lwaGVy",
            name_enc="bmFtZS1jaXBoZXI=",
        ),
    )


def webhook_events(
    payload: PartnerWebhook,
    *,
    user_id_pseudo: str,
    start_sequence: int,
    rng: random.Random,
) -> list[dict]:
    """Translate a partner application into pipeline CloudEvents.

    Modelled as a one-step, already-submitted session: the partner posts the
    finished application, so there is no abandonment window for the Flink
    watermark to fire on. The FSM's own event set is reused rather than
    reimplemented, which keeps the funnel, the `sequence` extension and the
    envelope in one place. The FSM's optional wishlist event is the one piece
    that does not fit a partner post; it is a 35% coin flip inside
    `generate_session` and is not worth forking the FSM to suppress.
    """
    events = generate_session(
        SessionConfig(
            career_site_id=payload.career_site_id,
            user_pseudo=user_id_pseudo,
            source_channel=THIRD_PARTY_CHANNEL,
            job_id=payload.job_id,
            steps=1,
            outcome=SUBMITTED,
            start_sequence=start_sequence,
            referrer=THIRD_PARTY_REFERRER,
            rng=rng,
        )
    )
    # The partner's own UTM replaces the generic pool: attribution a partner
    # paid for is data, not decoration, and it must survive the hop.
    metadata = {
        "referrer_url": payload.referrer_url,
        "utm_source": payload.utm_source,
        "utm_medium": payload.utm_medium,
    }
    for ev in events:
        # The FSM only tags the events that browse; a partner post has one
        # referrer for the whole application, so stamp them all.
        ev["referrertype"] = THIRD_PARTY_REFERRER
        ev["data"]["event_payload"]["client_metadata"] = dict(metadata)
    return events


__all__ = [
    "PartnerApplicant",
    "PartnerWebhook",
    "THIRD_PARTY_CHANNEL",
    "THIRD_PARTY_REFERRER",
    "parse_webhook",
    "webhook_events",
    "webhook_payload",
]
