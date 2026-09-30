"""T8a driver: the funnel state machine.

Random events produce random dashboards. This models the real journeys the
pipeline exists to measure: view a job, start an application, work through the
steps, then submit / save-for-later / walk away.

Deliberately does NOT emit APPLICATION_ABANDONED. The Queue team's Flink job
synthesises it on the watermark timeout (SPEC.txt:338-339). Emitting it here too
would double-count the drop-off metric.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from typing import Literal

from driver.sources import client_metadata_for

SUBMITTED = "SUBMITTED"
DRAFT_SAVED = "DRAFT_SAVED"
ABANDONED = "ABANDONED"

Outcome = Literal["SUBMITTED", "DRAFT_SAVED", "ABANDONED"]

TYPE_JOB_VIEWED = "com.careerpage.career.job-viewed"
TYPE_JOB_WISHLISTED = "com.careerpage.career.job-wishlisted"
TYPE_APPLICATION_STARTED = "com.careerpage.career.application-started"
TYPE_STEP_COMPLETED = "com.careerpage.career.application-step-completed"
TYPE_DRAFT_SAVED = "com.careerpage.career.application-draft-saved"
TYPE_APPLICATION_SUBMITTED = "com.careerpage.career.application-submitted"

STEP_NAMES = ["PERSONAL_DETAILS", "WORK_EXPERIENCE", "EDUCATION", "SKILLS", "REVIEW"]
COMPLETION_METHODS = ["MANUAL", "RESUME_AUTOFILL", "HYBRID"]
REFERRERS = ["SEARCH", "RECOMMENDATION", "DIRECT"]

#: CloudEvents events/sec is a 500-tenant system. A session is a handful of
#: events, so a per-tenant arrival rate is meaningful only over a window.
_SEQ_WIDTH = 10


@dataclass(frozen=True, slots=True)
class SessionConfig:
    career_site_id: str
    user_pseudo: str
    source_channel: str
    job_id: str
    steps: int = 3
    outcome: Outcome = SUBMITTED
    start_sequence: int = 0
    referrer: str = "SEARCH"
    rng: random.Random = field(default_factory=random.Random)

    @property
    def source(self) -> str:
        return f"/careers/{self.career_site_id}"


def _event(
    cfg: SessionConfig,
    seq: int,
    event_type: str,
    *,
    subject: str,
    payload: dict,
    **attrs,
) -> dict:
    """One CloudEvent. `sequence` is zero-padded so it sorts lexicographically,
    which is what the CloudEvents sequence extension requires."""
    ev = {
        "specversion": "1.0",
        "id": f"01J{uuid.uuid4().hex[:24].upper()}",
        "source": cfg.source,
        "type": event_type,
        "time": "2026-09-30T14:43:09.123Z",
        "subject": subject,
        "dataschema": "https://schema.careerpage.example/event/1.0",
        "datacontenttype": "application/json",
        "keyversion": 1,
        "sequence": str(seq).zfill(_SEQ_WIDTH),
        "sourcechannel": cfg.source_channel,
        "data": {
            "candidate": {
                "user_id_pseudo": cfg.user_pseudo,
                "email_hmac": uuid.uuid4().hex[:16],
                "email_enc": "Y2lwaGVydGV4dA==",
                "phone_enc": "cGhvbmUtY2lwaGVy",
                "alternate_phone_enc": "YWx0LWNpcGhlcg==",
                "name_enc": "bmFtZS1jaXBoZXI=",
                "gender_enc": "Z2VuZGVyLWNpcGhlcg==",
                "experience_status": "EXPERIENCED",
                "years_of_experience": 3.5,
                "education_degree": "B.Tech",
                "education_branch": "E&E",
            },
            "event_payload": payload,
        },
    }
    ev.update(attrs)
    return ev


def generate_session(cfg: SessionConfig) -> list[dict]:
    """One coherent session: a valid journey, contiguous in `sequence`."""
    session_id = f"sess_{uuid.uuid4().hex[:16]}"
    seq = cfg.start_sequence
    events: list[dict] = []

    def base_payload(**kw) -> dict:
        p = {
            "job_id": cfg.job_id,
            "session_id": session_id,
            "client_metadata": client_metadata_for(cfg.source_channel, cfg.rng),
        }
        p.update(kw)
        return p

    events.append(
        _event(
            cfg, seq, TYPE_JOB_VIEWED, subject=cfg.job_id,
            payload=base_payload(), referrertype=cfg.referrer,
        )
    )
    seq += 1

    if cfg.rng.random() < 0.35:
        events.append(
            _event(cfg, seq, TYPE_JOB_WISHLISTED, subject=cfg.job_id, payload=base_payload())
        )
        seq += 1

    started_payload = base_payload()
    if cfg.referrer == "RECOMMENDATION":
        started_payload["recommended_job_ids"] = [
            f"job_{uuid.uuid4().hex[:8]}" for _ in range(cfg.rng.randint(1, 3))
        ]
    events.append(
        _event(
            cfg, seq, TYPE_APPLICATION_STARTED, subject=cfg.job_id,
            payload=started_payload, referrertype=cfg.referrer,
        )
    )
    seq += 1

    elapsed = 0
    for i in range(cfg.steps):
        spent = cfg.rng.randint(3_000, 90_000)
        elapsed += spent
        events.append(
            _event(
                cfg, seq, TYPE_STEP_COMPLETED, subject=cfg.job_id,
                payload=base_payload(
                    step_number=i + 1,
                    step_name=STEP_NAMES[i % len(STEP_NAMES)],
                    action="NEXT",
                    completion_method=cfg.rng.choice(COMPLETION_METHODS),
                    time_spent_on_step_ms=spent,
                    total_application_duration_ms=elapsed,
                ),
                completionmethod=cfg.rng.choice(COMPLETION_METHODS),
            )
        )
        seq += 1

    # ABANDONED means: no terminal event. Flink synthesises the abandonment.
    if cfg.outcome == SUBMITTED:
        events.append(
            _event(
                cfg, seq, TYPE_APPLICATION_SUBMITTED, subject=cfg.job_id,
                payload=base_payload(
                    step_number=cfg.steps + 1, action="SUBMIT",
                    completion_method=cfg.rng.choice(COMPLETION_METHODS),
                    total_application_duration_ms=elapsed,
                ),
                completionmethod=cfg.rng.choice(COMPLETION_METHODS),
            )
        )
    elif cfg.outcome == DRAFT_SAVED:
        events.append(
            _event(
                cfg, seq, TYPE_DRAFT_SAVED, subject=cfg.job_id,
                payload=base_payload(
                    step_number=cfg.steps, action="SAVE_AND_APPLY_LATER",
                    time_spent_on_step_ms=elapsed,
                ),
            )
        )
    return events


def session_survival(
    sessions: int, *, drop_off_rate: float, steps: int
) -> list[int]:
    """How many sessions reach each funnel step. Monotonically non-increasing,
    so a generated corpus produces a plausible drop-off curve rather than noise."""
    rng = random.Random(0xC0FFEE)
    counts = [sessions]
    for _ in range(steps):
        # Jitter scales WITH the drop-off rate, so a zero drop-off rate is
        # exactly flat rather than losing sessions to noise.
        spread = 0.06 * drop_off_rate
        nxt = int(round(counts[-1] * (1.0 - drop_off_rate) * (1 + rng.uniform(-spread, spread))))
        counts.append(min(nxt, counts[-1]))
    return counts
