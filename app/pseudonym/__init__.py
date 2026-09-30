"""T6: pseudonymous twins of raw identifiers (plan D5).

`user_id_pseudo` and `email_hmac` are HMACs so the analytics path can still
group and join, while the raw identifier never leaves the process.
"""

from __future__ import annotations

from app.pseudonym.hmac import HMAC_ALGORITHM, pseudonymize

__all__ = ["HMAC_ALGORITHM", "pseudonymize"]
