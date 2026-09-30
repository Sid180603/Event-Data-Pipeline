"""Per-source payload shapes.

"Events come from multiple sources" is a stated requirement, and a single shape
would not exercise the `sourcechannel` attribute or prove the attribution story
(SPEC.txt #4/#5/#6).
"""

from __future__ import annotations

import random

_USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/118.0 Safari/537.36",
]
_LOCALES = ["en-US", "en-GB", "de-DE", "fr-FR", "en-IN"]
_PAGES = ["/", "/jobs", "/jobs/88320491", "/search", "/careers"]

_APPS = [
    {"name": "CareerApp", "version": "2.4.0", "build": "3.0.1.245"},
    {"name": "CareerApp", "version": "2.3.9", "build": "3.0.0.188"},
]
_DEVICES = [
    {"manufacturer": "Apple", "model": "iPhone15,3", "type": "ios"},
    {"manufacturer": "Google", "model": "Pixel 8", "type": "android"},
    {"manufacturer": "Samsung", "model": "SM-S918B", "type": "android"},
]
_SCREENS = [
    {"width": 390, "height": 844, "density": 3},
    {"width": 412, "height": 915, "density": 2.625},
    {"width": 360, "height": 800, "density": 2},
]

#: Third-party webhook shape. T8b; defined here so the corpus can opt in.
_REFERRER_HOSTS = ["boards.example.com", "jobs.example.org", "partner-careers.net"]
_UTM = [("linkedin", "cpc"), ("google", "cpc"), ("newsletter", "email"), ("indeed", "referral")]


def web_metadata(rng: random.Random) -> dict:
    return {
        "userAgent": rng.choice(_USER_AGENTS),
        "page": rng.choice(_PAGES),
        "locale": rng.choice(_LOCALES),
        "network": {"wifi": rng.random() > 0.2, "cellular": rng.random() > 0.8},
    }


def mobile_metadata(rng: random.Random) -> dict:
    return {
        "app": dict(rng.choice(_APPS)),
        "device": dict(rng.choice(_DEVICES)),
        "screen": dict(rng.choice(_SCREENS)),
        "locale": rng.choice(_LOCALES),
    }


def webhook_metadata(rng: random.Random) -> dict:
    source, medium = rng.choice(_UTM)
    host = rng.choice(_REFERRER_HOSTS)
    return {
        "referrer_url": f"https://{host}/jobs/{rng.randint(10_000, 99_999)}",
        "utm_source": source,
        "utm_medium": medium,
    }


def client_metadata_for(source_channel: str, rng: random.Random) -> dict:
    if source_channel == "MOBILE_APP":
        return mobile_metadata(rng)
    if source_channel == "THIRD_PARTY_SERVICE":
        return webhook_metadata(rng)
    return web_metadata(rng)
