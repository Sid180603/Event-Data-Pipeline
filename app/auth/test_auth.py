"""T4 auth tests. RED before implementation.

Two invariants are under test, and they are the whole point of §D4:

1. The gateway holds a *public* key and cannot mint a token for any tenant.
2. Tenant identity comes from the signed claim only -- never the body, never a
   header.

Keypairs are generated at runtime with `cryptography`. No key material is
committed to the repo and no real secret is ever read.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime as dt
import hashlib
import hmac
import json

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, generate_private_key

from app.auth import (
    AuthenticatedBatch,
    Credential,
    TenantRegistry,
    TokenParseBudget,
    TokenVerifier,
    authorize_batch,
)
from app.auth.errors import AuthError, Forbidden, Unauthorized
from contracts.attributes import career_site_id_from_source
from contracts.cloudevent import CloudEvent, decode_batch

AUDIENCE = "career-api"
TENANT_A = "acme_8921"
TENANT_B = "globex_4471"
TENANT_UNKNOWN = "ghost_0001"

CREDS = (
    Credential(credential_id="cred_a_web", career_site_id=TENANT_A, source_channel="WEB_APP"),
    Credential(credential_id="cred_a_mob", career_site_id=TENANT_A, source_channel="MOBILE_APP"),
    Credential(credential_id="cred_a_3p", career_site_id=TENANT_A, source_channel="THIRD_PARTY_SERVICE"),
    Credential(credential_id="cred_b_web", career_site_id=TENANT_B, source_channel="WEB_APP"),
)


# --- Runtime keypairs (nothing is committed) ---------------------------------


def _ed25519() -> tuple[Ed25519PrivateKey, str]:
    key = Ed25519PrivateKey.generate()
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return key, pem.decode()


def _rsa() -> tuple[RSAPrivateKey, str]:
    key = generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    return key, pem.decode()


@pytest.fixture(scope="module")
def issuer_eddsa() -> tuple[Ed25519PrivateKey, str]:
    return _ed25519()


@pytest.fixture(scope="module")
def issuer_rsa() -> tuple[RSAPrivateKey, str]:
    return _rsa()


@pytest.fixture(scope="module")
def stranger_eddsa() -> tuple[Ed25519PrivateKey, str]:
    """A private key the gateway does not hold -- the forging attack (H2)."""
    return _ed25519()


@pytest.fixture
def verifier(issuer_eddsa: tuple[Ed25519PrivateKey, str]) -> TokenVerifier:
    return TokenVerifier(
        public_key_pem=issuer_eddsa[1], algorithm="EdDSA", audience=AUDIENCE
    )


@pytest.fixture
def registry() -> TenantRegistry:
    return TenantRegistry(CREDS)


# --- Token minting (test-only) -----------------------------------------------


def mint(
    private_key,
    *,
    tenant: str = TENANT_A,
    credential_id: str = "cred_a_web",
    algorithm: str = "EdDSA",
    audience: str = AUDIENCE,
    expires_in_s: int = 3600,
    drop: tuple[str, ...] = (),
) -> str:
    now = dt.datetime.now(dt.timezone.utc)
    claims: dict = {
        "career_site_id": tenant,
        "credential_id": credential_id,
        "aud": audience,
        "iat": now,
        "exp": now + dt.timedelta(seconds=expires_in_s),
    }
    for name in drop:
        claims.pop(name, None)
    return jwt.encode(claims, private_key, algorithm=algorithm)


def bearer(token: str) -> str:
    return f"Bearer {token}"


def _raw_event(source: str, index: int, *, channel: str | None = None) -> dict:
    event = {
        "specversion": "1.0",
        "id": f"01J8XQ4M7K2P9R3S{index:02d}",
        "source": source,
        "type": "com.careerpage.career.job-viewed",
        "time": "2026-09-30T14:43:09.123Z",
        "subject": "job_88320491",
        "data": {
            "candidate": {"user_id_pseudo": "a1b2c3d4e5016f7a8b9c0d1e2f3a4"},
            "event_payload": {"job_id": "job_88320491", "session_id": "sess_8839201923"},
        },
    }
    if channel is not None:
        event["sourcechannel"] = channel
    return event


def events_for(tenant: str, count: int = 3) -> list[CloudEvent]:
    return decode_batch([_raw_event(f"/careers/{tenant}", i) for i in range(count)])


def authorize(verifier, registry, events, authorization, **kw) -> AuthenticatedBatch:
    return authorize_batch(
        events,
        authorization=authorization,
        verifier=verifier,
        registry=registry,
        **kw,
    )


def status_of(excinfo) -> int:
    assert isinstance(excinfo.value, AuthError)
    return excinfo.value.status_code


# --- Happy path --------------------------------------------------------------


def test_valid_eddsa_token_authorizes_batch(verifier, registry, issuer_eddsa):
    result = authorize(
        verifier, registry, events_for(TENANT_A), bearer(mint(issuer_eddsa[0]))
    )
    assert result.career_site_id == TENANT_A
    assert result.source == f"/careers/{TENANT_A}"
    assert result.source_channel == "WEB_APP"
    assert result.credential_id == "cred_a_web"
    assert result.event_count == 3


def test_rs256_is_also_accepted(registry, issuer_rsa):
    rsa_verifier = TokenVerifier(
        public_key_pem=issuer_rsa[1], algorithm="RS256", audience=AUDIENCE
    )
    result = authorize(
        rsa_verifier, registry, events_for(TENANT_A), bearer(mint(issuer_rsa[0], algorithm="RS256"))
    )
    assert result.career_site_id == TENANT_A


def test_hs256_is_not_a_configurable_algorithm(issuer_eddsa):
    """H2: we rejected HS256 because a shared secret is a 500-tenant compromise."""
    with pytest.raises(ValueError, match="HS256|algorithm"):
        TokenVerifier(public_key_pem=issuer_eddsa[1], algorithm="HS256", audience=AUDIENCE)


def test_bearer_scheme_is_case_insensitive(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0])
    result = authorize(verifier, registry, events_for(TENANT_A), f"bearer {token}")
    assert result.career_site_id == TENANT_A


# --- The gateway cannot mint a token (H2) ------------------------------------


def test_gateway_holds_only_a_public_key(verifier):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

    assert isinstance(verifier.public_key, Ed25519PublicKey)
    assert not hasattr(verifier, "sign")
    assert not hasattr(verifier, "encode")


def test_gateway_refuses_to_be_configured_with_a_private_key(issuer_eddsa):
    private_pem = issuer_eddsa[0].private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    with pytest.raises(ValueError, match="public"):
        TokenVerifier(public_key_pem=private_pem, algorithm="EdDSA", audience=AUDIENCE)


def test_token_signed_with_a_key_the_gateway_does_not_hold_is_rejected(
    verifier, registry, stranger_eddsa
):
    """The minting attack: forge a token for any tenant with our own key."""
    forged = mint(stranger_eddsa[0], tenant=TENANT_A, credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(forged))
    assert status_of(exc) == 401


def test_forged_token_for_another_tenant_is_rejected(verifier, registry, stranger_eddsa):
    forged = mint(stranger_eddsa[0], tenant=TENANT_B, credential_id="cred_b_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_B), bearer(forged))
    assert status_of(exc) == 401


def test_authentication_precedes_any_registry_lookup(verifier, stranger_eddsa):
    """An unauthenticated caller must never reach tenant state, not even an empty one.

    If the registry were consulted first, an empty registry would answer 403 and
    confirm that the gateway is running before the signature was ever checked.
    """
    forged = mint(stranger_eddsa[0], tenant=TENANT_A, credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, TenantRegistry([]), events_for(TENANT_A), bearer(forged))
    assert status_of(exc) == 401


def test_rs256_token_signed_with_another_rsa_key_is_rejected(registry, issuer_rsa, stranger_eddsa):
    rsa_verifier = TokenVerifier(
        public_key_pem=issuer_rsa[1], algorithm="RS256", audience=AUDIENCE
    )
    other_rsa_private, _ = _rsa()
    bad = mint(other_rsa_private, algorithm="RS256")
    with pytest.raises(AuthError) as exc:
        authorize(rsa_verifier, registry, events_for(TENANT_A), bearer(bad))
    assert status_of(exc) == 401


def _handmade_hs256_token(public_pem: str, **claims) -> str:
    """A real attacker's key-confusion token, built by hand.

    PyJWT 2.15 refuses to *mint* one (it rejects an asymmetric key as an HMAC
    secret), which is a good sign for the library but means the attack has to
    be assembled manually here. The gateway must still reject it.
    """

    def seg(obj: dict) -> bytes:
        return base64.urlsafe_b64encode(
            json.dumps(obj, separators=(",", ":")).encode()
        ).rstrip(b"=")

    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    payload = {
        "career_site_id": TENANT_A,
        "credential_id": "cred_a_web",
        "aud": AUDIENCE,
        "exp": now + 3600,
        **claims,
    }
    head, body = seg({"alg": "HS256", "typ": "JWT"}), seg(payload)
    sig = base64.urlsafe_b64encode(
        hmac.new(public_pem.encode(), head + b"." + body, hashlib.sha256).digest()
    ).rstrip(b"=")
    return (head + b"." + body + b"." + sig).decode()


def test_algorithm_confusion_hs256_using_the_public_key_is_rejected(
    verifier, registry, issuer_eddsa
):
    """Key confusion: sign HS256 with the public key the gateway already holds."""
    token = _handmade_hs256_token(issuer_eddsa[1])
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_alg_none_token_is_rejected(verifier, registry):
    def seg(obj: dict) -> bytes:
        return base64.urlsafe_b64encode(
            json.dumps(obj, separators=(",", ":")).encode()
        ).rstrip(b"=")

    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    token = (
        seg({"alg": "none", "typ": "JWT"})
        + b"."
        + seg(
            {
                "career_site_id": TENANT_A,
                "credential_id": "cred_a_web",
                "aud": AUDIENCE,
                "exp": now + 3600,
            }
        )
        + b"."
    ).decode()
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


# --- 401 paths ---------------------------------------------------------------


@pytest.mark.parametrize(
    "authorization",
    [None, "", "Bearer", "Bearer ", "Basic abc", "Bearer a b", "bearer"],
)
def test_missing_or_malformed_token_is_401(verifier, registry, authorization):
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), authorization)
    assert status_of(exc) == 401


def test_garbage_token_is_401(verifier, registry):
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer("not-a-jwt"))
    assert status_of(exc) == 401


def test_expired_token_is_401(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], expires_in_s=-60)
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_wrong_audience_is_401(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], audience="some-other-api")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_missing_career_site_id_claim_is_401(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], drop=("career_site_id",))
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401
    assert "career_site_id" in exc.value.reason


def test_missing_credential_id_claim_is_401(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], drop=("credential_id",))
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_non_string_tenant_claim_is_401(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], tenant=12345)
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_tenant_claim_that_is_not_a_path_segment_is_401(verifier, registry, issuer_eddsa):
    """A claim is never concatenated into a `source` without the contract check."""
    token = mint(issuer_eddsa[0], tenant="a/b")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 401


def test_a_401_never_leaks_why_the_crypto_failed(verifier, registry, stranger_eddsa):
    """The reason is the exception class only -- no oracle for a prober."""
    with pytest.raises(AuthError) as exc:
        authorize(
            verifier,
            registry,
            events_for(TENANT_A),
            bearer(mint(stranger_eddsa[0])),
        )
    assert exc.value.reason == "InvalidSignatureError"
    assert "public" not in exc.value.reason.lower()


# --- Tenant binding (JWT-only) -----------------------------------------------


def test_token_for_unknown_tenant_is_rejected(verifier, registry, issuer_eddsa):
    """H4: the registry is fixed, so an unknown career_site_id is not a tenant."""
    token = mint(issuer_eddsa[0], tenant=TENANT_UNKNOWN, credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_UNKNOWN), bearer(token))
    assert status_of(exc) == 403


def test_tenant_a_token_cannot_write_an_event_claiming_tenant_b(
    verifier, registry, issuer_eddsa
):
    token = mint(issuer_eddsa[0], tenant=TENANT_A, credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_B), bearer(token))
    assert status_of(exc) == 403


def test_one_bad_event_among_good_ones_is_still_403(verifier, registry, issuer_eddsa):
    """Never partially accept: a mixed batch is refused as a whole (D2)."""
    events = decode_batch(
        [_raw_event(f"/careers/{TENANT_A}", 0), _raw_event(f"/careers/{TENANT_B}", 1)]
    )
    token = mint(issuer_eddsa[0])
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events, bearer(token))
    assert status_of(exc) == 403


def test_batch_mixing_two_tenants_is_403(verifier, registry, issuer_eddsa):
    events = decode_batch(
        [
            _raw_event(f"/careers/{TENANT_A}", 0),
            _raw_event(f"/careers/{TENANT_A}", 1),
            _raw_event(f"/careers/{TENANT_B}", 2),
        ]
    )
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events, bearer(mint(issuer_eddsa[0])))
    assert status_of(exc) == 403
    assert "tenant" in exc.value.reason.lower()


def test_batch_of_one_tenant_is_accepted(verifier, registry, issuer_eddsa):
    events = decode_batch([_raw_event(f"/careers/{TENANT_A}", i) for i in range(50)])
    result = authorize(verifier, registry, events, bearer(mint(issuer_eddsa[0])))
    assert result.event_count == 50


def test_credential_registered_to_another_tenant_is_403(verifier, registry, issuer_eddsa):
    """Tenant A's token carrying tenant B's credential id."""
    token = mint(issuer_eddsa[0], tenant=TENANT_A, credential_id="cred_b_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 403


def test_unregistered_credential_id_is_403(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], credential_id="cred_z_web")
    with pytest.raises(AuthError) as exc:
        authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert status_of(exc) == 403


# --- sourcechannel comes from credential registration, never a header --------


def test_source_channel_is_derived_from_credential_registration(
    verifier, registry, issuer_eddsa
):
    cases = {
        "cred_a_web": "WEB_APP",
        "cred_a_mob": "MOBILE_APP",
        "cred_a_3p": "THIRD_PARTY_SERVICE",
    }
    for credential_id, expected in cases.items():
        token = mint(issuer_eddsa[0], credential_id=credential_id)
        result = authorize(verifier, registry, events_for(TENANT_A), bearer(token))
        assert result.source_channel == expected


def test_forged_x_source_type_on_a_web_app_credential_is_rejected(
    verifier, registry, issuer_eddsa
):
    token = mint(issuer_eddsa[0], credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(
            verifier,
            registry,
            events_for(TENANT_A),
            bearer(token),
            x_source_type="THIRD_PARTY_SERVICE",
        )
    assert status_of(exc) == 403


def test_forged_x_source_type_does_not_change_the_recorded_channel(
    verifier, registry, issuer_eddsa
):
    """A rejected forged hint leaves the recorded channel exactly as registered."""
    token = mint(issuer_eddsa[0], credential_id="cred_a_web")
    with pytest.raises(AuthError):
        authorize(
            verifier,
            registry,
            events_for(TENANT_A),
            bearer(token),
            x_source_type="MOBILE_APP",
        )

    after = authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert after.source_channel == "WEB_APP"

    # And when the hint agrees, it agrees with the registry rather than driving it.
    hinted = authorize(
        verifier, registry, events_for(TENANT_A), bearer(token), x_source_type="WEB_APP"
    )
    assert hinted.source_channel == "WEB_APP"


def test_invalid_x_source_type_value_is_rejected(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], credential_id="cred_a_web")
    with pytest.raises(AuthError) as exc:
        authorize(
            verifier,
            registry,
            events_for(TENANT_A),
            bearer(token),
            x_source_type="DROP_EVERYTHING",
        )
    assert status_of(exc) == 403


def test_no_x_source_type_header_is_fine(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0], credential_id="cred_a_web")
    result = authorize(verifier, registry, events_for(TENANT_A), bearer(token))
    assert result.source_channel == "WEB_APP"


# --- One verify per batch (D2) ------------------------------------------------


def test_jwt_is_verified_once_per_batch_not_once_per_event(verifier, registry, issuer_eddsa):
    events = events_for(TENANT_A, 500)
    token = mint(issuer_eddsa[0])
    result = authorize(verifier, registry, events, bearer(token))
    assert result.event_count == 500
    assert verifier.verify_count == 1, "one verify per request, not one per event"


def test_verify_count_accumulates_one_per_request(verifier, registry, issuer_eddsa):
    token = mint(issuer_eddsa[0])
    authorize(verifier, registry, events_for(TENANT_A, 10), bearer(token))
    authorize(verifier, registry, events_for(TENANT_A, 20), bearer(token))
    assert verifier.verify_count == 2


def test_a_rejected_batch_still_only_verifies_once(verifier, registry, issuer_eddsa):
    events = decode_batch([_raw_event(f"/careers/{TENANT_B}", i) for i in range(50)])
    with pytest.raises(AuthError):
        authorize(verifier, registry, events, bearer(mint(issuer_eddsa[0])))
    assert verifier.verify_count == 1


# --- Malformed-token path has its own budget (AC9) ---------------------------


def test_malformed_token_path_has_a_separate_budget(verifier, registry, issuer_eddsa):
    """A garbage-token flood must not be able to burn the event budget."""
    budget = TokenParseBudget(limit=2)
    for _ in range(2):
        with pytest.raises(AuthError) as exc:
            authorize(
                verifier,
                registry,
                events_for(TENANT_A),
                bearer("garbage"),
                parse_budget=budget,
            )
        assert status_of(exc) == 401

    with pytest.raises(AuthError) as exc:
        authorize(
            verifier, registry, events_for(TENANT_A), bearer("garbage"), parse_budget=budget
        )
    assert status_of(exc) == 401
    assert budget.malformed == 3
    assert budget.refused == 1

    # The genuine tenant is unaffected: its budget was never touched.
    good = authorize(
        verifier,
        registry,
        events_for(TENANT_A),
        bearer(mint(issuer_eddsa[0])),
        parse_budget=budget,
    )
    assert good.career_site_id == TENANT_A


def test_a_valid_token_never_charges_the_malformed_budget(
    verifier, registry, issuer_eddsa
):
    budget = TokenParseBudget(limit=1)
    authorize(
        verifier,
        registry,
        events_for(TENANT_A),
        bearer(mint(issuer_eddsa[0])),
        parse_budget=budget,
    )
    assert budget.malformed == 0


def test_counters_are_observable_for_metrics(verifier, registry, issuer_eddsa):
    budget = TokenParseBudget(limit=5)
    authorize(verifier, registry, events_for(TENANT_A), bearer(mint(issuer_eddsa[0])), parse_budget=budget)
    with pytest.raises(AuthError):
        authorize(verifier, registry, events_for(TENANT_A), None, parse_budget=budget)
    assert budget.malformed == 1
    assert budget.refused == 0


# --- Registry ----------------------------------------------------------------


def test_registry_rejects_a_duplicate_credential_id():
    """A shadowed registration would silently pick one tenant's channel."""
    with pytest.raises(ValueError, match="credential"):
        TenantRegistry(list(CREDS) + [CREDS[0]])


def test_registry_membership(registry):
    assert registry.contains(TENANT_A)
    assert not registry.contains(TENANT_UNKNOWN)
    assert len(registry) == 2, "two tenants, four credentials"


def test_registry_rejects_an_unusable_credential():
    with pytest.raises(ValueError, match="channel"):
        Credential(
            credential_id="cred_x",
            career_site_id=TENANT_A,
            source_channel="SMS",  # type: ignore[arg-type]
        )


# --- The result is a single authoritative source string ----------------------


def test_authorized_source_matches_the_contract_helper(verifier, registry, issuer_eddsa):
    result = authorize(
        verifier, registry, events_for(TENANT_A), bearer(mint(issuer_eddsa[0]))
    )
    assert career_site_id_from_source(result.source) == result.career_site_id


def test_authorized_batch_carries_no_client_controlled_input(verifier, registry, issuer_eddsa):
    """Every field on the result is derived here or verified by signature."""
    result = authorize(
        verifier, registry, events_for(TENANT_A), bearer(mint(issuer_eddsa[0]))
    )
    assert {f.name for f in dataclasses.fields(result)} == {
        "career_site_id",
        "source",
        "source_channel",
        "credential_id",
        "event_count",
    }
