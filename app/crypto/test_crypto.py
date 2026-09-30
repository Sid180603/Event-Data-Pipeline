"""T6 crypto tests: purpose separation, tenant registry, AAD, nonces.

RED before implementation.

Two rules shape every assertion here:

1. **Canary, never a value regex.** Plan H3 found that a "looks like an email"
   regex passed vacuously while the real rule was broken. So each leak test
   plants a literal sentinel in a PII field and asserts on the exact bytes that
   would reach Kafka. The sentinel contains `@`, which the base64url alphabet
   cannot produce, so a literal substring search cannot produce a false
   negative and cannot be defeated by encoding.
2. **Assert the attack, not the code path.** A ciphertext moved to another
   field, event or key version must fail *authentication* (`InvalidTag`), not
   merely raise something.
"""

from __future__ import annotations

import hashlib
import hmac as stdlib_hmac

import msgspec
import pytest
from cryptography.exceptions import InvalidTag

from app.config import Settings
from app.crypto import aesgcm as AE
from app.crypto.facade import PII_FIELDS, protect_candidate
from app.crypto.keys import ENC_INFO, KEY_LENGTH, MAC_INFO, derive_tenant_keys
from app.crypto.registry import (
    TenantKeyRegistry,
    UnknownKeyVersionError,
    UnknownTenantError,
)
from app.pseudonym.hmac import pseudonymize
from contracts.attributes import (
    career_site_id_from_source,
    derive_kafka_key,
    extra_attributes,
)
from contracts.cloudevent import CloudEvent, Data, EventPayload

# --- fixtures ----------------------------------------------------------------

#: Obviously synthetic key material, 35 bytes, never a real secret. Real key
#: material comes from the `MASTER_SECRET` env var and is never committed.
DEV_MASTER = b"test-only-master-secret-do-not-use!"

TENANTS = ("acme_8921", "globex_4471")

TENANT_A = "acme_8921"
TENANT_B = "globex_4471"

EVENT_ID = "01J8XQ4M7K2P9R3S01"
EVENT_TYPE = "com.careerpage.career.job-viewed"
SOURCE = f"/careers/{TENANT_A}"

#: The canary from plan M7.
CANARY_EMAIL = "SENTINEL-8f3a@example.invalid"
CANARY_USER_ID = "usr_CANARY_992182741"


def make_registry(**overrides) -> TenantKeyRegistry:
    settings = Settings(**{"master_secret": DEV_MASTER, "key_version": 1, **overrides})
    return TenantKeyRegistry(settings, TENANTS)


def aad(
    field_name: str = "name_enc",
    *,
    source: str = SOURCE,
    event_id: str = EVENT_ID,
    event_type: str = EVENT_TYPE,
    key_version: int = 1,
) -> bytes:
    return AE.build_aad(
        source=source,
        event_id=event_id,
        event_type=event_type,
        field_name=field_name,
        key_version=key_version,
    )


def build_event(metadata, *, key_version: int = 1, source: str = SOURCE, event_id: str = EVENT_ID):
    """A complete, wire-shaped CloudEvent around one candidate."""
    return CloudEvent(
        specversion="1.0",
        id=event_id,
        source=source,
        type=EVENT_TYPE,
        time="2026-09-30T14:43:09.123Z",
        keyversion=key_version,
        data=Data(
            candidate=metadata,
            event_payload=EventPayload(job_id="job_88320491", session_id="sess_8839201923"),
        ),
    )


# --- keys: purpose separation (H4) ------------------------------------------


def test_enc_and_mac_keys_are_not_equal():
    """One key for AES-GCM and HMAC is the crypto-hygiene error we are fixing."""
    keys = make_registry().keys_for(TENANT_A)
    assert keys.enc_key != keys.mac_key


def test_the_two_keys_come_from_one_extract_with_different_info():
    """Both keys are expands of a single HKDF extract, separated only by `info`.

    Asserted against the published formula
    `HKDF-SHA256(ikm=master, salt=career_site_id, info=b"enc" | b"mac")`
    so the handoff to the DB team (plan D5, §8) stays exact.
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF

    keys = make_registry().keys_for(TENANT_A)
    expected_enc = HKDF(
        algorithm=hashes.SHA256(), length=KEY_LENGTH, salt=TENANT_A.encode(), info=ENC_INFO
    ).derive(DEV_MASTER)
    expected_mac = HKDF(
        algorithm=hashes.SHA256(), length=KEY_LENGTH, salt=TENANT_A.encode(), info=MAC_INFO
    ).derive(DEV_MASTER)

    assert keys.enc_key == expected_enc
    assert keys.mac_key == expected_mac
    assert ENC_INFO == b"enc" and MAC_INFO == b"mac"
    assert ENC_INFO != MAC_INFO


def test_tenant_keys_are_32_bytes():
    keys = make_registry().keys_for(TENANT_A)
    assert len(keys.enc_key) == KEY_LENGTH == 32
    assert len(keys.mac_key) == KEY_LENGTH


def test_each_tenant_gets_independent_keys():
    registry = make_registry()
    a, b = registry.keys_for(TENANT_A), registry.keys_for(TENANT_B)
    assert a.enc_key != b.enc_key
    assert a.mac_key != b.mac_key


def test_key_derivation_is_deterministic():
    """A restart must re-derive the same keys or nothing written earlier decrypts."""
    assert derive_tenant_keys(DEV_MASTER, TENANT_A.encode(), career_site_id=TENANT_A, key_version=1) == (
        make_registry().keys_for(TENANT_A)
    )


def test_a_short_master_secret_is_rejected():
    """A weak master silently produces weak per-tenant keys; fail at startup."""
    with pytest.raises(ValueError, match="master"):
        TenantKeyRegistry(Settings(master_secret=b"short"), TENANTS)


# --- registry: fixed, bounded, startup-only (H4) -----------------------------


def test_unknown_career_site_id_is_rejected():
    with pytest.raises(UnknownTenantError):
        make_registry().keys_for("ghost_tenant_0001")


def test_the_registry_is_fixed_at_construction_not_grown_per_lookup():
    """A JWT-controlled cache key is an unbounded-growth DoS. Ours cannot grow."""
    registry = make_registry()
    before = len(registry)

    for i in range(1000):
        with pytest.raises(UnknownTenantError):
            registry.keys_for(f"attacker_tenant_{i}")

    assert len(registry) == before == len(TENANTS)


def test_keys_are_derived_once_not_on_every_lookup():
    """Same object every time, not a fresh HKDF per call."""
    registry = make_registry()
    assert registry.keys_for(TENANT_A) is registry.keys_for(TENANT_A)
    assert registry.keys_for(TENANT_A) is not registry.keys_for(TENANT_B)


def test_registry_exposes_the_key_version_for_the_envelope():
    assert make_registry().keys_for(TENANT_A).key_version == 1
    assert make_registry(key_version=7).keys_for(TENANT_A).key_version == 7


# --- AES-GCM round trip and randomized nonces -------------------------------


def test_round_trip_decrypt():
    key = make_registry().keys_for(TENANT_A).enc_key
    for plaintext in ("a", CANARY_EMAIL, "+91 98765 43210", "Zoë Ångström", "🔐"):
        token = AE.encrypt_field(
            key,
            plaintext,
            source=SOURCE,
            event_id=EVENT_ID,
            event_type=EVENT_TYPE,
            field_name="name_enc",
            key_version=1,
        )
        assert (
            AE.decrypt_field(
                key,
                token,
                source=SOURCE,
                event_id=EVENT_ID,
                event_type=EVENT_TYPE,
                field_name="name_enc",
                key_version=1,
            )
            == plaintext
        )


def test_same_plaintext_encrypted_twice_yields_different_ciphertext():
    """Randomized AES-GCM: a deterministic scheme would leak equality."""
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(
        source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc", key_version=1
    )
    first = AE.encrypt_field(key, CANARY_EMAIL, **args)
    second = AE.encrypt_field(key, CANARY_EMAIL, **args)

    assert first != second
    assert first.split(".")[1] != second.split(".")[1]
    # ...and both still decrypt to the same plaintext.
    assert AE.decrypt_field(key, first, **args) == AE.decrypt_field(key, second, **args) == CANARY_EMAIL


def test_nonce_is_96_bits():
    key = make_registry().keys_for(TENANT_A).enc_key
    token = AE.encrypt_field(
        key, "x", source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc", key_version=1
    )
    _version, body = AE.split_ciphertext(token)
    assert AE.NONCE_BYTES == 12
    assert len(body[: AE.NONCE_BYTES]) == 12


def test_one_million_encryptions_have_zero_nonce_collisions():
    """Nonce reuse under AES-GCM leaks the GHASH key. Random 96-bit, never a counter.

    Full encryptions, not a sample of `os.urandom`: the property under test is
    that *the code path we ship* never repeats a nonce. A one-byte payload
    keeps this at a few seconds so it can stay in the default run.
    """
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(
        source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc", key_version=1
    )

    seen: set[bytes] = set()
    add = seen.add
    for _ in range(1_000_000):
        _version, body = AE.split_ciphertext(AE.encrypt_field(key, "x", **args))
        add(body[: AE.NONCE_BYTES])

    assert len(seen) == 1_000_000


# --- tenant isolation --------------------------------------------------------


def test_tenant_a_key_cannot_decrypt_tenant_b_ciphertext():
    registry = make_registry()
    token = registry.encrypt_field(
        TENANT_B,
        CANARY_EMAIL,
        source=f"/careers/{TENANT_B}",
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        field_name="email_enc",
    )

    with pytest.raises(InvalidTag):
        registry.decrypt_field(
            TENANT_A,
            token,
            source=f"/careers/{TENANT_B}",
            event_id=EVENT_ID,
            event_type=EVENT_TYPE,
            field_name="email_enc",
        )


def test_decrypting_with_the_wrong_tenant_raises_unknown_tenant():
    registry = make_registry()
    token = registry.encrypt_field(
        TENANT_A, "x", source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc"
    )
    with pytest.raises(UnknownTenantError):
        registry.decrypt_field(
            "ghost_tenant_0001",
            token,
            source=SOURCE,
            event_id=EVENT_ID,
            event_type=EVENT_TYPE,
            field_name="name_enc",
        )


# --- AAD binding (plan D5 "Low fix") -----------------------------------------


def test_ciphertext_moved_to_another_field_fails_to_authenticate():
    """Field swap: a `name` ciphertext landing in `email_enc` must not decrypt."""
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, key_version=1)
    token = AE.encrypt_field(key, "Asha Rao", field_name="name_enc", **args)

    with pytest.raises(InvalidTag):
        AE.decrypt_field(key, token, field_name="email_enc", **args)


def test_ciphertext_transplanted_to_another_event_fails_to_authenticate():
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(source=SOURCE, event_type=EVENT_TYPE, field_name="name_enc", key_version=1)
    token = AE.encrypt_field(key, "Asha Rao", event_id=EVENT_ID, **args)

    with pytest.raises(InvalidTag):
        AE.decrypt_field(key, token, event_id="01J8XQ4M7K2P9R3S99", **args)


def test_ciphertext_moved_to_another_tenant_source_fails_to_authenticate():
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc", key_version=1)
    token = AE.encrypt_field(key, "Asha Rao", source=SOURCE, **args)

    with pytest.raises(InvalidTag):
        AE.decrypt_field(key, token, source="/careers/globex_4471", **args)


def test_ciphertext_moved_to_another_event_type_fails_to_authenticate():
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(source=SOURCE, event_id=EVENT_ID, field_name="name_enc", key_version=1)
    token = AE.encrypt_field(key, "Asha Rao", event_type=EVENT_TYPE, **args)

    with pytest.raises(InvalidTag):
        AE.decrypt_field(
            key, token, event_type="com.careerpage.career.application-submitted", **args
        )


def test_ciphertext_reversioned_fails_to_authenticate():
    """Rewriting the version prefix sends the consumer to the wrong key and a
    different AAD. It must not authenticate."""
    key = make_registry().keys_for(TENANT_A).enc_key
    args = dict(source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc")
    token = AE.encrypt_field(key, "Asha Rao", key_version=1, **args)

    forged = "2." + token.split(".", 1)[1]
    assert AE.ciphertext_key_version(forged) == 2

    with pytest.raises(InvalidTag):
        AE.decrypt_field(key, forged, key_version=2, **args)


def test_aad_components_cannot_be_confused_by_concatenation():
    """`id` is client-controlled, so a value that mimics the encoding must not be
    able to reproduce another event's AAD."""
    fields = dict(
        source="/careers/a", event_id="01ABC", event_type="com.x.y", field_name="name_enc", key_version=1
    )
    baseline = AE.build_aad(**fields)

    # Same bytes, different split: delimiter-joined AAD would collide here.
    assert baseline != AE.build_aad(**{**fields, "event_id": "01ABC\x1fcom.x.y", "event_type": ""})
    assert baseline != AE.build_aad(**{**fields, "source": "/careers/a\x1f01ABC", "event_id": ""})
    assert baseline != AE.build_aad(**{**fields, "field_name": "name_enc\x1f1", "key_version": 0})


def test_malformed_ciphertext_is_rejected_clearly():
    key = make_registry().keys_for(TENANT_A).enc_key
    with pytest.raises(ValueError, match="ciphertext"):
        AE.ciphertext_key_version("no-version-prefix-here")


# --- keyversion as an extension attribute (plan D5 "Low fix") ----------------


def test_keyversion_is_readable_without_decoding_the_payload():
    registry = make_registry()
    token = registry.encrypt_field(
        TENANT_A, "x", source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc"
    )
    # Readable as text, before any base64 work: that is what lets a consumer
    # select a key without decoding `data`.
    assert AE.ciphertext_key_version(token) == 1
    assert token.split(".", 1)[0] == "1"


def test_decrypting_a_ciphertext_from_another_key_version_is_refused():
    registry = make_registry(key_version=2)
    old_token = AE.encrypt_field(
        make_registry().keys_for(TENANT_A).enc_key,
        "x",
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        field_name="name_enc",
        key_version=1,
    )
    with pytest.raises(UnknownKeyVersionError):
        registry.decrypt_field(TENANT_A, old_token, source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc")


def test_keyversion_travels_as_a_context_attribute_not_in_data():
    """A version number is not sensitive, and consumers need it up front."""
    registry = make_registry()
    metadata = protect_candidate(
        registry=registry,
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
    )
    keys = registry.keys_for(career_site_id_from_source(SOURCE))
    event = build_event(metadata, key_version=keys.key_version)
    raw = msgspec.json.decode(msgspec.json.encode(event))

    assert raw["keyversion"] == 1
    assert "keyversion" not in raw["data"]
    assert extra_attributes(raw) == set()


# --- pseudonymous twins (D5) -------------------------------------------------


def test_user_id_pseudo_is_hmac_sha256_of_the_raw_id():
    """Published formula, asserted independently of our own code."""
    mac_key = make_registry().keys_for(TENANT_A).mac_key
    expected = stdlib_hmac.new(mac_key, CANARY_USER_ID.encode(), hashlib.sha256).hexdigest()

    assert pseudonymize(mac_key, CANARY_USER_ID) == expected
    assert expected not in CANARY_USER_ID
    assert len(expected) == 64


def test_user_id_pseudo_has_no_usr_prefix():
    """contracts.attributes rejects an `usr_`-prefixed subject (plan H3); a
    pseudonym that trips our own allowlist would break every identity event."""
    mac_key = make_registry().keys_for(TENANT_A).mac_key
    assert not pseudonymize(mac_key, CANARY_USER_ID).startswith(("usr_", "user_"))


def test_email_hmac_groups_equal_addresses_and_is_not_reversible():
    """Equality grouping only. A keyholder cannot get the address back from it."""
    mac_key = make_registry().keys_for(TENANT_A).mac_key
    a, b, other = pseudonymize(mac_key, CANARY_EMAIL), pseudonymize(mac_key, CANARY_EMAIL), pseudonymize(
        mac_key, "other@example.invalid"
    )

    assert a == b
    assert a != other
    assert CANARY_EMAIL not in a
    # A 32-byte tag is not a lookup table: no substring of it is the address.
    assert not any(CANARY_EMAIL[i : i + 4] in a for i in range(len(CANARY_EMAIL) - 3))


def test_the_same_pseudo_differs_per_tenant():
    """No cross-tenant join on pseudonyms: that would be a tenancy leak."""
    registry = make_registry()
    assert pseudonymize(registry.keys_for(TENANT_A).mac_key, CANARY_USER_ID) != pseudonymize(
        registry.keys_for(TENANT_B).mac_key, CANARY_USER_ID
    )


# --- the facade (one pass, five fields) --------------------------------------


def test_facade_encrypts_exactly_five_fields():
    assert len(PII_FIELDS) == 5
    metadata = protect_candidate(
        registry=make_registry(),
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
        phone="+91 98765 43210",
        alternate_phone="+91 90000 00000",
        name="Asha Rao",
        gender="FEMALE",
    )

    encrypted = [metadata.email_enc, metadata.phone_enc, metadata.alternate_phone_enc, metadata.name_enc, metadata.gender_enc]
    assert all(encrypted)
    assert len(set(encrypted)) == 5
    assert all(AE.ciphertext_key_version(t) == 1 for t in encrypted)


def test_facade_leaves_absent_pii_absent():
    metadata = protect_candidate(
        registry=make_registry(),
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
    )
    encoded = msgspec.json.encode(metadata)
    assert b"phone_enc" not in encoded
    assert b"email_hmac" in encoded


def test_facade_rejects_an_unknown_tenant():
    with pytest.raises(UnknownTenantError):
        protect_candidate(
            registry=make_registry(),
            source="/careers/ghost_tenant_0001",
            event_id=EVENT_ID,
            event_type=EVENT_TYPE,
            raw_user_id=CANARY_USER_ID,
        )


def test_facade_output_round_trips_through_the_registry():
    registry = make_registry()
    metadata = protect_candidate(
        registry=registry,
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
        name="Asha Rao",
    )
    assert (
        registry.decrypt_field(TENANT_A, metadata.name_enc, source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="name_enc")
        == "Asha Rao"
    )
    assert (
        registry.decrypt_field(TENANT_A, metadata.email_enc, source=SOURCE, event_id=EVENT_ID, event_type=EVENT_TYPE, field_name="email_enc")
        == CANARY_EMAIL
    )


# --- canary leak tests (M7, C2, H3) ------------------------------------------


def test_canary_appears_in_no_output_field_at_all():
    """M7. Search the exact bytes that would be produced, and count occurrences.

    Two halves, and both are required:

    * **zero** occurrences anywhere in the serialized event -- the sentinel is in
      a ciphertext, so it must not survive into any field, attribute or key;
    * each ciphertext still decrypts back to exactly its own sentinel, which is
      what stops the zero-count from passing *vacuously* because the value
      never reached the cipher at all.

    The sentinel contains `@`, which the base64url alphabet cannot produce, so
    a literal substring search cannot miss an encoding of it.
    """
    registry = make_registry()
    canaries = {
        "email": CANARY_EMAIL,
        "phone": "SENTINEL-8f3a-phone",
        "alternate_phone": "SENTINEL-8f3a-alt",
        "name": "SENTINEL-8f3a-name",
        "gender": "SENTINEL-8f3a-gender",
    }
    metadata = protect_candidate(
        registry=registry,
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        **canaries,
    )
    wire = msgspec.json.encode(build_event(metadata, key_version=registry.keys_for(TENANT_A).key_version))
    key = derive_kafka_key(TENANT_A, metadata.user_id_pseudo)

    for field, canary in canaries.items():
        assert canary.encode() not in wire, f"{field} sentinel leaked into the wire form"
        assert canary not in key, f"{field} sentinel leaked into the Kafka key"
        # ...and it really did go through the cipher.
        ciphertext = getattr(metadata, f"{field}_enc")
        assert (
            registry.decrypt_field(
                TENANT_A,
                ciphertext,
                source=SOURCE,
                event_id=EVENT_ID,
                event_type=EVENT_TYPE,
                field_name=f"{field}_enc",
            )
            == canary
        )

    # The raw user id is not a PII field but the single most-inspected value in
    # the system: it must appear nowhere at all.
    assert CANARY_USER_ID.encode() not in wire
    assert CANARY_USER_ID not in key


def test_canary_never_appears_in_a_context_attribute():
    registry = make_registry()
    metadata = protect_candidate(
        registry=registry,
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
        name="SENTINEL-8f3a-name",
    )
    event = build_event(metadata, key_version=registry.keys_for(TENANT_A).key_version)

    for attribute in (
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
    ):
        value = getattr(event, attribute, None)
        if isinstance(value, str):
            assert CANARY_EMAIL not in value, f"PII leaked into context attr {attribute}"
            assert "SENTINEL-8f3a-name" not in value, f"PII leaked into context attr {attribute}"
            assert CANARY_USER_ID not in value, f"raw user id leaked into context attr {attribute}"


def test_kafka_key_carries_the_pseudo_and_never_the_raw_id():
    """C2: the earlier test scanned only the envelope and passed while the raw
    identifier sat in the most-inspected field in the system."""
    registry = make_registry()
    metadata = protect_candidate(
        registry=registry,
        source=SOURCE,
        event_id=EVENT_ID,
        event_type=EVENT_TYPE,
        raw_user_id=CANARY_USER_ID,
        email=CANARY_EMAIL,
    )
    key = derive_kafka_key(TENANT_A, metadata.user_id_pseudo)

    assert metadata.user_id_pseudo in key
    assert CANARY_USER_ID not in key
    assert CANARY_EMAIL not in key
    assert "SENTINEL" not in key
