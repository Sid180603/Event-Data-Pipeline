"""One-pass candidate protection: pseudonymise the id, encrypt the PII.

This is the seam the ingest pipeline calls. Plan D6 fixes the order
`decode -> auth -> validate -> encrypt -> produce`, so this runs after the
tenant has been bound from the signed JWT claim (D4) and after validation, and
before anything is produced. The pipeline owns the envelope; this owns the
`data.candidate` block.

Five fields in, five ciphertexts out, two HMACs alongside -- and the raw values
are local variables that go out of scope when this returns. There is no code
path here that returns, logs or raises with a plaintext PII value.
"""

from __future__ import annotations

from contracts.attributes import career_site_id_from_source
from contracts.cloudevent import CandidateMetadata

from app.crypto.aesgcm import encrypt_field
from app.crypto.registry import TenantKeyRegistry
from app.pseudonym.hmac import pseudonymize

#: `(plaintext name, CandidateMetadata attribute)`, in the order of
#: `CandidateMetadata`. The destination name is what the AAD binds to, so a
#: ciphertext is welded to the field it is allowed to appear in.
PII_FIELDS: tuple[tuple[str, str], ...] = (
    ("email", "email_enc"),
    ("phone", "phone_enc"),
    ("alternate_phone", "alternate_phone_enc"),
    ("name", "name_enc"),
    ("gender", "gender_enc"),
)


def protect_candidate(
    *,
    registry: TenantKeyRegistry,
    source: str,
    event_id: str,
    event_type: str,
    raw_user_id: str,
    email: str | None = None,
    phone: str | None = None,
    alternate_phone: str | None = None,
    name: str | None = None,
    gender: str | None = None,
) -> CandidateMetadata:
    """Turn a candidate's raw fields into the publishable `CandidateMetadata`.

    The tenant is taken from `source` rather than passed separately, so the key
    we encrypt with and the tenant named in the AAD cannot disagree. The caller
    MUST already have rejected a `source` that disagrees with the JWT tenant
    (D4) -- that binding is an auth decision, not a crypto one, and this
    function is not where it is made.

    Absent PII is left absent rather than encrypted as an empty string: "we do
    not know this candidate's gender" and "this candidate is not a woman" are
    different facts and must not be the same bytes.
    """
    career_site_id = career_site_id_from_source(source)
    keys = registry.keys_for(career_site_id)

    supplied = {
        "email": email,
        "phone": phone,
        "alternate_phone": alternate_phone,
        "name": name,
        "gender": gender,
    }
    encrypted: dict[str, str] = {}
    for plaintext_field, ciphertext_field in PII_FIELDS:
        value = supplied[plaintext_field]
        if value is None:
            continue
        encrypted[ciphertext_field] = encrypt_field(
            keys.enc_key,
            value,
            source=source,
            event_id=event_id,
            event_type=event_type,
            field_name=ciphertext_field,
            key_version=keys.key_version,
        )

    return CandidateMetadata(
        user_id_pseudo=pseudonymize(keys.mac_key, raw_user_id),
        # Equality-grouping only, never reversible. One HMAC per identity, not
        # per event: the same address must group with itself.
        email_hmac=pseudonymize(keys.mac_key, email) if email is not None else None,
        **encrypted,
    )
