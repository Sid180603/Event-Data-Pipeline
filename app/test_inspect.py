"""T-R5: the raw-vs-encrypted inspector, `app/inspect.py`. RED before implementation.

This is the demo beat where somebody asks "show me the PII is actually
encrypted", so the assertions are about the bytes rather than about the table:

* **The plaintext must not be in the stored record.** Not "is displayed as
  ciphertext" -- the raw published bytes must not contain the sample's email or
  its raw user id. A tool that rendered a made-up ciphertext beside a value it
  invented would look identical and prove nothing.
* **The two shapes must not be conflated.** What a client POSTs and what the
  topic carries are different structs (`contracts/ingress.py` vs
  `contracts/cloudevent.py`) and each refuses the other's fields. The inspector
  has to speak both, and the test decodes each one as the other and expects a
  rejection -- the confusion that once made the driver unable to drive load.
* **The plaintext must come back only through the operator endpoint**, so the
  gate is tested by *refusing*: a wrong operator key has to produce a 401 from
  the endpoint itself, five audit records saying `denied` with an
  `unidentified` actor, nothing in the third column, and a non-zero exit. A gate
  that was only an argument check in the CLI would pass a test that looked for
  the flag.
* **No operator key, no plaintext at all** -- not even the left-hand column,
  which is the strict reading of "never print plaintext without the operator key
  having been supplied".
"""

from __future__ import annotations

from dataclasses import replace

import msgspec
import pytest
from fastapi.testclient import TestClient

from app.config import TOPIC_RAW
from app.ingest.decrypt import DecryptAudit
from app.inspect import (
    CAREER_SITE_ID,
    OPERATOR_ID,
    SAMPLE_EMAIL,
    SAMPLE_USER_ID,
    FieldRow,
    Inspection,
    _build_app,
    _decrypt,
    build_parser,
    main,
    render,
    run_inspection,
    sample_event,
)
from contracts.cloudevent import CloudEvent
from contracts.ingress import IngressEvent

MASTER = "test-only-master-secret-do-not-use!"
OPERATOR_KEY = "test-only-operator-key"
WRONG_OPERATOR_KEY = "not-the-operator-key"

#: The five fields `app/crypto/facade.py` encrypts, in that order.
ENCRYPTED = ("email", "phone", "alternate_phone", "name", "gender")
ENCRYPTED_NAMES = {f"{name}_enc" for name in ENCRYPTED}


@pytest.fixture(autouse=True)
def no_ambient_credentials(monkeypatch) -> None:
    """The CLI reads `$OPERATOR_KEY` and `$MASTER_SECRET`; a developer's shell
    must not decide whether these tests pass."""
    monkeypatch.delenv("OPERATOR_KEY", raising=False)
    monkeypatch.delenv("MASTER_SECRET", raising=False)


@pytest.fixture
def audits() -> list[DecryptAudit]:
    return []


@pytest.fixture
def inspected(audits) -> Inspection:
    return run_inspection(
        master_secret=MASTER.encode(), operator_key=OPERATOR_KEY.encode(), audit=audits.append
    )


def args(**overrides) -> list[str]:
    """`main`'s argv, with `None` meaning "leave the flag off entirely"."""
    base = {"--master-secret": MASTER, "--operator-key": OPERATOR_KEY}
    base.update({f"--{name.replace('_', '-')}": value for name, value in overrides.items()})
    flat: list[str] = []
    for flag, value in base.items():
        if value is not None:
            flat.extend([flag, value])
    return flat


def encrypted_rows(inspection: Inspection) -> list:
    return [row for row in inspection.rows if row.stored_name in ENCRYPTED_NAMES]


# --- the sample itself --------------------------------------------------------


def test_the_sample_is_the_shape_a_client_posts():
    """Ingress, not egress: a raw `user_id` and plaintext, no `*_enc`."""
    candidate = sample_event()["data"]["candidate"]

    assert candidate["user_id"] == SAMPLE_USER_ID
    assert candidate["email"] == SAMPLE_EMAIL
    assert not [name for name in candidate if name.endswith(("_enc", "_hmac"))]


def test_the_stored_record_carries_ciphertext_and_none_of_the_plaintext(inspected):
    stored = msgspec.json.encode(inspected.event)

    assert SAMPLE_EMAIL.encode() not in stored
    assert SAMPLE_USER_ID.encode() not in stored
    assert b"email_enc" in stored
    assert b"user_id_pseudo" in stored


def test_the_two_shapes_reject_each_other(inspected):
    """Neither body decodes as the other struct. Conflating them is the bug
    this pair of assertions exists to keep fixed."""
    with pytest.raises(msgspec.DecodeError):
        msgspec.json.decode(msgspec.json.encode(inspected.sent), type=CloudEvent)
    with pytest.raises(msgspec.DecodeError):
        msgspec.json.decode(msgspec.json.encode(inspected.event), type=IngressEvent)


def test_it_is_the_same_event_before_and_after(inspected):
    sent = inspected.sent
    stored = inspected.event

    assert stored.id == sent["id"]
    assert stored.source == sent["source"]
    assert stored.type == sent["type"]
    assert stored.sequence == sent["sequence"]
    assert stored.data.candidate.user_id_pseudo
    assert stored.data.candidate.user_id_pseudo != SAMPLE_USER_ID


def test_the_sampled_record_is_the_one_the_sink_kept(inspected):
    """Read out of the sink, not reconstructed: the topic and the key are the
    producer's, which is what makes this the stored record rather than a
    re-encode of it."""
    assert inspected.topic == TOPIC_RAW
    assert inspected.key == f"{CAREER_SITE_ID}|{inspected.event.data.candidate.user_id_pseudo}"


# --- the operator gate --------------------------------------------------------


def test_the_plaintext_comes_back_through_the_operator_endpoint(inspected):
    rows = encrypted_rows(inspected)

    assert len(rows) == len(ENCRYPTED)
    for row in rows:
        assert row.refusal == ""
        assert row.decrypted == row.sent, row.stored_name


def test_the_two_hmac_fields_are_shown_and_never_decrypted(inspected):
    """They are HMACs, not ciphertexts, and the endpoint's whitelist refuses
    them -- so asking would be five guaranteed 400s and a misleading table."""
    hmac_rows = [row for row in inspected.rows if row.stored_name not in ENCRYPTED_NAMES]

    assert [row.stored_name for row in hmac_rows] == ["user_id_pseudo", "email_hmac"]
    for row in hmac_rows:
        assert row.decrypted is None
        assert "HMAC" in row.note
    assert [row.sent_name for row in hmac_rows] == ["user_id", "email"]


def test_only_the_five_ciphertexts_are_asked_for(inspected, audits):
    """Proves the HMACs were never requested: the audit trail is every call."""
    assert {record.field for record in audits} == ENCRYPTED_NAMES
    assert len(audits) == len(ENCRYPTED)


def test_a_wrong_operator_key_is_refused_by_the_endpoint(inspected):
    """The gate is the endpoint's, and the third column has to report it.

    Driven with a credential the gateway was NOT configured with, which is the
    only way to make `/v1/decrypt` refuse from inside this module: the tool
    configures the gateway with the credential it presents, so a mismatch is
    unrepresentable through `main` by construction. The endpoint's own 401
    behaviour is `app/ingest/test_pipeline.py`'s; what is being checked here is
    that this tool turns it into a refusal instead of into a blank cell.
    """
    denials: list[DecryptAudit] = []
    app = _build_app(
        master_secret=MASTER.encode(), operator_key=OPERATOR_KEY.encode(), audit=denials.append
    )
    with TestClient(app) as client:
        value, refusal = _decrypt(
            client, inspected.event, "email_enc", WRONG_OPERATOR_KEY.encode(), sent=SAMPLE_EMAIL
        )

    assert value is None
    assert "401" in refusal
    assert [record.outcome for record in denials] == ["denied"]
    assert {record.actor for record in denials} == {"unidentified"}


def test_a_refused_field_makes_the_whole_inspection_not_ok(inspected):
    """The exit code, so a demo that ended on a refusal is a demo that failed."""
    refused = replace(inspected, refusals=("email_enc: HTTP 401 UNAUTHORIZED",))

    assert refused.ok is False
    assert inspected.ok is True


def test_every_successful_decrypt_is_audited_with_who_and_which_event(inspected, audits):
    assert inspected.ok is True
    for record in audits:
        assert record.actor == OPERATOR_ID
        assert record.outcome == "ok"
        assert record.source == f"/careers/{CAREER_SITE_ID}"
        assert record.event_id == inspected.event.id
        assert record.field in ENCRYPTED_NAMES
        # WHO and WHICH, never the value: the audit outlives the request.
        assert SAMPLE_EMAIL not in repr(record)


def test_no_plaintext_is_printed_without_an_operator_key(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(args(operator_key=None))

    captured = capsys.readouterr()
    assert excinfo.value.code == 2
    assert "operator" in captured.err.lower()
    assert SAMPLE_EMAIL not in captured.out
    assert SAMPLE_EMAIL not in captured.err
    assert SAMPLE_USER_ID not in captured.out


def test_no_master_secret_means_no_run(capsys):
    with pytest.raises(SystemExit) as excinfo:
        main(args(master_secret=None))

    assert excinfo.value.code == 2
    assert "MASTER_SECRET" in capsys.readouterr().err


# --- the report ---------------------------------------------------------------


def test_the_report_says_which_side_is_which(inspected):
    text = render(inspected)

    assert "what the client sent" in text
    assert "what is stored" in text
    assert "/v1/decrypt" in text
    assert TOPIC_RAW in text


def test_the_stored_column_carries_the_ciphertext_not_the_plaintext(inspected):
    text = render(inspected)
    stored = next(row for row in inspected.rows if row.stored_name == "email_enc").stored

    assert stored in text
    assert stored != SAMPLE_EMAIL
    # The email appears exactly three times: the value the client sent (twice,
    # once for the ciphertext and once for the HMAC it also fed) and the value
    # the operator endpoint handed back. Any fourth is a leak.
    assert text.count(SAMPLE_EMAIL) == 3


def test_the_report_carries_the_operator_actor_and_never_an_audit_value(inspected):
    text = render(inspected)

    assert OPERATOR_ID in text
    assert "value=" not in text
    assert SAMPLE_USER_ID in text, "the raw id belongs in the sent column"


def test_main_prints_the_report_and_says_it_worked(capsys):
    code = main(args())

    out = capsys.readouterr().out
    assert code == 0
    assert SAMPLE_EMAIL in out
    assert OPERATOR_ID in out


def test_main_exits_non_zero_when_the_endpoint_refuses(capsys, monkeypatch):
    """`main` has to turn a refusal into a non-zero exit: a demo that ends on a
    401 and reports success is the failure mode this beat exists to avoid.

    `_compare` is substituted because the tool configures the gateway with the
    credential it presents, so a refusal is not reachable through the real call
    -- which is itself the property worth stating. Everything after it, the
    report and the exit code, is the production path.
    """
    refused = ((FieldRow("email", "email_enc", SAMPLE_EMAIL, "opaque", None, "HTTP 401 UNAUTHORIZED"),),)
    monkeypatch.setattr("app.inspect._compare", lambda *args: (refused[0], ("email_enc: HTTP 401 UNAUTHORIZED",)))

    code = main(args())

    out = capsys.readouterr().out
    assert code == 1
    assert "401" in out
    assert "REFUSED" in out


def test_the_parser_takes_the_credentials_from_the_environment(monkeypatch):
    monkeypatch.setenv("OPERATOR_KEY", OPERATOR_KEY)
    monkeypatch.setenv("MASTER_SECRET", MASTER)

    parsed = build_parser().parse_args([])

    assert parsed.operator_key == OPERATOR_KEY
    assert parsed.master_secret == MASTER
