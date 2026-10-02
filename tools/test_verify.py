"""T-R3: the reconciliation oracle -- ground truth against what actually happened.

Everything here runs without a broker, which is the point: the arithmetic is a pure
function over plain data (`parse_*` then `reconcile`) and only the topic read sits
behind a seam (`TopicReader`, faked by `FakeReader`). A verifier that can only be
exercised against live infrastructure is a verifier nobody runs before the demo.
"""

from __future__ import annotations

import json
import subprocess
import sys

import msgspec
import pytest

from app.config import TOPIC_DLQ, TOPIC_RAW
from app.dlq.envelope import build_dlq_event
from app.validate.events import CODE_DUPLICATE_ID, CODE_SCHEMA
from contracts.cloudevent import CandidateMetadata, CloudEvent, Data, EventPayload
from contracts.ledger import Ledger, LedgerRecord
from tools.verify import (
    DEFAULT_GROUP_ROWS,
    EXIT_MISMATCH,
    EXIT_UNVERIFIED,
    BrokerUnreachable,
    DlqRecord,
    KafkaTopicReader,
    RawRecord,
    TopicMessage,
    TopicSnapshot,
    derive_expected_pseudonyms,
    main,
    ordering_violations,
    parse_dlq_messages,
    parse_raw_messages,
    reconcile,
    render_scorecard,
)

SOURCE = "/careers/tenant_0001"
OTHER_SOURCE = "/careers/tenant_0002"
TYPE_VIEWED = "com.careerpage.career.job-viewed"
TYPE_SUBMITTED = "com.careerpage.career.application-submitted"
PSEUDO = "a" * 64
OTHER_PSEUDO = "b" * 64
EVENT_IDS = tuple(f"01J{index:024X}" for index in range(1, 10))


# --- builders -----------------------------------------------------------------


def egress(
    *,
    source: str = SOURCE,
    event_id: str = EVENT_IDS[0],
    event_type: str = TYPE_VIEWED,
    pseudo: str = PSEUDO,
    sequence: str = "0000000001",
) -> bytes:
    """One record as `app.ingest.pipeline._encrypt` publishes it."""
    return msgspec.json.encode(
        CloudEvent(
            specversion="1.0",
            id=event_id,
            source=source,
            type=event_type,
            time="2026-09-30T14:43:09.123Z",
            data=Data(
                candidate=CandidateMetadata(user_id_pseudo=pseudo),
                event_payload=EventPayload(job_id="job_12345", session_id="sess_1"),
            ),
            sequence=sequence,
        )
    )


def dlq_record(
    *,
    source: str = SOURCE,
    payload: dict | None = None,
    reason: str = f"{CODE_SCHEMA} at $.type",
    event_id: str = EVENT_IDS[0],
    event_type: str = TYPE_VIEWED,
) -> bytes:
    """One DLQ record as `app.dlq.envelope.build_dlq_event` writes it."""
    rejected = (
        {"source": source, "id": event_id, "type": event_type} if payload is None else payload
    )
    return msgspec.json.encode(build_dlq_event(rejected, reason=reason, index=0))


def ledger_record(
    *,
    event_id: str = EVENT_IDS[0],
    source: str = SOURCE,
    event_type: str = TYPE_VIEWED,
    user: str = "usr_0001abcd",
    seq: int = 1,
) -> LedgerRecord:
    return LedgerRecord(
        id=event_id,
        source=source,
        type=event_type,
        tenant=source.rsplit("/", 1)[-1],
        user_pseudo=user,
        seq=seq,
    )


def raw(*values: bytes) -> list[RawRecord]:
    records, undecodable = parse_raw_messages(_messages(TOPIC_RAW, *values))
    assert undecodable == 0
    return records


def dlq(*values: bytes) -> list[DlqRecord]:
    records, undecodable = parse_dlq_messages(_messages(TOPIC_DLQ, *values))
    assert undecodable == 0
    return records


def _messages(topic: str, *values: bytes) -> list[TopicMessage]:
    """One message per value, on ascending offsets of partition 0."""
    return [
        TopicMessage(topic=topic, partition=index % 3, offset=index, value=value)
        for index, value in enumerate(values)
    ]


class FakeReader:
    """A `TopicReader` over messages built in a test. No broker, no network."""

    def __init__(self, raw_messages: list[TopicMessage], dlq_messages: list[TopicMessage]) -> None:
        self.topics = {TOPIC_RAW: raw_messages, TOPIC_DLQ: dlq_messages}
        self.drained = True
        self.reads: list[str] = []

    def read(self, topic: str, *, timeout: float) -> TopicSnapshot:
        self.reads.append(topic)
        if topic not in self.topics:
            raise BrokerUnreachable(f"no such topic: {topic}")
        return TopicSnapshot(topic=topic, messages=tuple(self.topics[topic]), drained=self.drained)


# --- parsing ------------------------------------------------------------------


def test_a_published_record_parses_to_the_fields_reconciliation_joins_on():
    records, undecodable = parse_raw_messages(
        _messages(TOPIC_RAW, egress(pseudo=PSEUDO, sequence="0000000042"))
    )

    assert undecodable == 0
    assert records == [
        RawRecord(
            source=SOURCE,
            id=EVENT_IDS[0],
            type=TYPE_VIEWED,
            user_pseudo=PSEUDO,
            seq=42,
            partition=0,
            offset=0,
        )
    ]


def test_a_dlq_record_yields_its_code_and_the_event_it_rejected():
    records, undecodable = parse_dlq_messages(_messages(TOPIC_DLQ, dlq_record()))

    assert undecodable == 0
    assert records == [
        DlqRecord(
            source=SOURCE,
            code=CODE_SCHEMA,
            rejected_source=SOURCE,
            rejected_id=EVENT_IDS[0],
            rejected_type=TYPE_VIEWED,
            partition=0,
            offset=0,
        )
    ]


def test_a_reason_that_is_a_code_and_a_sentence_still_yields_only_the_code():
    # `MIXED_TENANT: spans more than one tenant` -- the code is followed by a colon,
    # not a space, so taking the first whitespace-separated token would leave the
    # punctuation attached and the breakdown would carry two names for one thing.
    records, _ = parse_dlq_messages(
        _messages(TOPIC_DLQ, dlq_record(reason="MIXED_TENANT: spans more than one tenant"))
    )

    assert [r.code for r in records] == ["MIXED_TENANT"]


def test_a_message_that_does_not_decode_is_counted_not_raised():
    # Dropping it would turn a record nobody can vouch for into a record the ledger
    # never emitted, which reads as loss rather than as a broken decoder.
    records, undecodable = parse_raw_messages(
        _messages(TOPIC_RAW, egress(), b'{"not": "an event"}', b"not json at all")
    )

    assert len(records) == 1
    assert undecodable == 2


def test_a_dlq_record_with_no_original_payload_cannot_be_joined_to_the_ledger():
    # `_dlq_payload` returns {} when the request element was not an object, so this is
    # a shape the pipeline really produces, not a hypothetical one.
    records, undecodable = parse_dlq_messages(
        _messages(TOPIC_DLQ, msgspec.json.encode(build_dlq_event({}, reason="SCHEMA at $")))
    )

    assert undecodable == 0
    assert records[0].rejected_id is None


# --- reconciliation: the clean run -------------------------------------------


def test_a_clean_run_reconciles_to_zero_everywhere():
    ledger = [
        ledger_record(),
        ledger_record(event_id=EVENT_IDS[1], event_type=TYPE_SUBMITTED, seq=2),
        ledger_record(event_id=EVENT_IDS[2], seq=3),
    ]
    observed = raw(
        egress(),
        egress(event_id=EVENT_IDS[1], event_type=TYPE_SUBMITTED, sequence="0000000002"),
        egress(event_id=EVENT_IDS[2], sequence="0000000003"),
    )

    result = reconcile(ledger, observed, [])

    assert result.ok
    assert result.failures == ()
    assert (result.ledger_rows, result.expected, result.stored) == (3, 3, 3)
    assert (result.wire_duplicates, result.missing, result.unexpected) == (0, 0, 0)
    assert (result.dlq, result.ledger_duplicates) == (0, 0)
    assert (result.ordering_violations, result.seq_mismatches) == (0, 0)
    assert result.pseudonym_mismatches is None  # unmeasured is not the same as zero


def test_reconciliation_reports_the_offsets_it_read_from():
    observed = [
        RawRecord(SOURCE, EVENT_IDS[0], TYPE_VIEWED, PSEUDO, 1, 3, 91),
        RawRecord(SOURCE, EVENT_IDS[1], TYPE_VIEWED, PSEUDO, 2, 3, 92),
    ]

    result = reconcile([ledger_record(), ledger_record(event_id=EVENT_IDS[1], seq=2)], observed, [])

    assert result.stored_partitions == {3: 2}
    assert result.stored_end_offsets == {3: 93}


# --- rejected on purpose vs never arrived ------------------------------------


def test_an_event_the_gateway_rejected_on_purpose_is_accounted_for_by_the_dlq():
    ledger = [ledger_record(), ledger_record(event_id=EVENT_IDS[1], seq=2)]
    observed = raw(egress(event_id=EVENT_IDS[1], sequence="0000000002"))

    result = reconcile(ledger, observed, dlq(dlq_record()), expected_dlq=1)

    assert result.ok
    assert result.missing == 0
    assert result.accepted == 2
    assert (result.dlq, result.dlq_by_code) == (1, {CODE_SCHEMA: 1})


def test_an_event_that_neither_the_topic_nor_the_dlq_holds_is_missing():
    ledger = [ledger_record(), ledger_record(event_id=EVENT_IDS[1], seq=2)]
    observed = raw(egress(event_id=EVENT_IDS[1], sequence="0000000002"))

    result = reconcile(ledger, observed, [])

    assert not result.ok
    assert result.missing == 1
    assert result.accepted == 1
    assert any("neither stored nor rejected" in f for f in result.failures)


def test_a_dlq_depth_that_does_not_match_what_was_injected_fails_the_run():
    result = reconcile([ledger_record()], [], dlq(dlq_record()), expected_dlq=7)

    assert not result.ok
    assert result.dlq == 1
    assert any("expected 7" in f for f in result.failures)


def test_a_dlq_code_the_gateway_never_defines_is_reported():
    result = reconcile([ledger_record()], [], dlq(dlq_record(reason="TEAPOT at $.type")), expected_dlq=1)

    assert result.dlq_by_code == {"TEAPOT": 1}
    assert not result.ok
    assert any("TEAPOT" in f for f in result.failures)


def test_a_dlq_record_that_cannot_be_attributed_to_the_ledger_is_reported():
    # The gateway rejected something the ground truth never mentions: either the
    # ledger is incomplete or the rejection is not ours to explain.
    result = reconcile([ledger_record()], [], dlq(dlq_record(event_id=EVENT_IDS[7])), expected_dlq=1)

    assert not result.ok
    assert result.unattributed_dlq == 1
    assert any("could not be attributed" in f for f in result.failures)


def test_a_stored_event_the_ledger_never_sent_is_unexpected():
    result = reconcile([], raw(egress()), [])

    assert not result.ok
    assert result.unexpected == 1


# --- duplicates, scoped after (source, id) dedup ------------------------------


def test_wire_duplicates_are_counted_after_dedup_and_do_not_inflate_stored():
    # Chaos 1 kills the gateway mid-flight and the driver resends the batch, so one
    # (source, id) reaches the topic twice by construction. `stored` counts distinct
    # keys and only the extra copy is a duplicate -- counting rows would report the
    # kill-and-resend as a delivery twice over.
    observed = raw(egress(), egress())

    result = reconcile([ledger_record()], observed, [])

    assert (result.stored, result.wire_duplicates) == (1, 1)
    assert not result.ok
    assert any("1 wire duplicate" in f for f in result.failures)


def test_wire_duplicates_the_run_expects_are_reported_but_not_a_failure():
    result = reconcile([ledger_record()], raw(egress(), egress()), [], expected_duplicates=1)

    assert result.ok
    assert result.wire_duplicates == 1


def test_a_duplicate_id_injection_leaves_the_ledger_holding_a_repeated_key():
    # `driver.inject` copies the partner's id onto the corrupted event, so both the
    # clean event and the injected one sit in the ledger under one (source, id).
    # Counting rows instead of distinct keys would report a phantom loss of one.
    partner = ledger_record(seq=7)
    injected = ledger_record(seq=7)
    observed = raw(egress(sequence="0000000007"))

    result = reconcile([partner, injected], observed, dlq(dlq_record(reason=f"{CODE_DUPLICATE_ID} at $.id")), expected_dlq=1)

    assert result.ok
    assert (result.ledger_rows, result.expected, result.ledger_duplicates) == (2, 1, 1)
    assert (result.stored, result.missing) == (1, 0)


# --- per-source and per-type deltas ------------------------------------------


def test_deltas_are_reported_per_source_and_per_type():
    ledger = [
        ledger_record(),
        ledger_record(event_id=EVENT_IDS[1], event_type=TYPE_SUBMITTED, seq=2),
        ledger_record(event_id=EVENT_IDS[2], source=OTHER_SOURCE, seq=3),
        ledger_record(event_id=EVENT_IDS[3], source=OTHER_SOURCE, seq=4),
    ]
    observed = raw(
        egress(),
        egress(event_id=EVENT_IDS[1], event_type=TYPE_SUBMITTED, sequence="0000000002"),
        egress(event_id=EVENT_IDS[2], source=OTHER_SOURCE, sequence="0000000003"),
    )

    rejected_here = dlq(dlq_record(source=OTHER_SOURCE, event_id=EVENT_IDS[3]))
    result = reconcile(ledger, observed, rejected_here, expected_dlq=1)

    assert result.ok
    sources = {group.key: group for group in result.by_source}
    assert (sources[OTHER_SOURCE].sent, sources[OTHER_SOURCE].stored) == (2, 1)
    assert (sources[OTHER_SOURCE].rejected, sources[OTHER_SOURCE].delta) == (1, -1)
    assert sources[OTHER_SOURCE].unresolved == 0
    types = {group.key: group for group in result.by_type}
    # Three `job-viewed` events went out, two of them are on the topic and the third
    # is in the DLQ -- so the type's delta is -1 and its unresolved count is 0.
    assert (types[TYPE_VIEWED].sent, types[TYPE_VIEWED].stored) == (3, 2)
    assert (types[TYPE_VIEWED].rejected, types[TYPE_VIEWED].unresolved) == (1, 0)
    assert (types[TYPE_SUBMITTED].sent, types[TYPE_SUBMITTED].delta) == (1, 0)


def test_a_group_that_does_not_balance_is_reported_as_unresolved():
    ledger = [ledger_record(event_id=EVENT_IDS[8], seq=9)]
    observed = raw(egress())

    result = reconcile(ledger, observed, [])

    assert result.by_source[0].unresolved == 1
    assert not result.ok


def test_a_group_that_exists_only_on_the_topic_still_gets_a_row():
    result = reconcile([], raw(egress()), [])

    assert result.by_source[0].sent == 0
    assert result.by_source[0].stored == 1


# --- ordering and sequence fidelity -------------------------------------------


def test_a_sequence_going_backwards_for_one_user_is_an_ordering_violation():
    observed = raw(
        egress(sequence="0000000005"),
        egress(event_id=EVENT_IDS[1], sequence="0000000004"),
        egress(event_id=EVENT_IDS[2], pseudo=OTHER_PSEUDO, sequence="0000000001"),
    )

    assert ordering_violations(observed) == 1
    assert reconcile([], observed, []).ordering_violations == 1


def test_a_resent_duplicate_is_not_an_ordering_violation():
    # The same rule `app.metrics._SequenceWindow` applies: equal to the high-water
    # mark is progress, lower than it is not. A retry that republishes an event
    # verbatim is the former, and the verifier has to agree with the gateway's own
    # counter rather than invent a second definition of "out of order".
    observed = raw(egress(sequence="0000000003"), egress(sequence="0000000003"))

    assert ordering_violations(observed) == 0


def test_a_record_with_no_comparable_sequence_is_unchecked_not_a_violation():
    # `gateway_ordering_unchecked_total` exists for exactly this distinction.
    observed = raw(egress(sequence=None))

    assert ordering_violations(observed) == 0
    result = reconcile([ledger_record(seq=1)], observed, [])
    assert (result.ordering_unchecked, result.ordering_violations) == (1, 0)


def test_a_stored_sequence_that_disagrees_with_the_ledger_is_reported():
    observed = raw(egress(sequence="0000000099"))

    result = reconcile([ledger_record(seq=1)], observed, [])

    assert result.seq_mismatches == 1
    assert not result.ok


# --- pseudonym reconciliation --------------------------------------------------


def test_the_ledger_plaintext_user_id_is_joined_through_the_publishers_hmac():
    # The ledger field is named `user_pseudo` and holds the INGRESS plaintext
    # `user_id`; the topic carries `pseudonymize(mac_key, user_id)`. The two are
    # joined by re-deriving the HMAC from the master secret, not by comparing strings
    # -- and not by comparing the ledger's plaintext to the topic's tag, which would
    # report a total loss on a run where nothing was lost.
    ledger = [ledger_record(user="usr_0001abcd")]
    observed = raw(egress(pseudo=PSEUDO))
    expected = derive_expected_pseudonyms(ledger, master_secret=b"k" * 32)
    from app.crypto.keys import derive_tenant_keys
    from app.pseudonym.hmac import pseudonymize

    tag = pseudonymize(
        derive_tenant_keys(b"k" * 32, b"tenant_0001", career_site_id="tenant_0001", key_version=1).mac_key,
        "usr_0001abcd",
    )
    assert expected == {(SOURCE, EVENT_IDS[0]): tag}

    result = reconcile(ledger, raw(egress(pseudo=tag)), [], expected_pseudonyms=expected)
    assert result.ok
    assert (result.pseudonyms_checked, result.pseudonym_mismatches) == (1, 0)


def test_a_topic_pseudonym_that_is_not_the_expected_hmac_is_a_mismatch():
    ledger = [ledger_record(user="usr_0001abcd")]

    result = reconcile(ledger, raw(egress(pseudo=OTHER_PSEUDO)), [], expected_pseudonyms=derive_expected_pseudonyms(ledger, b"k" * 32))

    assert result.pseudonym_mismatches == 1
    assert not result.ok


def test_a_master_secret_too_short_to_derive_keys_is_refused():
    with pytest.raises(ValueError):
        derive_expected_pseudonyms([ledger_record()], master_secret=b"short")


# --- reads we could not do ----------------------------------------------------


def test_a_read_that_stopped_before_the_end_offsets_is_not_a_pass():
    result = reconcile([ledger_record()], raw(egress()), [], drained=False)

    assert not result.ok
    assert any("end offsets" in failure for failure in result.failures)


def test_undecodable_messages_are_a_failure_not_a_zero():
    result = reconcile([ledger_record()], [], [], undecodable=3)

    assert not result.ok
    assert result.undecodable == 3
    assert any("could not be decoded" in failure for failure in result.failures)


# --- the scorecard ------------------------------------------------------------


def test_the_scorecard_is_one_screen_and_names_every_headline_number():
    ledger = [ledger_record(), ledger_record(event_id=EVENT_IDS[1], seq=2)]
    observed = raw(egress(event_id=EVENT_IDS[1], sequence="0000000002"))

    text = render_scorecard(
        reconcile(ledger, observed, dlq(dlq_record()), expected_dlq=1), ledger_path="ledger.jsonl"
    )

    for heading in ("sent", "accepted", "stored", "duplicates", "dlq"):
        assert heading in text
    assert "by code" in text
    assert "end offsets p0=1" in text
    assert "PASS" in text
    assert len(text.splitlines()) < 40


def test_the_scorecard_scopes_the_duplicate_number_to_after_dedup():
    text = render_scorecard(reconcile([ledger_record()], raw(egress()), []), ledger_path="ledger.jsonl")

    assert "after (source, id) dedup" in text
    assert "kill-gateway" in text


def test_the_scorecard_says_a_skipped_dimension_was_skipped_not_that_it_was_zero():
    ledger = [ledger_record()]
    unmeasured = render_scorecard(reconcile(ledger, raw(egress()), []), ledger_path="ledger.jsonl")
    assert "pseudonyms  skipped" in unmeasured

    expected = derive_expected_pseudonyms(ledger, b"k" * 32)
    measured = render_scorecard(
        reconcile(ledger, raw(egress(pseudo=next(iter(expected.values())))), [], expected_pseudonyms=expected),
        ledger_path="ledger.jsonl",
    )
    assert "pseudonyms  1 checked, 0 mismatched" in measured


def test_the_scorecard_caps_the_group_tables_and_verbose_prints_them_all():
    ledger = [ledger_record(source=f"/careers/tenant_{n:04d}") for n in range(1, 21)]
    result = reconcile(ledger, [], [])

    default = render_scorecard(result, ledger_path="ledger.jsonl")
    verbose = render_scorecard(result, ledger_path="ledger.jsonl", verbose=True)

    assert f"sources  {20} ({DEFAULT_GROUP_ROWS} shown" in default
    assert f"{20 - DEFAULT_GROUP_ROWS} more sources" in default
    # Exactly the capped rows are the difference: 12 sources are summarised away.
    assert len(verbose.splitlines()) - len(default.splitlines()) == 20 - DEFAULT_GROUP_ROWS
    assert "/careers/tenant_0020" in verbose


def test_the_json_report_round_trips():
    result = reconcile([ledger_record()], raw(egress()), [])

    document = json.loads(json.dumps(result.to_dict()))

    assert document["ok"] is True
    assert document["counts"]["stored"] == 1
    assert document["by_source"][0]["key"] == SOURCE


# --- the CLI ------------------------------------------------------------------


def write_ledger(tmp_path, records: list[LedgerRecord]) -> str:
    path = tmp_path / "ledger.jsonl"
    with Ledger(path) as ledger:
        for record in records:
            ledger.append(record)
    return str(path)


def test_the_cli_passes_on_a_clean_run(tmp_path, capsys):
    path = write_ledger(tmp_path, [ledger_record(), ledger_record(event_id=EVENT_IDS[1], seq=2)])
    reader = FakeReader(
        _messages(TOPIC_RAW, egress(), egress(event_id=EVENT_IDS[1], sequence="0000000002")), []
    )

    code = main(["--ledger", path, "--expect-dlq", "0"], reader=reader)

    assert code == 0
    assert reader.reads == [TOPIC_RAW, TOPIC_DLQ]
    assert "PASS" in capsys.readouterr().out


def test_the_cli_fails_when_the_dlq_does_not_match_the_injected_count(tmp_path, capsys):
    path = write_ledger(tmp_path, [ledger_record()])
    reader = FakeReader([], _messages(TOPIC_DLQ, dlq_record()))

    code = main(["--ledger", path, "--expect-dlq", "0"], reader=reader)

    assert code == EXIT_MISMATCH
    assert "FAIL" in capsys.readouterr().out


def test_the_cli_prints_json_when_asked(tmp_path, capsys):
    path = write_ledger(tmp_path, [ledger_record()])
    reader = FakeReader(_messages(TOPIC_RAW, egress()), [])

    code = main(["--ledger", path, "--json"], reader=reader)

    assert code == 0
    assert json.loads(capsys.readouterr().out)["ok"] is True


def test_a_broker_that_cannot_be_read_exits_non_zero_and_prints_no_scorecard(tmp_path, capsys):
    path = write_ledger(tmp_path, [ledger_record()])
    reader = FakeReader(_messages(TOPIC_RAW, egress()), [])
    reader.drained = False

    code = main(["--ledger", path], reader=reader)

    out = capsys.readouterr().out
    assert code == EXIT_UNVERIFIED
    assert "could not read Kafka" in out
    assert "PASS" not in out and "FAIL" not in out


def test_a_reader_that_raises_is_reported_as_a_broker_failure(tmp_path, capsys):
    path = write_ledger(tmp_path, [ledger_record()])

    class Broken:
        def read(self, topic: str, *, timeout: float) -> TopicSnapshot:
            raise OSError("no route to host")

    code = main(["--ledger", path], reader=Broken())

    out = capsys.readouterr().out
    assert code == EXIT_UNVERIFIED
    assert "could not read Kafka" in out


def test_an_empty_ledger_verifies_nothing_and_says_so(tmp_path, capsys):
    path = write_ledger(tmp_path, [])
    reader = FakeReader([], [])

    code = main(["--ledger", path], reader=reader)

    assert code == EXIT_UNVERIFIED
    assert "holds no rows" in capsys.readouterr().out


# --- the real read, against a fake client ------------------------------------
#
# The loop that decides `drained` is where a verifier quietly lies, so it is worth
# testing without a broker. The stand-in implements the slice of the
# `confluent_kafka.Consumer` surface `KafkaTopicReader` uses; the real client is
# never constructed here, and never at import.


class FakeMessage:
    def __init__(self, partition: int, offset: int, value: bytes, error=None) -> None:
        self._partition = partition
        self._offset = offset
        self._value = value
        self._error = error

    def error(self):
        return self._error

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes:
        return self._value


class FakeConsumer:
    """Watermarks per partition, and a fixed queue of what `poll` hands back."""

    def __init__(self, watermarks: dict[int, tuple[int, int]], messages: list[FakeMessage]) -> None:
        self.watermarks = watermarks
        self.queue = list(messages)
        self.assigned: list = []
        self.closed = False

    def list_topics(self, *, topic: str, timeout: float):
        return _Metadata({topic: _TopicMetadata(partitions=self.watermarks)})

    def get_watermark_offsets(self, partition, *, timeout: float, cached: bool) -> tuple[int, int]:
        return self.watermarks[partition.partition]

    def assign(self, partitions) -> None:
        self.assigned = list(partitions)

    def poll(self, timeout: float):
        return self.queue.pop(0) if self.queue else None

    def close(self) -> None:
        self.closed = True


class _TopicMetadata:
    def __init__(self, *, partitions: dict[int, tuple[int, int]]) -> None:
        self.partitions = partitions
        self.error = None


class _Metadata:
    def __init__(self, topics: dict) -> None:
        self.topics = topics


def reader_for(consumer: FakeConsumer) -> KafkaTopicReader:
    return KafkaTopicReader(
        "localhost:9092",
        timeout=0.05,
        poll_seconds=0.0,
        consumer_factory=lambda config: consumer,
    )


def test_a_read_that_reaches_every_partition_s_watermark_is_drained():
    consumer = FakeConsumer(
        {0: (0, 2), 1: (0, 1)},
        [FakeMessage(0, 0, b"a"), FakeMessage(1, 0, b"b"), FakeMessage(0, 1, b"c")],
    )

    snapshot = reader_for(consumer).read(TOPIC_RAW)

    assert snapshot.drained
    assert [m.offset for m in snapshot.messages] == [0, 0, 1]
    assert [m.partition for m in snapshot.messages] == [0, 1, 0]
    assert consumer.closed


def test_a_read_that_stops_short_is_not_drained():
    # The verdict here decides whether a scorecard is printed at all, so it is the
    # one line in the read loop that gets its own test.
    consumer = FakeConsumer({0: (0, 5)}, [FakeMessage(0, 0, b"a")])

    snapshot = reader_for(consumer).read(TOPIC_RAW)

    assert not snapshot.drained
    assert len(snapshot.messages) == 1


def test_an_empty_topic_is_drained_and_holds_nothing():
    snapshot = reader_for(FakeConsumer({}, [])).read(TOPIC_DLQ)

    assert (snapshot.drained, snapshot.messages) == (True, ())


def test_a_topic_the_broker_does_not_have_is_a_broker_failure():
    consumer = FakeConsumer({0: (0, 1)}, [])
    consumer.list_topics = lambda **kwargs: _Metadata({})

    with pytest.raises(BrokerUnreachable):
        reader_for(consumer).read(TOPIC_RAW)


def test_a_client_failure_mid_read_is_a_broker_failure_and_the_consumer_is_closed():
    consumer = FakeConsumer({0: (0, 1)}, [FakeMessage(0, 0, b"a", error="broker died")])

    with pytest.raises(BrokerUnreachable):
        reader_for(consumer).read(TOPIC_RAW)

    assert consumer.closed


def test_a_truncated_ledger_line_is_reported_not_raised(tmp_path, capsys):
    # Chaos 1 kills the gateway; a kill during the driver's own write leaves the last
    # line half-written. A traceback here would hide the reason the demo was running
    # verify in the first place.
    path = tmp_path / "ledger.jsonl"
    path.write_text(
        '{"id":"01J1","source":"/careers/tenant_0001","type":"x","tenant":"tenant_0001",'
        '"user_pseudo":"usr_1","seq":1}\n{"id":"01J2","sour\n',
        encoding="utf-8",
    )

    code = main(["--ledger", str(path)], reader=FakeReader([], []))

    assert code == EXIT_UNVERIFIED
    assert "could not read the ledger" in capsys.readouterr().out


def test_a_missing_ledger_file_is_reported_before_any_topic_is_read(tmp_path, capsys):
    reader = FakeReader([], [])

    code = main(["--ledger", str(tmp_path / "absent.jsonl")], reader=reader)

    assert code == EXIT_UNVERIFIED
    assert "no such ledger" in capsys.readouterr().out
    assert reader.reads == []


def test_importing_the_module_does_not_load_the_kafka_client():
    # The Consumer must be constructible only when a read is actually asked for:
    # a module-level client would make `import tools.verify` need a broker.
    completed = subprocess.run(
        [sys.executable, "-c", "import sys, tools.verify; print('confluent_kafka' in sys.modules)"],
        capture_output=True,
        text=True,
        check=True,
    )

    assert completed.stdout.strip() == "False"