"""Ground-truth ledger (plan D8/M2).

The driver knows exactly what it emitted. This is what turns "we handled 50k/s"
into "we handled 50k/s and here is the receipt".

The schema is specified ONCE here. r3.1 carried three different schemas across
three documents; every claim in the plan (sent == accepted == stored, duplicates
== 0, DLQ == injected) depends on this being unambiguous.
"""

from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


def dedup_key(source: str, event_id: str) -> tuple[str, str]:
    """CloudEvents dedup rule: `source` + `id`.

    Deliberately not `id` alone. `id` is only unique within a source, so a shared
    ULID generator across tenants would false-positive on a per-id check while
    the real dedup is per-(source, id).
    """
    return (source, event_id)


@dataclass(frozen=True, slots=True)
class LedgerRecord:
    id: str
    source: str
    type: str
    tenant: str
    user_pseudo: str
    seq: int

    def to_json(self) -> str:
        return json.dumps(
            {
                "id": self.id,
                "source": self.source,
                "type": self.type,
                "tenant": self.tenant,
                "user_pseudo": self.user_pseudo,
                "seq": self.seq,
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, line: str) -> "LedgerRecord":
        d = json.loads(line)
        return cls(d["id"], d["source"], d["type"], d["tenant"], d["user_pseudo"], d["seq"])


class Ledger:
    """Append-only JSONL. 3M rows at peak, 15M at soak -- hence JSONL, not a
    list of dicts, and a streaming reader rather than a full load."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._fh = None

    def __enter__(self) -> "Ledger":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")
        return self

    def __exit__(self, *exc) -> None:
        if self._fh:
            self._fh.close()
            self._fh = None

    def append(self, rec: LedgerRecord) -> None:
        if self._fh is None:
            raise RuntimeError("use the ledger as a context manager")
        self._fh.write(rec.to_json() + "\n")

    def read_all(self) -> list[LedgerRecord]:
        if not self.path.exists():
            return []
        return [LedgerRecord.from_json(ln) for ln in self.path.read_text(encoding="utf-8").splitlines() if ln]

    def __len__(self) -> int:
        return len(self.read_all())

    @staticmethod
    def duplicates(records: list[LedgerRecord]) -> int:
        """Count records sharing a (source, id) pair with an earlier record."""
        counts = Counter(dedup_key(r.source, r.id) for r in records)
        return sum(n - 1 for n in counts.values() if n > 1)
