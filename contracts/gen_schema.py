"""Regenerate event.schema.json from the msgspec structs.

    python -m contracts.gen_schema

The test suite fails if the committed file drifts from the structs.
"""

from __future__ import annotations

from pathlib import Path

from contracts.cloudevent import generate_schema_json

if __name__ == "__main__":
    out = Path(__file__).parent / "event.schema.json"
    out.write_text(generate_schema_json() + "\n", encoding="utf-8")
    print(f"wrote {out}")
