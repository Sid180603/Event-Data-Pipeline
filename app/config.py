"""Gateway configuration.

Every limit here is a contract value published to the UI team in
`contracts/CONTRACT.md` (plan D2). Keep them in one place so the docs and the
code cannot drift.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# --- Contract limits (plan D1/D2) -------------------------------------------

MAX_EVENTS_PER_BATCH = 500
MAX_BATCH_BYTES = 4 * 1024 * 1024  # 4 MiB, enforced before allocation (H9)

# CloudEvents HTTP binding: intermediaries MUST forward events <= 64 KiB.
# Checked on the FINAL serialized event, post-encryption, because base64
# expands ciphertext ~33% plus nonce+tag per field (plan G2).
MAX_EVENT_BYTES = 64 * 1024

#: Ingress cap. Provisional: 48 KiB plaintext, which with base64 expansion and
#: five encrypted fields lands just under MAX_EVENT_BYTES. T6 measures the real
#: figure and this becomes a measured constant rather than an estimate.
INGRESS_EVENT_BYTES = 48 * 1024

# --- Kafka (plan D11) --------------------------------------------------------


def kafka_producer_config(
    bootstrap_servers: str, client_id: str = "event-gateway"
) -> dict:
    """Producer config with EVERY value explicit.

    Plan D11: no default may be relied upon, because `confluent-kafka` wraps
    librdkafka rather than the Java client whose defaults were researched, and
    a silent divergence would void our ordering and dedup claims.

    `enable.idempotence=false` is forbidden: with it off, retries produce
    duplicates and reordering with no error raised by any client.
    """
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "retries": 2_147_483_647,
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "zstd",
        # `compression.level`, NOT `compression.zstd.level`. The name that reads
        # correctly is not a librdkafka property -- `Producer()` raises
        # `_INVALID_ARG` on it, so the gateway does not start at all rather than
        # degrading. The level is not codec-scoped in librdkafka: one knob covers
        # every codec, and the two spellings of the codec itself (`compression.type`
        # and `compression.codec`) are accepted as aliases. Pinned by
        # `test_every_published_key_is_a_property_librdkafka_has`, which builds a
        # real client per key.
        "compression.level": 3,
        "batch.size": 262_144,
        "linger.ms": 10,
        "queue.buffering.max.kbytes": 65_536,
        "message.timeout.ms": 5_000,  # not delivery.timeout.ms: librdkafka name
        "queuing.strategy": "fifo",
        "partitioner": "consistent_random",  # hashes the key we supply
    }


TOPIC_RAW = "career.events.raw"
TOPIC_DLQ = "career.events.dlq"

#: Bounded producer queue. When full the handler returns 503 rather than
#: accepting work it cannot hold (plan C4).
PRODUCER_QUEUE_MAXSIZE = 50_000


@dataclass(frozen=True)
class Settings:
    """Runtime settings. Tests construct this directly; the app reads env."""

    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    )
    master_secret: bytes = field(
        default_factory=lambda: os.getenv("MASTER_SECRET", "").encode()
    )
    key_version: int = 1
    #: Sticky routing (plan D12). A (tenant, user) pair must land on one worker
    #: so that ordering and per-tenant rate limits are actually per-(tenant,user).
    sticky_routing: bool = field(
        default_factory=lambda: os.getenv("STICKY_ROUTING", "1") == "1"
    )
    jwt_public_key_pem: str = field(
        default_factory=lambda: os.getenv("JWT_PUBLIC_KEY_PEM", "")
    )
    jwt_algorithm: str = field(default_factory=lambda: os.getenv("JWT_ALG", "EdDSA"))
    jwt_audience: str = field(default_factory=lambda: os.getenv("JWT_AUD", "career-api"))

    def tenant_key_salt(self, career_site_id: str) -> bytes:
        return career_site_id.encode()
