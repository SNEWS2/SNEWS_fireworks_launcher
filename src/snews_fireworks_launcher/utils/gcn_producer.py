"""
GCN Test Notice Utility

DEVELOPER UTILITY for publishing hand-built SNEWS2 GCN notices directly to
the real NASA GCN Kafka broker (test or production).

This intentionally does NOT go through
snews_fireworks_launcher.schemas.snews2_messages / snews2_gcn_schema or the
GCN Bridge - it builds a plain dict matching the SNEWS2GCNNotice JSON shape
by hand, so it keeps working even while the upstream snews-data-formats
schema mismatch (CoincidenceTierAlert) is unresolved.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from gcn_kafka import Producer as GCNProducer

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_test_gcn_notice(
    detector_names: Optional[list] = None,
    is_test: bool = True,
    far: Optional[float] = 1e-6,
) -> Dict[str, Any]:
    """
    Build a SNEWS2GCNNotice-shaped dict for a CoincidenceTier test alert.

    Mirrors the shape produced by
    snews_fireworks_launcher.schemas.snews2_gcn_schema.SNEWS2GCNNotice /
    transform_snews2_to_gcn, without importing that module.
    """
    now = _now_iso()
    detector_names = detector_names or ["Super-K", "IceCube"]
    event_id = str(uuid4())

    return {
        "alert": {
            "alert_datetime": now,
            "alert_tense": "test" if is_test else "current",
            "alert_type": "initial",
        },
        "event": {
            "id": event_id,
            "data_archive_page": None,
        },
        "reporter": {
            "mission": "SNEWS",
            "messenger": "Neutrino",
            "record_number": 1,
        },
        "datetime": {
            "trigger_time": now,
        },
        "statistics": {"far": far} if far is not None else None,
        "localization": None,
        "schema_version": "1.0",
        "snews2_tier": "CoincidenceTier",
        "detector_names": detector_names,
        "event_times_utc": [now for _ in detector_names],
        "tier_data": {
            "p_values": [0.05 for _ in detector_names],
            "false_alarm_prob": 0.0,
        },
    }


class GCNKafkaProducer:
    """
    Minimal producer for publishing pre-built GCN notice payloads directly
    to GCN's Kafka broker, with delivery confirmation.
    """

    DEFAULT_TOPIC = "gcn.notices.snews2.alert"
    DEFAULT_DOMAIN = "test.gcn.nasa.gov"

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        topic: str = None,
        domain: str = None,
    ):
        self.client_id = client_id or os.getenv("GCN_PRODUCER_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("GCN_PRODUCER_CLIENT_SECRET")
        self.topic = topic or os.getenv("SNEWS2_GCN_TOPIC", self.DEFAULT_TOPIC)
        self.domain = domain or os.getenv("GCN_DOMAIN", self.DEFAULT_DOMAIN)

        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "GCN_PRODUCER_CLIENT_ID / GCN_PRODUCER_CLIENT_SECRET are not set (check your .env)."
            )

        self._producer = GCNProducer(
            client_id=self.client_id,
            client_secret=self.client_secret,
            domain=self.domain,
        )

        logger.info(f"GCN Producer initialized: domain={self.domain}, topic={self.topic}")

    def send_notice(self, payload: Dict[str, Any], timeout: float = 10.0) -> Dict[str, Any]:
        """
        Publish a notice dict to GCN and block until delivery is confirmed.

        Returns:
            Dict with 'topic' and 'offset' on success.

        Raises:
            RuntimeError if delivery failed or timed out.
        """
        delivery_result: Dict[str, Any] = {}

        def _on_delivery(err, msg):
            if err is not None:
                delivery_result["error"] = err
            else:
                delivery_result["topic"] = msg.topic()
                delivery_result["offset"] = msg.offset()

        self._producer.produce(
            self.topic,
            json.dumps(payload).encode("utf-8"),
            callback=_on_delivery,
        )
        remaining = self._producer.flush(timeout=timeout)

        if remaining > 0:
            raise RuntimeError(f"GCN delivery timed out: {remaining} message(s) still in flight")
        if "error" in delivery_result:
            raise RuntimeError(f"GCN delivery failed: {delivery_result['error']}")

        logger.info(f"Delivered to GCN: topic={delivery_result['topic']}, offset={delivery_result['offset']}")
        return delivery_result

    def close(self) -> None:
        self._producer.flush(timeout=10.0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
