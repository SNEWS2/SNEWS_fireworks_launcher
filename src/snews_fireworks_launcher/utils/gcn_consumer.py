"""
GCN Verification Utility

DEVELOPER UTILITY for reading notices back from the real NASA GCN Kafka
broker (test or production), to confirm that the GCN Bridge is actually
publishing SNEWS 2.0 alerts as expected.
"""

import json
import logging
import os
from typing import Callable, Optional

from gcn_kafka import Consumer as GCNConsumer

logger = logging.getLogger(__name__)


class GCNKafkaConsumer:
    """
    Kafka consumer for reading SNEWS2 GCN notices back from GCN's own broker.
    """

    DEFAULT_TOPIC = "gcn.notices.snews2.alert"
    DEFAULT_DOMAIN = "test.gcn.nasa.gov"

    def __init__(
        self,
        client_id: str = None,
        client_secret: str = None,
        topic: str = None,
        domain: str = None,
        group_id: str = None,
        auto_offset_reset: str = "earliest",
        on_message: Optional[Callable[[dict], None]] = None,
    ):
        self.client_id = client_id or os.getenv("GCN_CONSUMER_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("GCN_CONSUMER_CLIENT_SECRET")
        self.topic = topic or os.getenv("SNEWS2_GCN_TOPIC", self.DEFAULT_TOPIC)
        self.domain = domain or os.getenv("GCN_DOMAIN", self.DEFAULT_DOMAIN)
        self.group_id = group_id or os.getenv("GCN_CONSUMER_GROUP", "snews2-gcn-verify")
        self.on_message = on_message

        if not self.client_id or not self.client_secret:
            raise RuntimeError(
                "GCN_CONSUMER_CLIENT_ID / GCN_CONSUMER_CLIENT_SECRET are not set (check your .env)."
            )

        self._consumer = GCNConsumer(
            client_id=self.client_id,
            client_secret=self.client_secret,
            domain=self.domain,
            config={
                "group.id": self.group_id,
                "auto.offset.reset": auto_offset_reset,
            },
        )
        self._consumer.subscribe([self.topic])

        logger.info(
            f"GCN Consumer initialized: domain={self.domain}, topic={self.topic}, group={self.group_id}"
        )

    def consume(self, timeout: float = 1.0, max_messages: int = None) -> list:
        """
        Poll for notices from GCN.

        Returns:
            List of parsed JSON dicts for each received notice.
        """
        messages = []
        count = 0

        try:
            while True:
                for msg in self._consumer.consume(timeout=timeout):
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue

                    try:
                        value = json.loads(msg.value().decode("utf-8"))
                    except (ValueError, UnicodeDecodeError) as e:
                        logger.error(f"Failed to decode message: {e}")
                        continue

                    logger.info(f"Received notice: topic={msg.topic()}, offset={msg.offset()}")
                    messages.append(value)

                    if self.on_message:
                        self.on_message(value)

                    count += 1
                    if max_messages and count >= max_messages:
                        return messages

                if max_messages is None:
                    return messages
        except KeyboardInterrupt:
            logger.info("Consumer interrupted by user")

        return messages

    @staticmethod
    def pretty_print(notice: dict) -> None:
        """Print a SNEWS2GCNNotice JSON payload in a readable form."""
        alert = notice.get("alert", {})
        event = notice.get("event", {})
        reporter = notice.get("reporter", {})
        dt = notice.get("datetime", {})

        print("\n" + "=" * 60)
        print(f"GCN Notice [{alert.get('alert_tense', '?').upper()}] {alert.get('alert_type', '?')}")
        print("=" * 60)
        print(f"Event ID:      {event.get('id')}")
        print(f"Mission:       {reporter.get('mission')}")
        print(f"Trigger Time:  {dt.get('trigger_time')}")
        print(f"Detectors:     {notice.get('detector_names')}")
        print(f"SNEWS2 Tier:   {notice.get('snews2_tier')}")
        if notice.get("statistics"):
            print(f"FAR:           {notice['statistics'].get('far')}")
        print("=" * 60 + "\n")

    def list_topics(self, timeout: float = 10.0) -> list:
        """List all topics visible to these credentials on this GCN domain."""
        metadata = self._consumer.list_topics(timeout=timeout)
        return sorted(metadata.topics.keys())

    def close(self) -> None:
        self._consumer.close()
        logger.info("GCN Consumer closed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False
