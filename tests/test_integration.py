"""
Integration tests for SNEWS 2.0 Kafka Producer/Consumer.

Requires running Kafka instance (localhost:9092).
"""

import pytest
import time
import uuid
import os
from snews_fireworks_launcher.utils.snews2_producer import SNEWS2KafkaProducer, CoincidenceTierMessage
from snews_fireworks_launcher.utils.snews2_consumer import SNEWS2KafkaConsumer

@pytest.mark.integration
class TestSNEWS2KafkaIntegration:
    """
    Integration tests for the SNEWS 2.0 Kafka pipeline.
    
    Verifies that messages can be produced to, and consumed from, a 
    live Kafka broker while maintaining schema integrity and 
    tier-specific properties.
    """
    
    def test_snews2_produce_consume_cycle(self):
        """Test full cycle: Produce SNEWS2 -> Kafka -> Consume SNEWS2"""
        # Unique topic for this test to avoid interference
        test_topic = f"snews2-test-{uuid.uuid4()}"
        
        # 1. Produce a message
        detector_name = "TestDetector"
        with SNEWS2KafkaProducer(topic=test_topic) as producer:
            msg = CoincidenceTierMessage(
                detector_name=detector_name,
                detector_names=[detector_name],
                neutrino_times_utc=["2025-01-15T14:30:00.123456+00:00"],
                p_values=[0.1],
                false_alarm_prob=0.01,
                is_test=True
            )
            producer.send_message(msg)
            producer.flush()
        
        # 2. Consume the message
        received_msg = None
        
        # Give Kafka a moment to settle
        time.sleep(3)
        
        with SNEWS2KafkaConsumer(
            topic=test_topic, 
            auto_offset_reset="earliest",
            group_id=f"test-group-{uuid.uuid4()}"
        ) as consumer:
            # Try to consume for up to 10 seconds
            received_messages = consumer.consume(timeout_ms=10000, max_messages=1)
            if received_messages:
                received_msg = received_messages[0]
        
        # 3. Verify
        assert received_msg is not None, "Failed to consume SNEWS2 message"
        assert received_msg.detector_name == detector_name
        assert received_msg.tier == "CoincidenceTier"
        assert received_msg.is_test is True

if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v"])
