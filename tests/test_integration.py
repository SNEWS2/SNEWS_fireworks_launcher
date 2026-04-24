"""
Integration tests for SNEWS 2.0 Kafka Producer/Consumer.

Requires running Kafka instance (localhost:9092).
"""

import pytest
import time
import uuid
import os
from snews_fireworks_launcher.utils.snews2_producer import SNEWS2KafkaProducer, CoincidenceTierAlert
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
            now = "2025-01-15T14:30:00.123456+00:00"
            msg = CoincidenceTierAlert(
                id=f"SNEWS_Coincidence_ALERT {now}",
                alert_type="TEST COINC_MSG",
                server_tag="test-server",
                detector_names=[detector_name],
                neutrino_times=[now],
                sent_time=now,
                p_values=[0.1],
                p_values_average=0.1,
                sub_list_number=0,
                false_alarm_prob=0.01,
            )
            producer.send_message(msg)
            producer.flush()
        
        # 2. Consume the message
        received_msg = None
        
        # Give Kafka a moment to settle
        time.sleep(1)
        
        with SNEWS2KafkaConsumer(
            topic=test_topic, 
            auto_offset_reset="earliest",
            group_id=f"test-group-{uuid.uuid4()}"
        ) as consumer:
            # Try to consume for up to 5 seconds
            received_messages = consumer.consume(timeout_ms=5000, max_messages=1)
            if received_messages:
                received_msg = received_messages[0]
        
        # 3. Verify
        assert received_msg is not None, "Failed to consume SNEWS2 message"
        assert received_msg.detector_names == [detector_name]
        assert "TEST" in received_msg.alert_type

if __name__ == "__main__":
    # Allow running directly
    pytest.main([__file__, "-v"])
