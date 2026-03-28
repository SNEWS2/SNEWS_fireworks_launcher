"""
GCN Bridge: SNEWS 2.0 to NASA GCN Pass-through Translation Service

This is the CORE component of the pipeline. It listens for live SNEWS 2.0 
alerts from the SCiMMA/Hopskotch network and immediately republishes them 
to the NASA GCN (either a local mock Docker container or the real 
production GCN Kafka brokers).
"""

import json
import logging
import os
import sys
from typing import Optional, Dict, Any

try:
    from gcn_kafka import Producer as GCNProducer
    HAS_GCN_KAFKA = True
except ImportError:
    HAS_GCN_KAFKA = False

from kafka import KafkaProducer

from .schemas.snews2_messages import parse_snews2_message, SNEWS2MessageBase
from .schemas.snews2_gcn_schema import transform_snews2_to_gcn, SNEWS2GCNNotice

logger = logging.getLogger(__name__)


class SNEWS2HopskotchListener:
    """
    Bridge between SCiMMA Hopskotch and GCN Kafka.
    
    This listener is designed to be run as an 'snews_pt' subscriber plugin.
    It receives SNEWS 2.0 JSON messages from the Hopskotch network, 
    validates them, transforms them into the unified GCN JSON format, 
    and republishes them to a local mock GCN (Kafka) or a real NASA GCN 
    endpoint.
    """
    DEFAULT_GCN_TOPIC = "snews2-gcn-alerts"
    
    def __init__(
        self,
        gcn_topic: Optional[str] = None,
        bootstrap_servers: Optional[str] = None,
        use_gcn_credentials: bool = False
    ):
        """
        Initialize the listener.
        
        Args:
            gcn_topic: Kafka topic to publish to.
            bootstrap_servers: Kafka bootstrap servers.
            use_gcn_credentials: If True, attempts to use GCN credentials via gcn-kafka.
        """
        self.gcn_topic = gcn_topic or os.getenv("SNEWS2_GCN_TOPIC", self.DEFAULT_GCN_TOPIC)
        self.bootstrap_servers = bootstrap_servers or os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
        )
        self.use_gcn_credentials = use_gcn_credentials or os.getenv("USE_GCN_CREDENTIALS", "false").lower() == "true"
        
        if self.use_gcn_credentials:
            if not HAS_GCN_KAFKA:
                logger.error("gcn-kafka not installed but USE_GCN_CREDENTIALS is True. Falling back to mock.")
                self.setup_mock_producer()
            else:
                self.setup_gcn_producer()
        else:
            self.setup_mock_producer()

    def setup_mock_producer(self):
        """Setup standard Kafka producer for local mock environment."""
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        logger.info(f"Initialized mock GCN KafkaProducer for topic {self.gcn_topic} at {self.bootstrap_servers}")

    def setup_gcn_producer(self):
        """Setup GCN producer for production environment."""
        # Note: GCNProducer from gcn-kafka uses client_id/client_secret from env/config
        self.producer = GCNProducer(
            client_id=os.getenv("GCN_CLIENT_ID"),
            client_secret=os.getenv("GCN_CLIENT_SECRET"),
            # gcn-kafka Producer handles its own serialization if needed, 
            # but usually it's better to pass bytes or dict depending on version
        )
        logger.info(f"Initialized real GCN Producer for topic {self.gcn_topic}")

    def process_message(self, message_data: Dict[str, Any]) -> Optional[SNEWS2GCNNotice]:
        """
        Processes a single Hopskotch JSON payload.
        
        1. Parses and validates against SNEWS2 models.
        2. Transforms the message into the unified GCN schema.
        3. Publishes the result to Kafka (Mock or Real).
        """
        try:
            # 1. Parse against SNEWS2 models
            snews2_msg = parse_snews2_message(message_data)
            logger.info(f"Received SNEWS2 {snews2_msg.tier} from {snews2_msg.detector_name}")
            
            # 2. Transform to unified GCN schema
            gcn_notice = transform_snews2_to_gcn(snews2_msg)
            
            # 3. Publish to Kafka
            # Key by primary detector to maintain order
            key = snews2_msg.detector_name
            payload = gcn_notice.model_dump(mode="json")
            
            if self.use_gcn_credentials and HAS_GCN_KAFKA:
                # Real GCN publishing
                self.producer.produce(self.gcn_topic, json.dumps(payload).encode("utf-8"))
            else:
                # Mock GCN publishing
                future = self.producer.send(self.gcn_topic, key=key, value=payload)
                future.get(timeout=10)
                
            logger.info(f"Successfully published SNEWS2 alert to '{self.gcn_topic}'")
            return gcn_notice
            
        except Exception as e:
            logger.error(f"Failed to process SNEWS2 message: {e}", exc_info=True)
            return None


def run_hopskotch_plugin():
    """
    Main entrypoint for the snews_pt subscriber plugin hook.
    
    When snews_pt is run with '-p src/snews2_hopskotch_listener.py', it
    calls this script as a subprocess, passing the path to a temporary
    JSON file containing the received message as the first argument.
    """
    if len(sys.argv) < 2:
        print("Error: path to JSON file not provided by snews_pt plugin hook.")
        sys.exit(1)
        
    json_path = sys.argv[1]
    
    try:
        with open(json_path, "r") as f:
            message_data = json.load(f)
            
        listener = SNEWS2HopskotchListener()
        listener.process_message(message_data)
        
    except Exception as e:
        print(f"Failed to process hopskotch plugin message: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # If run directly by snews_pt subscriber plugin system
    run_hopskotch_plugin()
