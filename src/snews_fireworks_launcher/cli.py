"""
CLI Interface for SNEWS Kafka Pipeline

Commands for producing and consuming both legacy SNEWS and SNEWS2 notices.
"""

import argparse
import logging
import sys
import os

from dotenv import load_dotenv


def setup_logging(verbose: bool = False):
    """
    Configure the global logging settings for the CLI.
    
    Args:
        verbose: If True, set logging level to DEBUG. Otherwise, set to INFO.
    """
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ---------------------------------------------------------------------------
# SNEWS2 commands
# ---------------------------------------------------------------------------

SNEWS2_TIERS = ["heartbeat", "coincidence", "significance", "timing", "retraction"]


def _sample_summary(msg):
    """
    Describe a sample message for CLI output, handling both per-detector tier
    messages (have .tier/.detector_name/.uuid/.is_test) and the aggregated
    CoincidenceTierAlert shape (has .id/.detector_names/.alert_type instead).
    """
    from snews_fireworks_launcher.schemas.snews2_messages import CoincidenceTierAlert

    if isinstance(msg, CoincidenceTierAlert):
        return {
            "tier_display": "CoincidenceTier",
            "is_test": "TEST" in msg.alert_type.upper(),
            "detector": ", ".join(msg.detector_names),
            "uid": msg.id,
        }
    return {
        "tier_display": msg.tier.value if hasattr(msg.tier, "value") else msg.tier,
        "is_test": msg.is_test,
        "detector": msg.detector_name,
        "uid": msg.uuid,
    }


def cmd_snews2_produce(args):
    """
    Handle the 'snews2-produce' command.
    
    Generates a mock SNEWS 2.0 message for a specified tier and publishes it 
    to the configured Kafka topic.
    
    Args:
        args: Argparse namespace containing 'tier' and 'test' flag.
    """
    from snews_fireworks_launcher.utils.snews2_producer import SNEWS2KafkaProducer, SAMPLE_GENERATORS

    tier = args.tier
    if tier not in SAMPLE_GENERATORS:
        print(f"Unknown tier: {tier}. Choose from: {', '.join(SNEWS2_TIERS)}")
        sys.exit(1)

    with SNEWS2KafkaProducer() as producer:
        msg = SAMPLE_GENERATORS[tier](is_test=args.test)
        producer.send_message(msg)
        producer.flush()

        summary = _sample_summary(msg)
        test_label = " [TEST]" if summary["is_test"] else ""
        print(f"✓ Sent SNEWS2 {summary['tier_display']}{test_label}")
        print(f"  Detector:  {summary['detector']}")
        print(f"  UUID:      {summary['uid'][:12]}...")


def cmd_snews2_consume(args):
    """
    Handle the 'snews2-consume' command.
    
    Subscribes to SNEWS 2.0 alerts from Kafka and prints them to the console
    using a tier-aware formatter.
    
    Args:
        args: Argparse namespace containing optional 'count' limit.
    """
    from snews_fireworks_launcher.utils.snews2_consumer import SNEWS2KafkaConsumer

    def on_message(msg):
        SNEWS2KafkaConsumer.pretty_print(msg)

    topic = os.getenv("SNEWS2_TOPIC", "snews2-alerts")
    print(f"Subscribing to SNEWS2 alerts (Ctrl+C to stop)...")
    print(f"Topic: {topic}")
    print("-" * 60)

    with SNEWS2KafkaConsumer(on_message=on_message) as consumer:
        try:
            if args.count:
                messages = consumer.consume(max_messages=args.count)
                print(f"\n✓ Consumed {len(messages)} SNEWS2 messages")
            else:
                consumer.consume()
        except KeyboardInterrupt:
            print("\n\n✓ Consumer stopped")


def cmd_snews2_transform(args):
    """
    Handle the 'snews2-transform' command.
    
    Generates a sample SNEWS 2.0 message for a given tier and prints the
    validated JSON output to stdout. This does not require a Kafka broker.
    
    Args:
        args: Argparse namespace containing 'tier' and 'test' flag.
    """
    import json
    from snews_fireworks_launcher.utils.snews2_producer import SAMPLE_GENERATORS

    tier = args.tier
    if tier not in SAMPLE_GENERATORS:
        print(f"Unknown tier: {tier}. Choose from: {', '.join(SNEWS2_TIERS)}")
        sys.exit(1)

    msg = SAMPLE_GENERATORS[tier](is_test=args.test)
    summary = _sample_summary(msg)
    print(f"SNEWS2 {summary['tier_display']} sample message:\n")
    print(json.dumps(msg.model_dump(mode="json"), indent=2))


def cmd_gcn_produce_test(args):
    """
    Handle the 'gcn-produce-test' command.

    Publishes a hand-built SNEWS2 GCN notice directly to the real GCN Kafka
    broker (test domain by default), bypassing the schema-transform pipeline
    entirely (snews2_messages.py / transform_snews2_to_gcn / GCN Bridge).

    Args:
        args: Argparse namespace containing optional 'file' path and 'live' flag.
    """
    import json
    from snews_fireworks_launcher.utils.gcn_producer import GCNKafkaProducer, create_test_gcn_notice

    if args.file:
        with open(args.file, "r") as f:
            payload = json.load(f)
    else:
        payload = create_test_gcn_notice(is_test=not args.live)

    producer = GCNKafkaProducer()
    print(f"Publishing to GCN...")
    print(f"Domain: {producer.domain}")
    print(f"Topic:  {producer.topic}")
    print(json.dumps(payload, indent=2))
    print("-" * 60)

    result = producer.send_notice(payload)
    print(f"✓ Delivered: topic={result['topic']}, offset={result['offset']}")


def cmd_gcn_list_topics(args):
    """
    Handle the 'gcn-list-topics' command.

    Lists every topic visible to the configured GCN_CONSUMER credentials on
    the configured GCN_DOMAIN, to find the exact topic name GCN provisioned
    (since it may not match the naming convention exactly).
    """
    from snews_fireworks_launcher.utils.gcn_consumer import GCNKafkaConsumer

    consumer = GCNKafkaConsumer()
    print(f"Domain: {consumer.domain}")
    print("-" * 60)
    topics = consumer.list_topics()
    matches = [t for t in topics if "snews" in t.lower()]

    print(f"All topics visible ({len(topics)}):")
    for t in topics:
        print(f"  {t}")

    if matches:
        print(f"\nLikely SNEWS2 topic(s): {matches}")
    else:
        print("\nNo topic containing 'snews' found - check with the GCN team.")

    consumer.close()


def cmd_gcn_consume(args):
    """
    Handle the 'gcn-consume' command.

    Subscribes to SNEWS2 notices directly from the real NASA GCN Kafka broker
    (test or production, per GCN_DOMAIN) and prints them to the console. Used
    to verify that the GCN Bridge is actually publishing successfully.

    Args:
        args: Argparse namespace containing optional 'count' limit.
    """
    from snews_fireworks_launcher.utils.gcn_consumer import GCNKafkaConsumer

    def on_message(notice):
        GCNKafkaConsumer.pretty_print(notice)

    domain = os.getenv("GCN_DOMAIN", GCNKafkaConsumer.DEFAULT_DOMAIN)
    topic = os.getenv("SNEWS2_GCN_TOPIC", GCNKafkaConsumer.DEFAULT_TOPIC)
    print(f"Subscribing to GCN notices (Ctrl+C to stop)...")
    print(f"Domain: {domain}")
    print(f"Topic:  {topic}")
    print("-" * 60)

    with GCNKafkaConsumer(on_message=on_message) as consumer:
        try:
            if args.count:
                messages = consumer.consume(max_messages=args.count)
                print(f"\n✓ Consumed {len(messages)} GCN notices")
            else:
                consumer.consume(max_messages=float("inf"))
        except KeyboardInterrupt:
            print("\n\n✓ Consumer stopped")


def cmd_snews2_gcn_bridge(args):
    """
    Handle the 'snews2-gcn-bridge' command.
    
    Executes the 'snews_pt subscribe' command as a subprocess, attaching the
    Core GCN Bridge as a plugin.
    
    Args:
        args: Argparse namespace containing the 'no_firedrill' flag.
    """
    import subprocess
    
    plugin_path = os.path.join(os.path.dirname(__file__), "gcn_bridge.py")
    
    cmd = ["snews_pt", "subscribe", "-p", plugin_path]
    if args.no_firedrill:
        cmd.append("--no-firedrill")
    else:
        cmd.append("--firedrill")
    
    if args.test:
        cmd.append("--test")
        
    print(f"Starting SNEWS 2.0 to GCN Bridge...")
    print(f"Command: {' '.join(cmd)}")
    print("-" * 60)
    
    try:
        # snews_pt runs the plugin via a bare `python` (os.system), so put this
        # interpreter's bin dir first on PATH; otherwise both `snews_pt` and the
        # plugin may resolve to another environment lacking gcn-kafka.
        env = os.environ.copy()
        env["PATH"] = os.path.dirname(sys.executable) + os.pathsep + env.get("PATH", "")
        subprocess.run(cmd, check=True, env=env)
    except KeyboardInterrupt:
        print("\n\n✓ Listener stopped")
    except Exception as e:
        print(f"\nError running hopskotch listener: {e}")



# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------

def main():
    """
    Main CLI entry point. 
    
    Parses arguments, loads environment variables, and dispatches to the 
    appropriate command handler.
    """
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="SNEWS Kafka Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python -m snews-fireworks-launcher snews2-gcn-bridge --firedrill
  python -m snews-fireworks-launcher snews2-produce --tier coincidence --test
  python -m snews-fireworks-launcher snews2-consume --count 5
  python -m snews-fireworks-launcher snews2-transform --tier timing
        """,
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # --- SNEWS2 commands ---
    s2_produce = subparsers.add_parser("snews2-produce", help="Produce SNEWS2 alerts to Kafka")
    s2_produce.add_argument("--tier", type=str, default="coincidence",
                            choices=SNEWS2_TIERS, help="Message tier to send")
    s2_produce.add_argument("--test", action="store_true", help="Mark as TEST")
    s2_produce.set_defaults(func=cmd_snews2_produce)

    s2_consume = subparsers.add_parser("snews2-consume", help="Subscribe to SNEWS2 alerts from Kafka")
    s2_consume.add_argument("--count", type=int, help="Maximum messages to consume")
    s2_consume.set_defaults(func=cmd_snews2_consume)

    gcn_list_topics = subparsers.add_parser("gcn-list-topics", help="List topics visible to your GCN consumer credentials")
    gcn_list_topics.set_defaults(func=cmd_gcn_list_topics)

    gcn_consume = subparsers.add_parser("gcn-consume", help="Subscribe to SNEWS2 notices from the real GCN Kafka broker")
    gcn_consume.add_argument("--count", type=int, help="Maximum messages to consume")
    gcn_consume.set_defaults(func=cmd_gcn_consume)

    gcn_produce_test = subparsers.add_parser("gcn-produce-test", help="Publish a hand-built SNEWS2 notice directly to GCN (bypasses schema pipeline)")
    gcn_produce_test.add_argument("--file", type=str, help="Path to a JSON file with a pre-built notice payload")
    gcn_produce_test.add_argument("--live", action="store_true", help="Mark alert_tense as 'current' instead of 'test'")
    gcn_produce_test.set_defaults(func=cmd_gcn_produce_test)

    s2_transform = subparsers.add_parser("snews2-transform", help="Show sample SNEWS2 JSON (no Kafka)")
    s2_transform.add_argument("--tier", type=str, default="coincidence",
                              choices=SNEWS2_TIERS, help="Message tier to display")
    s2_transform.add_argument("--test", action="store_true", help="Mark as TEST")
    s2_transform.set_defaults(func=cmd_snews2_transform)
    
    s2_bridge = subparsers.add_parser("snews2-gcn-bridge", 
                                      aliases=["snews2-hopskotch-listen"],
                                      help="Listen to Hopskotch and bridge alerts to GCN (mock or real)")
    s2_bridge.add_argument("--no-firedrill", action="store_true", help="Listen to real hopskotch network instead of firedrill")
    s2_bridge.add_argument("--test", action="store_true", help="Mark as TEST")
    s2_bridge.set_defaults(func=cmd_snews2_gcn_bridge)
    
    args = parser.parse_args()
    setup_logging(args.verbose)
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    args.func(args)


if __name__ == "__main__":
    main()

