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

        test_label = " [TEST]" if msg.is_test else ""
        tier_display = msg.tier.value if hasattr(msg.tier, 'value') else msg.tier
        print(f"✓ Sent SNEWS2 {tier_display}{test_label}")
        print(f"  Detector:  {msg.detector_name}")
        print(f"  UUID:      {msg.uuid[:12]}...")


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
    tier_display = msg.tier.value if hasattr(msg.tier, 'value') else msg.tier
    print(f"SNEWS2 {tier_display} sample message:\n")
    print(json.dumps(msg.to_json(), indent=2))


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
        subprocess.run(cmd, check=True)
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

