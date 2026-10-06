import os
import time
from datetime import datetime, timezone

import snews_pt  # noqa: F401 - importing loads snews_pt's OBSERVATION_TOPIC/ALERT_TOPIC env vars
from snews_pt.messages import Publisher

# snews_pt >=3.0 publishers/subscribers just take a raw kafka topic URI - no more
# env="test"/firedrill_mode kwargs. OBSERVATION_TOPIC is where individual detector
# observations go; the coincidence server reads from there and republishes the
# aggregated alert. Test alerts (is_test=True) are published to
# snews.connection-testing, which is what `snews2-gcn-bridge --test` subscribes to.
OBSERVATION_TOPIC = os.getenv(
    "OBSERVATION_TOPIC", "kafka://kafka.scimma.org/snews.experiments-test"
)


def trigger_test_coincidence():
    print("Initializing SCiMMA Publisher...")
    print(f"Topic: {OBSERVATION_TOPIC}")
    pub = Publisher(OBSERVATION_TOPIC)

    # Use the current time to guarantee both messages match the 10-second coincidence window
    now = datetime.now(timezone.utc)
    print(f"Base Event Time: {now.isoformat()}")

    print("\n[1/2] Queuing Detector A (Super-K)...")
    pub.add_message({
        "detector_name": "Super-K",
        "neutrino_time_utc": now,
        "machine_time_utc": now,
        "p_val": 0.05,
        "meta": {"test_reason": "Verifying GCN Bridge Aggregation"},
        "is_test": True,
    })

    time.sleep(1.5)

    print("[2/2] Queuing Detector B (IceCube)...")
    pub.add_message({
        "detector_name": "IceCube",
        "neutrino_time_utc": now,  # same neutrino time to guarantee the coincidence match
        "machine_time_utc": now,
        "p_val": 0.02,
        "meta": {"test_reason": "Verifying GCN Bridge Aggregation"},
        "is_test": True,
    })

    pub.send()

    print("\nPublished. The SNEWS coincidence server should now publish a TEST alert to snews.connection-testing for the bridge to pick up.")


if __name__ == "__main__":
    trigger_test_coincidence()
