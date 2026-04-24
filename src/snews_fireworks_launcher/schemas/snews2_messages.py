"""
SNEWS 2.0 Message Schema Models

This module integrates the official SNEWS 2.0 message models from the 
snews-data-formats package (https://github.com/SNEWS2/snews-data-formats).
"""

from typing import Dict, Type, Any, List, Optional

# Re-exporting from the official package
from snews.models.messages import (
    Tier,
    DetectionChannel,
    MessageBase as SNEWS2MessageBase,
    HeartbeatMessage,
    RetractionMessage,
    CoincidenceTierMessage,
    CoincidenceTierAlert,
    SignificanceTierMessage,
    TimingTierMessage,
    TierMessageBase,
)

# Registry for easy lookup
TIER_MODEL_MAP: Dict[Tier, Type[SNEWS2MessageBase]] = {
    Tier.HEART_BEAT: HeartbeatMessage,
    Tier.RETRACTION: RetractionMessage,
    Tier.SIGNIFICANCE_TIER: SignificanceTierMessage,
    Tier.TIMING_TIER: TimingTierMessage,
    Tier.COINCIDENCE_TIER: CoincidenceTierMessage,
}

def parse_snews2_message(data: Dict[str, Any]):
    """
    Parse a JSON dict into the appropriate SNEWS2 message model.

    Args:
        data: Dict with at least a 'tier' field identifying the message type,
              or an alert dict matching snews_cs.

    Returns:
        Validated Pydantic model instance.
    """
    # Specifically catch the snews_cs aggregated alert output since it lacks a "tier" field natively
    if "alert_type" in data and "id" in data and str(data["id"]).startswith("SNEWS_Coincidence_ALERT"):
        return CoincidenceTierAlert(**data)

    tier_str = data.get("tier")
    if tier_str is None:
        raise ValueError("Message missing required 'tier' field")

    try:
        tier = Tier(tier_str)
    except ValueError:
        raise ValueError(f"Unknown tier: {tier_str}")

    model_cls = TIER_MODEL_MAP[tier]
    return model_cls(**data)
