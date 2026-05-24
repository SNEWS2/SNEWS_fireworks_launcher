"""
SNEWS 2.0 to GCN JSON Schema transformation.

Encapsulates the 5 SNEWS2 tiers into a single schema for GCN publication.
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field

from .snews2_messages import SNEWS2MessageBase, Tier, CoincidenceTierAlert


class GCNAlert(BaseModel):
    """General alert metadata for GCN."""
    alert_datetime: str = Field(..., description="Date and time of notice creation [UTC, ISO 8601]")
    alert_tense: str = Field(..., description="Current, test, or injection")
    alert_type: str = Field(..., description="Initial, update, or retraction")

class GCNEvent(BaseModel):
    """Event-specific Identification for GCN."""
    id: str = Field(..., description="Instrument-specific trigger ID")
    data_archive_page: Optional[str] = Field(default=None, description="URL of archived data files")

class GCNReporter(BaseModel):
    """Reporter metadata identifying SNEWS."""
    mission: str = Field(default="SNEWS", description="Name of Mission or Telescope")
    messenger: str = Field(default="Neutrino", description="Messenger of report")

class GCNDateTime(BaseModel):
    """Primary event timestamp for GCN."""
    trigger_time: str = Field(..., description="Time of the trigger [ISO 8601]")

class GCNStatistics(BaseModel):
    """Statistical verification data for GCN."""
    far: Optional[float] = Field(default=None, description="False alarm rate [Hz]")

class SNEWS2GCNNotice(BaseModel):
    """
    Unified JSON schema representing a SNEWS 2.0 alert wrapped for GCN.
    """
    alert: GCNAlert
    event: GCNEvent
    reporter: GCNReporter
    datetime: GCNDateTime
    statistics: Optional[GCNStatistics] = None
    
    schema_version: str = Field(default="1.0", description="Schema version")
    snews2_tier: Tier = Field(..., description="The tier of the original message")
    detector_names: List[str] = Field(..., description="Originating detector(s)")
    event_times_utc: List[str] = Field(..., description="Primary timestamp(s) for the event")
    tier_data: Dict[str, Any] = Field(..., description="Tier-specific data payload")


def transform_snews2_to_gcn(msg: Union[SNEWS2MessageBase, CoincidenceTierAlert]) -> SNEWS2GCNNotice:
    """
    Transform a SNEWS 2.0 message into a unified GCN-compatible JSON payload.
    
    This function handles the extraction of common fields (detectors, times, FAR)
    and promotes them to the top-level GCN envelope, while preserving
    tier-specific data in the 'tier_data' dictionary.
    
    Args:
        msg: A validated SNEWS 2.0 message model or CoincidenceTierAlert.
        
    Returns:
        A SNEWS2GCNNotice instance ready for GCN publication.
    """
    from datetime import timezone, datetime
    
    # Exclude base fields so tier_data only holds tier-specific properties
    excludes = {
        "id", "uuid", "tier", "sent_time_utc", "machine_time_utc", 
        "is_pre_sn", "is_test", "is_firedrill", "meta", "schema_version", "detector_name",
        "sent_time", "alert_type", "server_tag"
    }
    tier_data = msg.model_dump(exclude=excludes, exclude_none=True, mode="json")
    
    if isinstance(msg, CoincidenceTierAlert):
        detector_names = msg.detector_names
        event_times_utc = msg.neutrino_times
        event_time = msg.sent_time or datetime.now(timezone.utc).isoformat()
        
        tier_data.pop("detector_names", None)
        tier_data.pop("neutrino_times", None)
        
        # Test tense parsing based on the server's alert_type formatting
        alert_tense = "test" if "TEST" in msg.alert_type.upper() else "current"
        alert_type_val = "initial"
        msg_tier = Tier.COINCIDENCE_TIER
        msg_uuid = msg.id.split(" ")[-1] if " " in msg.id else msg.id  # Extract a usable ID
        
        far_val = msg.false_alarm_prob if msg.false_alarm_prob != "N/A" else None
    else:
        detector_names = [msg.detector_name]
        event_time = msg.machine_time_utc or msg.sent_time_utc or datetime.now(timezone.utc).isoformat()
        event_times_utc = [event_time]
        
        if msg.tier in [Tier.TIMING_TIER, "TimingTier"]:
            event_times_utc = [getattr(msg, "neutrino_time_utc", event_time)]
            
        # Standardize tense and type
        alert_tense = "test" if msg.is_test else ("injection" if msg.is_firedrill else "current")
        alert_type_val = "retraction" if msg.tier in [Tier.RETRACTION, "RetractionTier"] else "initial"
        msg_tier = msg.tier
        msg_uuid = msg.uuid
        
        far_val = getattr(msg, "false_alarm_prob", None) or getattr(msg, "false_alarm_rate_hz", None)
    
    gcn_alert = GCNAlert(
        alert_datetime=datetime.now(timezone.utc).isoformat(),
        alert_tense=alert_tense,
        alert_type=alert_type_val
    )
    
    gcn_event = GCNEvent(
        id=msg_uuid,
        data_archive_page=None
    )
    
    gcn_reporter = GCNReporter()
    
    gcn_datetime = GCNDateTime(
        trigger_time=event_times_utc[0] if event_times_utc else event_time
    )
    
    gcn_statistics = None
    if far_val is not None:
        try:
            gcn_statistics = GCNStatistics(far=float(far_val))
        except ValueError:
            pass
    
    return SNEWS2GCNNotice(
        alert=gcn_alert,
        event=gcn_event,
        reporter=gcn_reporter,
        datetime=gcn_datetime,
        statistics=gcn_statistics,
        snews2_tier=msg_tier,
        detector_names=detector_names,
        event_times_utc=event_times_utc,
        tier_data=tier_data
    )
