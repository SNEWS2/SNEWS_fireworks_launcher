"""
SNEWS 2.0 to GCN JSON Schema transformation.

Produces notices conforming to GCN's SNEWS2 coincidence schema:
https://gcn.nasa.gov/schema/main/gcn/notices/snews2/coincidence.schema.json
(source: nasa-gcn/gcn-schema, gcn/notices/snews2/coincidence.schema.json).

GCN notices are flat: the core GCN fields (Alert, Event, Reporter, DateTime,
Statistics, Localization) sit at the top level alongside the SNEWS2-specific
fields. Use ``SNEWS2GCNNotice.to_gcn_dict()`` to get the exact JSON payload
to publish; it emits ``$schema`` and omits unset optional fields (GCN's
schema types them as non-nullable, so ``null`` would fail validation).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from .snews2_messages import SNEWS2MessageBase, Tier, CoincidenceTierAlert

GCN_SNEWS2_SCHEMA_ID = (
    "https://gcn.nasa.gov/schema/main/gcn/notices/snews2/coincidence.schema.json"
)
SNEWS_SCHEMA_VERSION = "1.0"

# core/DetectorStatus.schema.json
DetectorStatus = Literal["on", "off", "triggered"]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class SNEWS2GCNNotice(BaseModel):
    """A SNEWS 2.0 alert in GCN's flat snews2/coincidence notice format."""

    model_config = ConfigDict(populate_by_name=True)

    schema_id: str = Field(default=GCN_SNEWS2_SCHEMA_ID, alias="$schema")

    # core/Alert
    alert_datetime: str = Field(..., description="Date and time of notice creation [UTC, ISO 8601]")
    alert_tense: Literal["current", "archival", "planned", "injection", "commanded", "test"]
    alert_type: Literal["initial", "subsequent", "update", "retraction"]

    # core/Event
    id: str = Field(..., description="SNEWS alert ID")
    data_archive_page: Optional[str] = None

    # core/Reporter
    mission: str = "SNEWS"
    messenger: Literal["EM", "GW", "Neutrino"] = "Neutrino"
    record_number: Optional[int] = Field(
        default=None,
        description="Incremental number for messages from the instrument during a given trigger",
    )

    # core/DateTime
    trigger_time: str = Field(..., description="Time of the trigger [ISO 8601]")

    # core/Statistics
    far: Optional[float] = Field(default=None, description="False alarm rate")

    # core/Localization
    healpix_url: Optional[str] = None

    # snews2/coincidence
    snews_schema_version: str = SNEWS_SCHEMA_VERSION
    snews2_tier: Tier = Field(..., description="The tier of the original message")
    detector_status: Dict[str, DetectorStatus] = Field(
        ..., description="Status of each SNEWS2 detector at the time of the alert"
    )
    event_times_utc: List[str] = Field(..., description="Primary timestamp(s) for the event")
    tier_data: Dict[str, Any] = Field(..., description="Tier-specific data payload")

    def to_gcn_dict(self) -> Dict[str, Any]:
        """The JSON payload to publish to GCN."""
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


def _as_float(value: Any) -> Optional[float]:
    """False-alarm values from snews_cs can be "N/A"; treat non-numbers as absent."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def transform_snews2_to_gcn(msg: Union[SNEWS2MessageBase, CoincidenceTierAlert]) -> SNEWS2GCNNotice:
    """
    Transform a SNEWS 2.0 message into a GCN snews2/coincidence notice.

    Common fields (detectors, times, FAR) are promoted to the top level;
    tier-specific properties are kept in ``tier_data``.
    """
    # Exclude base fields so tier_data only holds tier-specific properties
    excludes = {
        "id", "uuid", "tier", "sent_time_utc", "machine_time_utc",
        "is_pre_sn", "is_test", "is_firedrill", "meta", "schema_version", "detector_name",
        "sent_time", "alert_type", "server_tag", "healpix_url",
    }
    tier_data = msg.model_dump(exclude=excludes, exclude_none=True, mode="json")

    if isinstance(msg, CoincidenceTierAlert):
        event_times_utc = list(msg.neutrino_times)
        trigger_time = event_times_utc[0] if event_times_utc else (msg.sent_time or _utc_now_iso())

        # Every detector in a coincidence alert contributed a trigger.
        detector_status = {name: "triggered" for name in msg.detector_names}

        tier_data.pop("detector_names", None)
        tier_data.pop("neutrino_times", None)
        if event_times_utc:
            tier_data["neutrino_time_utc"] = trigger_time

        # snews_cs marks test alerts in its alert_type string, e.g. "TEST COINC_MSG"
        alert_tense = "test" if "TEST" in msg.alert_type.upper() else "current"

        # sub_list_number 0 is the first alert for this coincidence; >0 are updates
        sub_list_num = msg.sub_list_number or 0
        alert_type = "update" if sub_list_num > 0 else "initial"
        record_number = sub_list_num + 1

        msg_tier = Tier.COINCIDENCE_TIER
        event_id = msg.id
        far = _as_float(msg.false_alarm_prob)
    else:
        fallback_time = msg.machine_time_utc or msg.sent_time_utc or _utc_now_iso()
        trigger_time = getattr(msg, "neutrino_time_utc", None) or fallback_time
        event_times_utc = [trigger_time]

        heartbeat_status = getattr(msg, "detector_status", None)
        if msg.tier in (Tier.HEART_BEAT, "Heartbeat") and heartbeat_status:
            status = "on" if str(heartbeat_status).upper() == "ON" else "off"
        else:
            status = "triggered"
        detector_status = {msg.detector_name: status}

        alert_tense = "test" if msg.is_test else ("injection" if msg.is_firedrill else "current")
        alert_type = "retraction" if msg.tier in (Tier.RETRACTION, "Retraction") else "initial"
        record_number = None
        msg_tier = msg.tier
        event_id = msg.uuid
        far = _as_float(getattr(msg, "false_alarm_prob", None) or getattr(msg, "false_alarm_rate_hz", None))

    # tier_data.false_alarm_prob must be a number in the GCN schema
    if "false_alarm_prob" in tier_data and _as_float(tier_data["false_alarm_prob"]) is None:
        tier_data.pop("false_alarm_prob")

    healpix_url = getattr(msg, "healpix_url", None)
    if healpix_url is None and getattr(msg, "model_extra", None):
        healpix_url = msg.model_extra.get("healpix_url")

    return SNEWS2GCNNotice(
        alert_datetime=_utc_now_iso(),
        alert_tense=alert_tense,
        alert_type=alert_type,
        id=event_id,
        record_number=record_number,
        trigger_time=trigger_time,
        far=far,
        healpix_url=healpix_url,
        snews2_tier=msg_tier,
        detector_status=detector_status,
        event_times_utc=event_times_utc,
        tier_data=tier_data,
    )
