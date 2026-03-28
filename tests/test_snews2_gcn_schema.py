import pytest
from datetime import datetime

from src.schemas.snews2_messages import CoincidenceTierMessage, Tier, DetectorStatus, DetectionChannel, HeartbeatMessage, TimingTierMessage, RetractionMessage, SignificanceTierMessage
from src.schemas.snews2_gcn_schema import transform_snews2_to_gcn, SNEWS2GCNNotice

def test_transform_coincidence_tier_to_gcn():
    """Verify that a CoincidenceTier alert is correctly transformed into GCN format."""
    msg = CoincidenceTierMessage(
        detector_name="Super-K",
        detector_names=["Super-K", "IceCube"],
        neutrino_times_utc=["2025-01-15T14:30:00.123456+00:00", "2025-01-15T14:30:00.123457+00:00"],
        p_values=[0.07, 0.05],
        false_alarm_prob=0.001,
        is_test=True,
        is_firedrill=False
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert isinstance(gcn_notice, SNEWS2GCNNotice)
    assert gcn_notice.alert.alert_tense == "test"
    assert gcn_notice.alert.alert_type == "initial"
    assert gcn_notice.snews2_tier == Tier.COINCIDENCE_TIER
    assert gcn_notice.event.id == msg.uuid
    assert gcn_notice.event.id == msg.uuid
    assert gcn_notice.datetime.trigger_time == "2025-01-15T14:30:00.123456+00:00"
    assert gcn_notice.detector_names == ["Super-K", "IceCube"]
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.123456+00:00", "2025-01-15T14:30:00.123457+00:00"]
    assert gcn_notice.statistics.far == 0.001
    
    # Promoted fields should be removed from tier_data
    assert "detector_names" not in gcn_notice.tier_data
    assert "neutrino_times_utc" not in gcn_notice.tier_data
    assert gcn_notice.tier_data["p_values"] == [0.07, 0.05]
    assert gcn_notice.tier_data["false_alarm_prob"] == 0.001

def test_transform_heartbeat_to_gcn():
    """Verify that a Heartbeat alert is correctly transformed into GCN format."""
    msg = HeartbeatMessage(
        detector_name="IceCube",
        detector_status=DetectorStatus.ON,
        machine_time_utc="2025-01-15T14:35:00.000000+00:00",
        is_test=False,
        is_firedrill=False
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert gcn_notice.snews2_tier == Tier.HEARTBEAT
    assert gcn_notice.alert.alert_tense == "current"
    assert gcn_notice.alert.alert_tense == "current"
    assert gcn_notice.event_times_utc == ["2025-01-15T14:35:00.000000+00:00"]
    assert gcn_notice.tier_data["detector_status"] == DetectorStatus.ON.value
    assert "machine_time_utc" not in gcn_notice.tier_data # excluded base field

def test_transform_timing_tier_to_gcn():
    """Verify that a TimingTier alert is correctly transformed into GCN format."""
    msg = TimingTierMessage(
        detector_name="Borexino",
        neutrino_time_utc="2025-01-15T14:30:00.123456+00:00",
        start_time_utc="2025-01-15T14:30:00.000000+00:00",
        timing_series=[1000, 2000, 3000],
        detection_channel=DetectionChannel.NU_E,
        is_firedrill=True
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    
    assert gcn_notice.snews2_tier == Tier.TIMING_TIER
    assert gcn_notice.alert.alert_tense == "injection"
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.123456+00:00"]
    assert gcn_notice.tier_data["start_time_utc"] == "2025-01-15T14:30:00.000000+00:00"
    assert gcn_notice.tier_data["timing_series"] == [1000, 2000, 3000]
    assert gcn_notice.tier_data["detection_channel"] == DetectionChannel.NU_E.value
    
def test_transform_retraction_to_gcn():
    """Verify that a Retraction alert is correctly transformed into GCN format."""
    msg = RetractionMessage(
        detector_name="Super-K",
        retract_latest_n=1,
        retraction_reason="False alarm",
        machine_time_utc="2025-01-15T14:40:00.000000+00:00"
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.alert.alert_type == "retraction"
    assert gcn_notice.tier_data["retract_latest_n"] == 1
    assert gcn_notice.tier_data["retraction_reason"] == "False alarm"
    
def test_transform_significance_to_gcn():
    """Verify that a SignificanceTier alert is correctly transformed into GCN format."""
    msg = SignificanceTierMessage(
        detector_name="HALO",
        p_values=[0.1, 0.05, 0.01],
        t_bin_width_sec=0.5,
        machine_time_utc="2025-01-15T14:30:00.000000+00:00"
    )
    
    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.000000+00:00"]
    assert gcn_notice.tier_data["p_values"] == [0.1, 0.05, 0.01]
    assert gcn_notice.tier_data["t_bin_width_sec"] == 0.5
