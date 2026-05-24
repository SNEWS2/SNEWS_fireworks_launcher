"""
Tests for SNEWS 2.0 message schemas and serialization.

Covers all 5 message tiers: Heartbeat, Retraction, Coincidence,
Significance, and Timing.
"""

import json
import pytest
from datetime import datetime, timezone

from snews_fireworks_launcher.schemas.snews2_messages import (
    SNEWS2MessageBase,
    HeartbeatMessage,
    RetractionMessage,
    CoincidenceTierMessage,
    SignificanceTierMessage,
    TimingTierMessage,
    Tier,
    parse_snews2_message,
)
from snews_fireworks_launcher.utils.snews2_producer import (
    create_sample_heartbeat,
    create_sample_coincidence,
    create_sample_significance,
    create_sample_timing,
    create_sample_retraction,
    SAMPLE_GENERATORS,
)


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeatMessage:
    """Tests for the Heartbeat message tier (detector health)."""
    
    def test_basic_creation(self):
        """Verify that a heartbeat can be created with required fields."""
        msg = HeartbeatMessage(detector_name="Super-K", detector_status="ON")
        assert msg.tier == Tier.HEARTBEAT
        assert msg.detector_name == "Super-K"
        assert msg.detector_status == "ON"
        assert msg.is_test is False

    def test_invalid_status_rejected(self):
        with pytest.raises(Exception):
            HeartbeatMessage(detector_name="Super-K", detector_status="MAYBE")

    def test_auto_id_generation(self):
        msg = HeartbeatMessage(
            detector_name="IceCube",
            detector_status="OFF",
            machine_time_utc="2025-01-01T00:00:00",
        )
        assert "IceCube" in msg.id
        assert "Heartbeat" in msg.id

    def test_sample_generator(self):
        msg = create_sample_heartbeat(is_test=True)
        assert msg.is_test is True
        assert msg.detector_status == "ON"


# ---------------------------------------------------------------------------
# Retraction
# ---------------------------------------------------------------------------

class TestRetractionMessage:
    """Tests for the Retraction message tier (alert withdrawal)."""

    def test_retract_by_uuid(self):
        """Verify that a specific message can be retracted by its UUID."""
        msg = RetractionMessage(
            detector_name="Super-K",
            retract_message_uuid="abc-123-def",
            retraction_reason="Calibration artifact",
        )
        assert msg.retract_message_uuid == "abc-123-def"
        assert msg.retract_latest_n == 0

    def test_retract_latest_n(self):
        msg = RetractionMessage(
            detector_name="Super-K",
            retract_latest_n=3,
        )
        assert msg.retract_latest_n == 3
        assert msg.retract_message_uuid is None

    def test_both_methods_rejected(self):
        with pytest.raises(Exception):
            RetractionMessage(
                detector_name="Super-K",
                retract_message_uuid="abc-123",
                retract_latest_n=1,
            )

    def test_neither_method_rejected(self):
        with pytest.raises(Exception):
            RetractionMessage(
                detector_name="Super-K",
                retract_latest_n=0,
            )

    def test_sample_generator(self):
        msg = create_sample_retraction()
        assert msg.retract_latest_n == 1
        assert msg.retraction_reason is not None


# ---------------------------------------------------------------------------
# Coincidence Tier
# ---------------------------------------------------------------------------

class TestCoincidenceTierMessage:
    """Tests for the Coincidence tier (multi-detector events)."""

    def test_basic_creation(self):
        """Verify that a coincidence alert can be created with shared detector list."""
        now = datetime.now(timezone.utc).isoformat()
        msg = CoincidenceTierMessage(
            detector_name="Super-K",
            detector_names=["Super-K", "IceCube"],
            neutrino_times_utc=[now, now],
            p_values=[0.07, 0.05],
        )
        assert getattr(msg, "p_val", None) is None
        assert getattr(msg, "neutrino_time_utc", None) is None
        assert msg.tier == Tier.COINCIDENCE_TIER
        assert msg.p_values == [0.07, 0.05]

    def test_datetime_auto_conversion(self):
        """datetime objects should auto-convert to strings."""
        now = datetime.now(timezone.utc)
        msg = CoincidenceTierMessage(
            detector_name="KamLAND",
            detector_names=["KamLAND", "SNO+"],
            neutrino_times_utc=[now, now],
            p_values=[0.1, 0.2]
        )
        assert isinstance(msg.neutrino_times_utc[0], str)
        assert msg.neutrino_times_utc[0] == now.isoformat()

    def test_p_val_range_validation(self):
        with pytest.raises(Exception):
            CoincidenceTierMessage(
                detector_name="Super-K",
                detector_names=["Super-K"],
                neutrino_times_utc=[datetime.now(timezone.utc).isoformat()],
                p_values=[1.5],  # Out of range
            )

    def test_sample_generator(self):
        msg = create_sample_coincidence()
        assert msg.neutrino_times_utc is not None
        assert msg.is_test is True


# ---------------------------------------------------------------------------
# Significance Tier
# ---------------------------------------------------------------------------

class TestSignificanceTierMessage:
    """Tests for the Significance tier (statistical burst analysis)."""

    def test_basic_creation(self):
        """Verify that a significance alert can be created with binned p-values."""
        msg = SignificanceTierMessage(
            detector_name="KamLAND",
            p_values=[0.43, 0.32, 0.09, 0.01],
            t_bin_width_sec=0.005,
        )
        assert msg.tier == Tier.SIGNIFICANCE_TIER
        assert len(msg.p_values) == 4

    def test_invalid_p_values_rejected(self):
        with pytest.raises(Exception):
            SignificanceTierMessage(
                detector_name="KamLAND",
                p_values=[0.5, 1.5],  # 1.5 out of range
                t_bin_width_sec=0.1,
            )

    def test_empty_p_values_rejected(self):
        with pytest.raises(Exception):
            SignificanceTierMessage(
                detector_name="KamLAND",
                p_values=[],
                t_bin_width_sec=0.1,
            )

    def test_sample_generator(self):
        msg = create_sample_significance()
        assert len(msg.p_values) > 0
        assert msg.t_bin_width_sec > 0


# ---------------------------------------------------------------------------
# Timing Tier
# ---------------------------------------------------------------------------

class TestTimingTierMessage:
    """Tests for the Timing tier (high-precision arrival data)."""

    def test_unbinned_creation(self):
        """Verify unbinned timing alerts with raw neutrino offsets (ns)."""
        now = datetime.now(timezone.utc).isoformat()
        msg = TimingTierMessage(
            detector_name="IceCube",
            neutrino_time_utc=now,
            start_time_utc=now,
            timing_series=[0, 303000, 659236],
        )
        assert msg.tier == Tier.TIMING_TIER
        assert msg.is_binned() is False

    def test_binned_creation(self):
        now = datetime.now(timezone.utc).isoformat()
        msg = TimingTierMessage(
            detector_name="KM3NeT",
            neutrino_time_utc=now,
            start_time_utc=now,
            timing_series=[5, 12, 8, 3, 1],
            time_bin_width_ns=1000000,  # 1ms bins
        )
        assert msg.is_binned() is True

    def test_empty_timing_series_rejected(self):
        now = datetime.now(timezone.utc).isoformat()
        with pytest.raises(Exception):
            TimingTierMessage(
                detector_name="IceCube",
                neutrino_time_utc=now,
                start_time_utc=now,
                timing_series=[],
            )

    def test_detection_channel(self):
        now = datetime.now(timezone.utc).isoformat()
        msg = TimingTierMessage(
            detector_name="Super-K",
            neutrino_time_utc=now,
            start_time_utc=now,
            timing_series=[0, 500000],
            detection_channel="Electron Antineutrino",
        )
        assert msg.detection_channel == "Electron Antineutrino"

    def test_sample_generator(self):
        msg = create_sample_timing()
        assert len(msg.timing_series) > 0


# ---------------------------------------------------------------------------
# Serialization / Deserialization
# ---------------------------------------------------------------------------

class TestSerialization:
    def test_roundtrip_all_tiers(self):
        """Each tier should survive JSON serialization and deserialization."""
        for name, generator in SAMPLE_GENERATORS.items():
            original = generator(is_test=True)
            json_dict = original.to_json()
            json_str = json.dumps(json_dict)
            restored_dict = json.loads(json_str)
            restored = parse_snews2_message(restored_dict)

            assert restored.tier == original.tier
            assert restored.detector_name == original.detector_name
            assert restored.uuid == original.uuid
            assert restored.is_test == original.is_test

    def test_parse_unknown_tier_raises(self):
        with pytest.raises(ValueError, match="Unknown tier"):
            parse_snews2_message({"tier": "FakeTier", "detector_name": "X"})

    def test_parse_missing_tier_raises(self):
        with pytest.raises(ValueError, match="missing"):
            parse_snews2_message({"detector_name": "X"})


# ---------------------------------------------------------------------------
# Common fields
# ---------------------------------------------------------------------------

class TestCommonFields:
    def test_uuid_auto_generated(self):
        msg = HeartbeatMessage(detector_name="Super-K", detector_status="ON")
        assert len(msg.uuid) > 0

    def test_schema_version_default(self):
        msg = HeartbeatMessage(detector_name="Super-K", detector_status="ON")
        assert msg.schema_version == "0.2"

    def test_flags_default_false(self):
        msg = HeartbeatMessage(detector_name="Super-K", detector_status="ON")
        assert msg.is_test is False
        assert msg.is_firedrill is False
        assert msg.is_pre_sn is False

    def test_metadata_attachment(self):
        msg = CoincidenceTierMessage(
            detector_name="Super-K",
            detector_names=["Super-K", "IceCube"],
            neutrino_times_utc=[datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()],
            p_values=[0.05, 0.01],
            meta={"run_id": 42, "notes": "calibration run"},
        )
        assert msg.meta["run_id"] == 42
        assert msg.meta["notes"] == "calibration run"
