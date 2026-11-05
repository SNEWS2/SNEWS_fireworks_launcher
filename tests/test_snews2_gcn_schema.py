import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

from snews_fireworks_launcher.schemas.snews2_messages import CoincidenceTierAlert, Tier, DetectionChannel, HeartbeatMessage, TimingTierMessage, RetractionMessage, SignificanceTierMessage
from snews_fireworks_launcher.schemas.snews2_gcn_schema import (
    GCN_SNEWS2_SCHEMA_ID,
    SNEWS2GCNNotice,
    transform_snews2_to_gcn,
)
from snews_fireworks_launcher.utils.gcn_producer import create_test_gcn_notice

# Vendored copy of GCN's snews2/coincidence schema + the core schemas it $refs
SCHEMA_DIR = Path(__file__).parent / "gcn_schema" / "gcn" / "notices"


def _load_schemas():
    return {
        path: json.loads(path.read_text())
        for path in SCHEMA_DIR.rglob("*.schema.json")
    }


@pytest.fixture(scope="module")
def gcn_validator():
    schemas = _load_schemas()
    registry = Registry().with_resources(
        (s["$id"], Resource.from_contents(s)) for s in schemas.values()
    )
    snews2 = next(s for s in schemas.values() if s["$id"] == GCN_SNEWS2_SCHEMA_ID)
    return Draft202012Validator(snews2, registry=registry)


@pytest.fixture(scope="module")
def declared_properties():
    """Every top-level property name the snews2 schema (incl. its core $refs) declares."""
    schemas = {s["$id"]: s for s in _load_schemas().values()}
    snews2 = schemas[GCN_SNEWS2_SCHEMA_ID]
    names = set(snews2["properties"]) | {"$schema"}
    for ref in snews2["allOf"]:
        core_id = GCN_SNEWS2_SCHEMA_ID.rsplit("/", 2)[0] + "/" + ref["$ref"].removeprefix("../")
        names |= set(schemas[core_id]["properties"])
    return names


def assert_valid_gcn(payload, validator, declared):
    errors = sorted(validator.iter_errors(payload), key=lambda e: list(e.path))
    assert not errors, "\n".join(f"{list(e.path)}: {e.message}" for e in errors)
    # The schema allows extra keys, so also catch fields the schema doesn't know about
    # (e.g. a typo or a field renamed upstream) rather than silently passing them.
    unknown = set(payload) - declared
    assert not unknown, f"fields not in GCN snews2 schema: {sorted(unknown)}"


def make_coincidence_alert(**overrides):
    fields = dict(
        id="SNEWS_Coincidence_ALERT 2025-01-15T14:30:00",
        alert_type="TEST COINC_MSG",
        server_tag="fake-server",
        detector_names=["Super-K", "IceCube"],
        neutrino_times=["2025-01-15T14:30:00.123456+00:00", "2025-01-15T14:30:00.123457+00:00"],
        p_values=[0.07, 0.05],
        p_values_average=0.06,
        sub_list_number=0,
        false_alarm_prob=0.001,
        sent_time="2025-01-15T14:30:00",
    )
    fields.update(overrides)
    return CoincidenceTierAlert(**fields)


def test_vendored_examples_are_valid(gcn_validator, declared_properties):
    """Sanity-check the vendored schema copy against its own examples."""
    examples = sorted(SCHEMA_DIR.glob("snews2/*.example.json"))
    assert examples
    for path in examples:
        assert_valid_gcn(json.loads(path.read_text()), gcn_validator, declared_properties)


def test_transform_coincidence_tier_to_gcn(gcn_validator, declared_properties):
    """Verify that a CoincidenceTier alert is transformed into GCN's flat snews2 format."""
    gcn_notice = transform_snews2_to_gcn(make_coincidence_alert())

    assert isinstance(gcn_notice, SNEWS2GCNNotice)
    assert gcn_notice.alert_tense == "test"
    assert gcn_notice.alert_type == "initial"
    assert gcn_notice.record_number == 1
    assert gcn_notice.snews2_tier == Tier.COINCIDENCE_TIER
    assert gcn_notice.id == "SNEWS_Coincidence_ALERT 2025-01-15T14:30:00"
    assert gcn_notice.trigger_time == "2025-01-15T14:30:00.123456+00:00"
    assert gcn_notice.detector_status == {"Super-K": "triggered", "IceCube": "triggered"}
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.123456+00:00", "2025-01-15T14:30:00.123457+00:00"]
    assert gcn_notice.far == 0.001

    # Promoted fields should be removed from tier_data
    assert "detector_names" not in gcn_notice.tier_data
    assert "neutrino_times" not in gcn_notice.tier_data
    assert gcn_notice.tier_data["neutrino_time_utc"] == "2025-01-15T14:30:00.123456+00:00"
    assert gcn_notice.tier_data["p_values"] == [0.07, 0.05]
    assert gcn_notice.tier_data["false_alarm_prob"] == 0.001

    payload = gcn_notice.to_gcn_dict()
    assert payload["$schema"] == GCN_SNEWS2_SCHEMA_ID
    assert payload["snews_schema_version"] == "1.0"
    assert_valid_gcn(payload, gcn_validator, declared_properties)


def test_coincidence_update_and_missing_far(gcn_validator, declared_properties):
    """An UPDATE with far 'N/A' (as snews_cs can send) still produces a valid notice."""
    gcn_notice = transform_snews2_to_gcn(
        make_coincidence_alert(alert_type="COINC_MSG", sub_list_number=2, false_alarm_prob="N/A")
    )
    assert gcn_notice.alert_tense == "current"
    assert gcn_notice.alert_type == "update"
    assert gcn_notice.record_number == 3

    payload = gcn_notice.to_gcn_dict()
    assert "far" not in payload
    assert "false_alarm_prob" not in payload["tier_data"]
    assert_valid_gcn(payload, gcn_validator, declared_properties)


def test_test_gcn_notice_builder_matches_schema(gcn_validator, declared_properties):
    """The hand-built notice used by `gcn-produce-test` must also match the schema."""
    assert_valid_gcn(create_test_gcn_notice(), gcn_validator, declared_properties)
    assert_valid_gcn(create_test_gcn_notice(is_test=False, far=None), gcn_validator, declared_properties)


def test_transform_heartbeat_to_gcn(gcn_validator, declared_properties):
    """Verify that a Heartbeat alert is correctly transformed into GCN format."""
    msg = HeartbeatMessage(
        detector_name="IceCube",
        detector_status="ON",
        machine_time_utc="2025-01-15T14:35:00.000000+00:00",
        is_test=False,
        is_firedrill=False
    )

    gcn_notice = transform_snews2_to_gcn(msg)

    assert gcn_notice.snews2_tier == Tier.HEART_BEAT
    assert gcn_notice.alert_tense == "current"
    assert gcn_notice.detector_status == {"IceCube": "on"}
    assert gcn_notice.event_times_utc == ["2025-01-15T14:35:00.000000000Z"]
    assert gcn_notice.tier_data["detector_status"] == "ON"
    assert "machine_time_utc" not in gcn_notice.tier_data # excluded base field
    assert_valid_gcn(gcn_notice.to_gcn_dict(), gcn_validator, declared_properties)


def test_transform_timing_tier_to_gcn(gcn_validator, declared_properties):
    """Verify that a TimingTier alert is correctly transformed into GCN format."""
    msg = TimingTierMessage(
        detector_name="Borexino",
        neutrino_time_utc="2025-01-15T14:30:00.123456+00:00",
        start_time_utc="2025-01-15T14:30:00.000000+00:00",
        timing_series=[1000, 2000, 3000],
        detection_channel=DetectionChannel.NU_E,
        is_firedrill=True,
        is_test=True
    )

    gcn_notice = transform_snews2_to_gcn(msg)

    assert gcn_notice.snews2_tier == Tier.TIMING_TIER
    assert gcn_notice.alert_tense == "test"
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.123456000Z"]
    assert gcn_notice.tier_data["start_time_utc"] == "2025-01-15T14:30:00.000000000Z"
    assert gcn_notice.tier_data["timing_series"] == [1000, 2000, 3000]
    assert gcn_notice.tier_data["detection_channel"] == DetectionChannel.NU_E.value
    assert_valid_gcn(gcn_notice.to_gcn_dict(), gcn_validator, declared_properties)


def test_transform_retraction_to_gcn(gcn_validator, declared_properties):
    """Verify that a Retraction alert is correctly transformed into GCN format."""
    msg = RetractionMessage(
        detector_name="Super-K",
        retract_latest_n=1,
        retraction_reason="False alarm",
        machine_time_utc="2025-01-15T14:40:00.000000+00:00"
    )

    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.alert_type == "retraction"
    assert gcn_notice.tier_data["retract_latest_n"] == 1
    assert gcn_notice.tier_data["retraction_reason"] == "False alarm"
    assert_valid_gcn(gcn_notice.to_gcn_dict(), gcn_validator, declared_properties)


def test_transform_significance_to_gcn(gcn_validator, declared_properties):
    """Verify that a SignificanceTier alert is correctly transformed into GCN format."""
    msg = SignificanceTierMessage(
        detector_name="HALO",
        p_values=[0.1, 0.05, 0.01],
        t_bin_width_sec=0.5,
        machine_time_utc="2025-01-15T14:30:00.000000+00:00"
    )

    gcn_notice = transform_snews2_to_gcn(msg)
    assert gcn_notice.event_times_utc == ["2025-01-15T14:30:00.000000000Z"]
    assert gcn_notice.tier_data["p_values"] == [0.1, 0.05, 0.01]
    assert gcn_notice.tier_data["t_bin_width_sec"] == 0.5
    assert_valid_gcn(gcn_notice.to_gcn_dict(), gcn_validator, declared_properties)
