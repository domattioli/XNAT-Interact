"""
Test outcome summary schema conformance (T030).

Offline validation of OutcomeSummaryWriter output against contracts/outcome-summary.schema.json.
No requires_server or slow markers — this is a fast, offline test suitable for the default CI lane.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest


def _load_schema() -> dict:
    """Load the outcome summary JSON schema."""
    schema_path = (
        Path(__file__).parent.parent.parent.parent
        / "specs"
        / "014-real-xnat-integration-testing"
        / "contracts"
        / "outcome-summary.schema.json"
    )
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema not found: {schema_path}")
    return json.loads(schema_path.read_text())


def _validate_json_against_schema(data: dict, schema: dict) -> list:
    """
    Manual JSON schema validation (no jsonschema dependency).

    Returns a list of validation errors (empty if valid).
    """
    errors = []

    # Check required root properties
    required_root = schema.get("required", [])
    for req in required_root:
        if req not in data:
            errors.append(f"Missing required field: {req}")

    # Validate each required field's type and constraints
    properties = schema.get("properties", {})

    # case_name: enum
    if "case_name" in data:
        valid_cases = properties.get("case_name", {}).get("enum", [])
        if data["case_name"] not in valid_cases:
            errors.append(
                f"case_name must be one of {valid_cases}, got {data['case_name']}"
            )

    # run_timestamp: ISO datetime string
    if "run_timestamp" in data:
        try:
            datetime.fromisoformat(data["run_timestamp"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            errors.append(f"run_timestamp must be ISO-8601, got {data['run_timestamp']}")

    # files_processed, files_recovered, etc.: non-negative integers
    int_fields = [
        "files_processed",
        "files_recovered",
        "files_rejected",
        "checksum_pass_count",
        "checksum_fail_count",
        "uid_collisions_resolved",
    ]
    for field in int_fields:
        if field in data:
            if not isinstance(data[field], int):
                errors.append(f"{field} must be an integer, got {type(data[field])}")
            elif data[field] < 0:
                errors.append(f"{field} must be non-negative, got {data[field]}")

    # phases: array of objects
    if "phases" in data:
        if not isinstance(data["phases"], list):
            errors.append(f"phases must be an array, got {type(data['phases'])}")
        else:
            for i, phase_record in enumerate(data["phases"]):
                # Check required phase fields
                phase_required = ["case_name", "phase", "status", "timestamp"]
                for req in phase_required:
                    if req not in phase_record:
                        errors.append(
                            f"phases[{i}] missing required field: {req}"
                        )

                # Validate phase enum
                valid_phases = [
                    "ingestion",
                    "deidentification",
                    "annotation",
                    "segmentation",
                    "staple",
                    "integrity_validation",
                ]
                if "phase" in phase_record and phase_record["phase"] not in valid_phases:
                    errors.append(
                        f"phases[{i}].phase must be one of {valid_phases}, got {phase_record['phase']}"
                    )

                # Validate status enum
                valid_statuses = ["success", "failure", "partial"]
                if "status" in phase_record and phase_record["status"] not in valid_statuses:
                    errors.append(
                        f"phases[{i}].status must be one of {valid_statuses}, got {phase_record['status']}"
                    )

                # Validate timestamp
                if "timestamp" in phase_record:
                    try:
                        datetime.fromisoformat(phase_record["timestamp"].replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        errors.append(
                            f"phases[{i}].timestamp must be ISO-8601, got {phase_record['timestamp']}"
                        )

                # files_affected, xnat_resource_links should be arrays of strings
                if "files_affected" in phase_record:
                    if not isinstance(phase_record["files_affected"], list):
                        errors.append(
                            f"phases[{i}].files_affected must be an array"
                        )
                if "xnat_resource_links" in phase_record:
                    if not isinstance(phase_record["xnat_resource_links"], list):
                        errors.append(
                            f"phases[{i}].xnat_resource_links must be an array"
                        )

                # error_detail should be string or null
                if "error_detail" in phase_record:
                    if phase_record["error_detail"] is not None and not isinstance(
                        phase_record["error_detail"], str
                    ):
                        errors.append(
                            f"phases[{i}].error_detail must be string or null"
                        )

    return errors


class TestOutcomeSummarySchema:
    """Offline schema validation tests."""

    def test_valid_knee_2025_outcome_conformance(self):
        """T030: Validate a minimal KNEE_2025 outcome against the schema."""
        from tests.integration.live_xnat.outcome_summary import (
            OutcomeSummaryWriter,
            PipelinePhaseRecord,
        )

        schema = _load_schema()

        # Create a mock outcome
        writer = OutcomeSummaryWriter()
        now = datetime.utcnow().isoformat() + "Z"

        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="ingestion",
                status="success",
                timestamp=now,
                files_affected=["file1.dcm", "file2.dcm"],
                xnat_resource_links=["/data/experiments/exp1/resources/DICOM"],
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="deidentification",
                status="success",
                timestamp=now,
                files_affected=["file1.dcm", "file2.dcm"],
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="annotation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="segmentation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="staple",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="KNEE_2025",
                phase="integrity_validation",
                status="success",
                timestamp=now,
                files_affected=["file1.dcm", "file2.dcm"],
            )
        )

        # Get the outcome as dict
        summary = writer._records
        outcome_dict = {
            "case_name": "KNEE_2025",
            "run_timestamp": now,
            "files_processed": 2,
            "files_recovered": 0,
            "files_rejected": 0,
            "checksum_pass_count": 2,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 0,
            "uid_collisions_recorded": 0,
            "absent_tags": {},
            "dispositions": {},
            "phases": [r.__dict__ for r in summary],
        }

        # Validate against schema
        errors = _validate_json_against_schema(outcome_dict, schema)
        assert len(errors) == 0, f"Schema validation errors: {errors}"

    def test_valid_hip_2024_outcome_conformance(self):
        """T030: Validate a minimal HIP_2024 outcome against the schema."""
        from tests.integration.live_xnat.outcome_summary import (
            OutcomeSummaryWriter,
            PipelinePhaseRecord,
        )

        schema = _load_schema()

        # Create a mock outcome
        writer = OutcomeSummaryWriter()
        now = datetime.utcnow().isoformat() + "Z"

        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="ingestion",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="deidentification",
                status="partial",
                timestamp=now,
                error_detail="Some files had missing tags",
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="annotation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="segmentation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="staple",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="HIP_2024",
                phase="integrity_validation",
                status="success",
                timestamp=now,
            )
        )

        # Get the outcome as dict
        summary = writer._records
        outcome_dict = {
            "case_name": "HIP_2024",
            "run_timestamp": now,
            "files_processed": 5,
            "files_recovered": 1,
            "files_rejected": 0,
            "checksum_pass_count": 5,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 0,
            "uid_collisions_recorded": 0,
            "absent_tags": {},
            "dispositions": {},
            "phases": [r.__dict__ for r in summary],
        }

        # Validate against schema
        errors = _validate_json_against_schema(outcome_dict, schema)
        assert len(errors) == 0, f"Schema validation errors: {errors}"

    def test_valid_radiofluoro_2026_outcome_conformance(self):
        """T030: Validate a minimal RADIOFLUORO_2026 outcome against the schema."""
        from tests.integration.live_xnat.outcome_summary import (
            OutcomeSummaryWriter,
            PipelinePhaseRecord,
        )

        schema = _load_schema()

        # Create a mock outcome with UID collisions and rejections
        writer = OutcomeSummaryWriter()
        now = datetime.utcnow().isoformat() + "Z"

        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="ingestion",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="deidentification",
                status="partial",
                timestamp=now,
                error_detail="Corrupted and truncated files recorded",
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="annotation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="segmentation",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="staple",
                status="success",
                timestamp=now,
            )
        )
        writer.add_phase_record(
            PipelinePhaseRecord(
                case_name="RADIOFLUORO_2026",
                phase="integrity_validation",
                status="success",
                timestamp=now,
            )
        )

        # Get the outcome as dict
        summary = writer._records
        outcome_dict = {
            "case_name": "RADIOFLUORO_2026",
            "run_timestamp": now,
            "files_processed": 8,
            "files_recovered": 1,
            "files_rejected": 2,
            "checksum_pass_count": 8,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 1,
            "uid_collisions_recorded": 0,
            "absent_tags": {},
            "dispositions": {},
            "phases": [r.__dict__ for r in summary],
        }

        # Validate against schema
        errors = _validate_json_against_schema(outcome_dict, schema)
        assert len(errors) == 0, f"Schema validation errors: {errors}"

    def test_schema_rejects_missing_required_fields(self):
        """T030: Ensure schema validation catches missing required fields."""
        schema = _load_schema()

        # Outcome missing case_name
        incomplete_outcome = {
            "run_timestamp": "2026-07-07T12:00:00Z",
            "files_processed": 5,
            "files_recovered": 0,
            "files_rejected": 0,
            "checksum_pass_count": 5,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 0,
            "phases": [],
        }

        errors = _validate_json_against_schema(incomplete_outcome, schema)
        assert len(errors) > 0, "Validation should catch missing case_name"
        assert any("case_name" in e for e in errors)

    def test_schema_rejects_invalid_case_name(self):
        """T030: Ensure schema validation catches invalid case_name enum."""
        schema = _load_schema()

        # Outcome with invalid case name
        invalid_outcome = {
            "case_name": "INVALID_CASE",
            "run_timestamp": "2026-07-07T12:00:00Z",
            "files_processed": 5,
            "files_recovered": 0,
            "files_rejected": 0,
            "checksum_pass_count": 5,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 0,
            "phases": [],
        }

        errors = _validate_json_against_schema(invalid_outcome, schema)
        assert len(errors) > 0, "Validation should catch invalid case_name"
        assert any("case_name" in e for e in errors)

    def test_schema_rejects_invalid_phase_status(self):
        """T030: Ensure schema validation catches invalid phase status."""
        schema = _load_schema()

        # Outcome with invalid phase status
        invalid_outcome = {
            "case_name": "KNEE_2025",
            "run_timestamp": "2026-07-07T12:00:00Z",
            "files_processed": 5,
            "files_recovered": 0,
            "files_rejected": 0,
            "checksum_pass_count": 5,
            "checksum_fail_count": 0,
            "uid_collisions_resolved": 0,
            "phases": [
                {
                    "case_name": "KNEE_2025",
                    "phase": "ingestion",
                    "status": "invalid_status",  # Should be success/failure/partial
                    "timestamp": "2026-07-07T12:00:00Z",
                }
            ],
        }

        errors = _validate_json_against_schema(invalid_outcome, schema)
        assert len(errors) > 0, "Validation should catch invalid phase status"
        assert any("status" in e for e in errors)


class TestRevision2OutcomeFields:
    """T007 (spec 014 rev 2): writer output carries the FR-019 fields and six phases."""

    @staticmethod
    def _six_records(case_name: str):
        from tests.integration.live_xnat.outcome_summary import PHASES, PipelinePhaseRecord

        now = datetime.utcnow().isoformat() + "Z"
        return [PipelinePhaseRecord(case_name=case_name, phase=p, status="success", timestamp=now) for p in PHASES]

    def test_writer_output_has_revision2_fields(self, tmp_path):
        from tests.integration.live_xnat.outcome_summary import OutcomeSummaryWriter

        schema = _load_schema()
        writer = OutcomeSummaryWriter(output_dir=tmp_path)
        for r in self._six_records("HIP_2024"):
            writer.add_phase_record(r)
        out = writer.write_outcome(
            case_name="HIP_2024", files_processed=20, files_recovered=0, files_rejected=0,
            checksum_pass_count=20, checksum_fail_count=0, uid_collisions_resolved=0,
            uid_collisions_recorded=0,
            absent_tags={"hip_fluoro_faint_000.dcm": ["InstitutionName"]},
            dispositions={"hip_fluoro_faint_000.dcm": "published"},
        )
        data = json.loads(out.read_text())
        for field_name in ("uid_collisions_recorded", "absent_tags", "dispositions"):
            assert field_name in schema["required"]
            assert field_name in data
        assert isinstance(data["uid_collisions_recorded"], int)
        assert all(isinstance(v, list) for v in data["absent_tags"].values())
        allowed = set(schema["properties"]["dispositions"]["additionalProperties"]["enum"])
        assert set(data["dispositions"].values()) <= allowed
        assert len(data["phases"]) == 6
        assert _validate_json_against_schema(data, schema) == []

    def test_writer_refuses_five_phases(self, tmp_path):
        from tests.integration.live_xnat.outcome_summary import OutcomeSummaryWriter

        writer = OutcomeSummaryWriter(output_dir=tmp_path)
        for r in self._six_records("KNEE_2025")[:5]:
            writer.add_phase_record(r)
        with pytest.raises(ValueError):
            writer.write_outcome("KNEE_2025", 1, 0, 0, 1, 0, 0)

    def test_writer_refuses_foreign_case_records(self, tmp_path):
        from tests.integration.live_xnat.outcome_summary import OutcomeSummaryWriter

        writer = OutcomeSummaryWriter(output_dir=tmp_path)
        records = self._six_records("KNEE_2025")
        records[0].case_name = "HIP_2024"
        for r in records:
            writer.add_phase_record(r)
        with pytest.raises(ValueError):
            writer.write_outcome("KNEE_2025", 1, 0, 0, 1, 0, 0)

    def test_schema_requires_exactly_six_phases(self):
        schema = _load_schema()
        assert schema["properties"]["phases"]["minItems"] == 6
        assert schema["properties"]["phases"]["maxItems"] == 6
