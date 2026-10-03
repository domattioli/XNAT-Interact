"""Spec 016 US1: provenance from the spec 015 download record."""
import json

import pytest

from src.services.analysis_intake import IntakeRefusal, fill_provenance, load_descriptor
from tests.fakes.analysis_folders import (
    CASE_UID, EXPERIMENT_QS, SCAN_QS, USERNAME, clean_angles_folder, make_frames, synthetic_manifest,
)


def _desc(case):
    return load_descriptor(case.output)


def test_manifest_filters_by_case(tmp_path):
    case = clean_angles_folder(tmp_path)
    other = make_frames(tmp_path / "other", "CASEB", 2)
    extra = json.loads(synthetic_manifest(tmp_path / "m2.json", other, tmp_path / "other").read_text())["files"]
    synthetic_manifest(case.manifest, case.frames, case.inputs, extra_entries=extra)
    desc, warnings = fill_provenance(_desc(case), manifest=case.manifest, username=USERNAME)
    run = desc["run"]
    assert len(run["source_hashes"]) == 3
    assert run["provenance"] == "recorded"
    assert run["input_refs"] == [SCAN_QS]
    assert run["experiment_query_string"] == EXPERIMENT_QS
    assert run["manifest_run_id"]
    assert warnings == []


def test_incomplete_manifest_refused(tmp_path):
    case = clean_angles_folder(tmp_path)
    synthetic_manifest(case.manifest, case.frames, case.inputs, complete=False)
    with pytest.raises(IntakeRefusal) as exc:
        fill_provenance(_desc(case), manifest=case.manifest, username=USERNAME)
    assert exc.value.friendly.recourse


def test_case_absent_from_manifest_refused(tmp_path):
    case = clean_angles_folder(tmp_path, case_uid="CASEA")
    desc = _desc(case)
    desc["run"]["case_uid"] = "CASEZ"
    with pytest.raises(IntakeRefusal) as exc:
        fill_provenance(desc, manifest=case.manifest, username=USERNAME)
    assert "CASEZ" in exc.value.friendly.message


def test_inputs_reconstructed_with_warning(tmp_path):
    case = clean_angles_folder(tmp_path)
    desc, warnings = fill_provenance(_desc(case), inputs=case.inputs, username=USERNAME)
    assert desc["run"]["provenance"] == "reconstructed"
    assert [h["sha256"] for h in desc["run"]["source_hashes"]] == [e["sha256"] for e in case.entries()]
    assert warnings and "reconstructed" in warnings[0]


def test_neither_refused(tmp_path):
    case = clean_angles_folder(tmp_path)
    with pytest.raises(IntakeRefusal):
        fill_provenance(_desc(case), username=USERNAME)


def test_producer_is_real_username(tmp_path):
    case = clean_angles_folder(tmp_path)
    desc, _ = fill_provenance(_desc(case), manifest=case.manifest, username=USERNAME)
    assert desc["run"]["producer"] == USERNAME
    assert desc["run"]["intake_started_at"] and desc["run"]["tool_version"]


def test_empty_username_refused(tmp_path):
    case = clean_angles_folder(tmp_path)
    with pytest.raises(IntakeRefusal):
        fill_provenance(_desc(case), manifest=case.manifest, username="  ")


def test_input_descriptor_not_mutated(tmp_path):
    case = clean_angles_folder(tmp_path)
    desc = _desc(case)
    fill_provenance(desc, manifest=case.manifest, username=USERNAME)
    assert "source_hashes" not in desc["run"]
    assert desc["run"]["case_uid"] == CASE_UID


def test_rest_plural_scan_path_is_normalised_for_pyxnat():
    """The real download manifest records plural REST paths (found live, 2026-10-03)."""
    from src.services.analysis_intake.provenance import experiment_qs_from_scan_qs, to_pyxnat_qs

    plural = "/projects/P/subjects/S/experiments/E/scans/0"
    assert to_pyxnat_qs(plural) == "/project/P/subject/S/experiment/E/scan/0"
    assert experiment_qs_from_scan_qs(plural) == "/project/P/subject/S/experiment/E"
    assert experiment_qs_from_scan_qs("/project/P/subject/S/experiment/E/scan/0") == "/project/P/subject/S/experiment/E"
