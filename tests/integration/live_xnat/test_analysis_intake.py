"""
Spec 016 live tests: the analysis intake against a real XNAT (FR-026).

Uses the shared KNEE_2025 publish (never publishing that case itself), downloads
it through production to get a real download record, and publishes stub
analysis results through ``run_intake``.  Requires the spec 014 local test bed.

Cleanup: nothing is deleted afterwards.  The local XNAT image refuses admin
DELETE (SPEC-ISSUE-15), and every publish lands as a new ``__v<n>`` version,
so a leftover result never collides with the next run.
"""
from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pytest

from tests.integration.live_xnat import helpers as H
from tests.integration.live_xnat.stub_analyses import STUB_CODE_REF, write_angles_csv

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

SYNTHETIC_NAME = "Jane Q Synthetic"


def stub_classifier(text):
    # The real text classifier needs requirements-pixeldeid.txt (Presidio and a
    # language model), which the live test bed does not install.  This stub
    # flags only the synthetic name the dirty folder plants, so the rest of the
    # intake runs for real.  test_no_classifier_fails_closed proves that without
    # any classifier the intake refuses instead of passing text unchecked.
    return (True, ["PERSON"]) if SYNTHETIC_NAME in text else (False, [])


@pytest.fixture(scope="module")
def downloaded(live_xnat_server, knee_rf_published):
    from app.logic.download import download_selection
    from app.logic.download_manifest import ManifestIdentity

    result = knee_rf_published["rf"]
    assert result.scan_uri, f"publish failed: {result.production_exception or result.publish_exception}"
    dest = knee_rf_published["work"] / "intake_download"
    gw = live_xnat_server["gateway_session"]()
    row = [{"subject": result.subject_label, "experiment": result.experiment_label,
            "scan_id": result.scan_uri.rstrip("/").split("/")[-1]}]
    identity = ManifestIdentity(username=live_xnat_server["username"], server_url=live_xnat_server["server_url"])
    outcome = download_selection(gw.server, live_xnat_server["project_name"], row, dest, identity=identity, uploader=gw)
    assert outcome.ok, outcome.friendly
    manifest = Path(outcome.manifest_path)
    entries = json.loads(manifest.read_text(encoding="utf-8"))["files"]
    case_uid = next(e["case_uid"] for e in entries if e.get("case_uid"))
    return {"manifest": manifest, "entries": [e for e in entries if e.get("case_uid") == case_uid],
            "case_uid": case_uid, "work": knee_rf_published["work"], "dest": dest}


def _folder(downloaded, name, *, notes="", supersedes=None):
    folder = downloaded["work"] / name
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    (folder / "analysis.json").write_text(json.dumps({
        "descriptor_version": "1", "type_name": "knee_flexion_angle", "type_version": 1,
        "run": {"case_uid": downloaded["case_uid"], "code_ref": STUB_CODE_REF, "parameters": {},
                "notes": notes, "supersedes": supersedes}}), encoding="utf-8")
    write_angles_csv(folder, downloaded["entries"])
    return folder


def _intake(live, downloaded, folder, **kw):
    from src.services.analysis_intake import run_intake
    kw.setdefault("classifier", stub_classifier)
    return run_intake(folder, gateway=live["gateway_session"](), config_tables=live["config_factory"](),
                      username=live["username"], manifest=downloaded["manifest"], **kw)


def _published_qs(live, result):
    run = result.descriptor["run"]
    if run["placement_used"] == "assessor":
        return f"{run['experiment_query_string']}/assessor/{result.label}", "KNEE_FLEXION_ANGLE"
    return run["input_refs"][0], result.label


@pytest.fixture(scope="module")
def first_publish(live_xnat_server, downloaded):
    folder = _folder(downloaded, "intake_clean")
    result = _intake(live_xnat_server, downloaded, folder)
    assert result.status == "done", result.friendly
    return {"folder": folder, "result": result}


def test_dirty_folders_write_nothing(live_xnat_server, downloaded):
    from src.services import xnat_conventions as conventions
    gw = live_xnat_server["gateway_session"]()
    exp_qs = downloaded["entries"][0]["scan_query_string"].split("/scan")[0]
    before = sorted(gw.list_assessors(exp_qs))
    folder = _folder(downloaded, "intake_dirty_notes", notes=f"Patient {SYNTHETIC_NAME}")
    assert _intake(live_xnat_server, downloaded, folder).status == "refused"
    folder = _folder(downloaded, "intake_dirty_extra")
    (folder / "scratch.txt").write_text("left over\n")
    assert _intake(live_xnat_server, downloaded, folder).status == "refused"
    assert sorted(gw.list_assessors(exp_qs)) == before
    assert conventions  # module import is part of the check that production code loads


def test_no_classifier_fails_closed(live_xnat_server, downloaded):
    from src.services.analysis_intake import gates
    folder = _folder(downloaded, "intake_no_classifier")
    try:
        gates.default_classifier()
        pytest.skip("the PHI text tools are installed here, so the missing-tool path cannot be shown")
    except gates.ClassifierUnavailable:
        pass
    result = _intake(live_xnat_server, downloaded, folder, classifier=None)
    assert result.status == "refused" and "PHI" in result.friendly.title


def test_clean_publish_matches_server(live_xnat_server, first_publish, tmp_path):
    result = first_publish["result"]
    run = result.descriptor["run"]
    assert result.label.startswith(("knee_flexion_angle__v", "KNEE_FLEXION_ANGLE__v"))
    if run["placement_used"] == "scan_resource":
        assert run["fallback_reason"]
    qs, resource = _published_qs(live_xnat_server, result)
    gw = live_xnat_server["gateway_session"]()
    paths = gw.download_resource(qs, resource, tmp_path)
    got = {Path(p).name: H.sha256(Path(p).read_bytes()) for p in paths}
    assert got["angles.csv"] == H.sha256((first_publish["folder"] / "angles.csv").read_bytes())
    assert "analysis.json" in got


def test_supersedes_lands_as_next_version(live_xnat_server, downloaded, first_publish):
    first = first_publish["result"]
    folder = _folder(downloaded, "intake_second", supersedes=first.label)
    second = _intake(live_xnat_server, downloaded, folder)
    assert second.status == "done", second.friendly
    base, n = first.label.rsplit("__v", 1)
    assert second.label == f"{base}__v{int(n) + 1}"
    qs, resource = _published_qs(live_xnat_server, first)
    assert live_xnat_server["gateway_session"]().list_files(qs, resource)


def test_catalog_row_after_fresh_pull(live_xnat_server, first_publish):
    cfg = live_xnat_server["config_factory"]()
    assert cfg.item_exists("ANALYSES", first_publish["result"].label)


@pytest.mark.known_issue
@pytest.mark.xfail(strict=True, reason="#63: source frames pass on hash plus a human click; burned-in text is not checked automatically (FR-014)")
def test_source_frame_with_burned_in_text_is_stopped(live_xnat_server, downloaded, tmp_path):
    from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR
    types_dir = tmp_path / "types"
    shutil.copytree(ANALYSIS_TYPES_DIR / "schemas", types_dir / "schemas")
    (types_dir / "consensus_with_frames.json").write_text(json.dumps({
        "type_name": "consensus_with_frames", "type_version": 1,
        "description": "Test-only type: a mask plus the source frame it was drawn on.",
        "inputs": ["source_frames"], "outputs": [{"pattern": "*.npz", "format": "npz", "mask": True},
                                                 {"pattern": "*.dcm", "format": "dcm"}],
        "placement": "assessor", "resource_label": "SEGMENTATION_CONSENSUS",
        "label_template": "CONSENSUS_WITH_FRAMES-{case_uid}", "phi_policy": ["pixels_from_source_only"]}))
    folder = tmp_path / "frames_out"
    folder.mkdir()
    entry = downloaded["entries"][0]
    shutil.copy(downloaded["dest"] / entry["relative_path"], folder / "frame.dcm")
    np.savez(folder / "mask.npz", mask=np.zeros((4, 4), dtype=np.uint8))
    (folder / "analysis.json").write_text(json.dumps({
        "descriptor_version": "1", "type_name": "consensus_with_frames", "type_version": 1,
        "run": {"case_uid": downloaded["case_uid"], "code_ref": STUB_CODE_REF, "parameters": {}, "notes": ""}}))
    try:
        from src.services.pixel_deid import verdict
        verdict._get_analyzer()
    except Exception as exc:  # noqa: BLE001
        # Without the text classifier the intake fails closed (FR-026) and refuses every
        # frame, which would make this strict xfail pass for the wrong reason.
        pytest.skip(f"text classifier not installed ({type(exc).__name__}); install requirements-pixeldeid.txt")
    result = _intake(live_xnat_server, downloaded, folder, types_dir=types_dir,
                     confirmer=lambda context: ("confirmed", []))
    # The KNEE_2025 frames carry synthetic burned-in text; a safe intake would refuse them.
    assert result.status == "refused"
