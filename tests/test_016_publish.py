"""Spec 016 US1: publish, keep-all versions, permission fallback and verify-by-download."""
import json
import tempfile
from pathlib import Path

import pytest

from src.services.analysis_intake import IntakeRefusal, load_types, publish_analysis, run_intake
from src.services.analysis_intake.publish import is_permission_error
from tests.fakes.analysis_folders import (
    EXPERIMENT, EXPERIMENT_QS, PROJECT, SCAN_QS, SUBJECT, USERNAME, FakeConfigTables,
    clean_angles_folder, consensus_folder, fake_name_classifier, write_analysis_json,
)
from tests.fakes.fake_xnat import FakeXNAT

WRITE_OPS = {"file.put", "file.insert", "resource.put_zip", "assessor.create", "assessor.file.put"}


def _fake():
    fx = FakeXNAT(project_name=PROJECT)
    fx.seed_rf_experiment(SUBJECT, EXPERIMENT)
    return fx


def _run(case, fx, ct=None, **kw):
    return run_intake(case.output, gateway=fx, config_tables=ct if ct is not None else FakeConfigTables(),
                      username=USERNAME, manifest=case.manifest, classifier=fake_name_classifier, **kw)


def _writes(fx):
    return [c for c in fx.calls if c["op"] in WRITE_OPS]


def test_assessor_v1_then_v2_keeps_v1(tmp_path):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    r1 = _run(case, fx)
    assert r1.status == "done" and r1.label == "knee_flexion_angle__v1"
    local = json.loads((case.output / "analysis.json").read_text())
    assert local["run"]["label"] == "knee_flexion_angle__v1" and local["run"]["placement_used"] == "assessor"
    write_analysis_json(case.output, supersedes="knee_flexion_angle__v1")
    r2 = _run(case, fx)
    assert r2.status == "done" and r2.label == "knee_flexion_angle__v2"
    v1 = fx.list_files(f"{EXPERIMENT_QS}/assessor/knee_flexion_angle__v1", "KNEE_FLEXION_ANGLE")
    assert sorted(v1) == ["analysis.json", "angles.csv"]
    assert set(fx.list_assessors(EXPERIMENT_QS)) == {"knee_flexion_angle__v1", "knee_flexion_angle__v2"}


def test_uploaded_descriptor_records_label_and_provenance(tmp_path):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    _run(case, fx)
    dest = Path(tempfile.mkdtemp())
    fx.download_resource(f"{EXPERIMENT_QS}/assessor/knee_flexion_angle__v1", "KNEE_FLEXION_ANGLE", dest)
    remote = json.loads((dest / "analysis.json").read_text())
    assert remote["run"]["label"] == "knee_flexion_angle__v1"
    assert remote["run"]["producer"] == USERNAME
    assert len(remote["run"]["source_hashes"]) == 3


def test_scan_resource_versioning(tmp_path):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    atype = load_types()["knee_flexion_angle"]
    from dataclasses import replace
    scan_type = replace(atype, placement="scan_resource")
    desc = json.loads((case.output / "analysis.json").read_text())
    from src.services.analysis_intake import fill_provenance
    desc, _ = fill_provenance(desc, manifest=case.manifest, username=USERNAME)
    o1 = publish_analysis(desc, case.output, fx, atype=scan_type)
    o2 = publish_analysis(desc, case.output, fx, atype=scan_type)
    assert (o1.label, o2.label) == ("KNEE_FLEXION_ANGLE__v1", "KNEE_FLEXION_ANGLE__v2")
    assert o1.verified and o2.verified
    assert sorted(fx.list_files(SCAN_QS, "KNEE_FLEXION_ANGLE__v1")) == ["analysis.json", "angles.csv"]


def test_permission_refusal_falls_back_to_scan_resource(tmp_path, monkeypatch):
    fx, case = _fake(), clean_angles_folder(tmp_path)

    def refuse_assessor(*args, **kwargs):
        raise PermissionError("403 Forbidden")
    monkeypatch.setattr(fx, "create_assessor", refuse_assessor)
    r = _run(case, fx)
    assert r.status == "done"
    assert r.label == "KNEE_FLEXION_ANGLE__v1"
    run = r.descriptor["run"]
    assert run["placement_used"] == "scan_resource" and run["fallback_reason"]
    assert any("scan resource" in w for w in r.warnings)


def test_non_permission_error_is_refused_not_fallen_back(tmp_path, monkeypatch):
    fx, case = _fake(), clean_angles_folder(tmp_path)

    def broken(*args, **kwargs):
        raise RuntimeError("disk full")
    monkeypatch.setattr(fx, "create_assessor", broken)
    r = _run(case, fx)
    assert r.status == "refused"
    assert not _writes(fx)


def test_is_permission_error_follows_cause():
    try:
        try:
            raise ValueError("HTTP 401 unauthorized")
        except ValueError as inner:
            raise RuntimeError("wrapped") from inner
    except RuntimeError as exc:
        assert is_permission_error(exc)
    assert not is_permission_error(RuntimeError("timeout"))


def test_no_write_uses_overwrite(tmp_path):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    _run(case, fx)
    for call in _writes(fx):
        assert call["kwargs"].get("overwrite") in (None, False)


def test_missing_supersedes_refused_before_write(tmp_path):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    write_analysis_json(case.output, supersedes="knee_flexion_angle__v7")
    r = _run(case, fx)
    assert r.status == "refused" and "knee_flexion_angle__v7" in r.friendly.message
    assert not _writes(fx)


def test_altered_server_file_is_not_verified(tmp_path, monkeypatch):
    fx, case = _fake(), clean_angles_folder(tmp_path)
    fx.set_file_content("angles.csv", b"frame_index\n0\n")
    made = []
    real = tempfile.mkdtemp

    def tracking(*a, **k):
        made.append(real(*a, **k))
        return made[-1]
    monkeypatch.setattr(tempfile, "mkdtemp", tracking)
    ct = FakeConfigTables()
    r = _run(case, fx, ct)
    assert r.status == "not_verified"
    assert "angles.csv" in r.friendly.message
    assert ct.server_rows == {}
    assert made and not any(Path(p).exists() for p in made)
    local = json.loads((case.output / "analysis.json").read_text())
    assert "label" not in local["run"]


def test_catalog_failure_then_retry_adds_row_without_republish(tmp_path):
    from tests.fakes.analysis_folders import LostUpdateError
    fx, case = _fake(), clean_angles_folder(tmp_path)
    ct = FakeConfigTables(push_failures=[LostUpdateError("a"), LostUpdateError("b")])
    r = _run(case, fx, ct)
    assert r.status == "catalog_failed" and r.friendly.recourse
    n_writes = len(_writes(fx))
    r2 = _run(case, fx, ct)
    assert r2.status == "done"
    assert len(_writes(fx)) == n_writes
    assert len(ct.server_rows["ANALYSES"]) == 1


def test_consensus_uses_label_template(tmp_path):
    fx, case = _fake(), consensus_folder(tmp_path)
    confirmed = lambda context: ("confirmed", [])  # noqa: E731
    r = _run(case, fx, confirmer=confirmed)
    assert r.status == "done", r.friendly
    assert r.label == "SEGMENTATION_CONSENSUS-CASEA__v1"


def test_hidden_session_falls_back_to_scan_resource(tmp_path):
    """Local server quirk (found live 2026-10-03): session files reachable, session not listed."""
    import inspect
    from src.services.analysis_intake import publish as P

    class HidingGateway:
        def __init__(self, inner):
            self._inner = inner
        def exists(self, qs):
            return "/scan/" in qs
        def __getattr__(self, name):
            return getattr(self._inner, name)

    assert P._session_hidden_from_listing(HidingGateway(object()), "/project/P/subject/S/experiment/E", "/project/P/subject/S/experiment/E/scan/0")
    assert not P._session_hidden_from_listing(HidingGateway(object()), "/project/P/subject/S/experiment/E/scan/0", "/project/P/subject/S/experiment/E/scan/0")
    assert "_session_hidden_from_listing(gateway, exp_qs, scan_qs)" in inspect.getsource(P.publish_analysis)
