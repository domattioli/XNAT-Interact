"""
scripts/measure_device_profile.py — offline, twin data only, non-interactive (--boxes).

Checks: writes only the device key + boxes in the profile schema, the written
profile drives the pixel review, and no header value, file name or pixel is
printed or written.
"""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

from tests import synthetic_twins as tw

REPO = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("measure_device_profile", REPO / "scripts" / "measure_device_profile.py")
mdp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mdp)

BOXES = "8,100,120,120;200,4,250,18"


@pytest.fixture(scope="module")
def twin089(tmp_path_factory):
    return tw.make_twin_case(tmp_path_factory.mktemp("mdp"), "089")


def _run(capsys, *args):
    rc = mdp.main([str(a) for a in args])
    cap = capsys.readouterr()
    return rc, cap.out + cap.err


def test_writes_only_key_and_boxes(twin089, tmp_path, capsys):
    rc, text = _run(capsys, twin089["folder"], "--boxes", BOXES, "--out-dir", tmp_path)
    assert rc == 0
    files = list(tmp_path.glob("*.json"))
    assert [f.name for f in files] == [f"{twin089['device_key']}.json"]
    data = json.loads(files[0].read_text())
    assert set(data) == {"device_id", "description", "boxes", "tolerance"}
    assert data["device_id"] == twin089["device_key"]
    assert data["boxes"] == [[8, 100, 120, 120], [200, 4, 250, 18]]
    assert data["tolerance"] == 8
    from src.services.pixel_deid.profiles import load_profiles
    assert twin089["device_key"] in load_profiles(str(tmp_path))
    assert "Saved 2 box(es)" in text


def test_no_header_value_file_name_or_folder_leaks(twin089, tmp_path, capsys):
    rc, text = _run(capsys, twin089["folder"], "--boxes", BOXES, "--out-dir", tmp_path)
    assert rc == 0
    written = (tmp_path / f"{twin089['device_key']}.json").read_text()
    secrets = [p["value"] for p in twin089["header_plants"]] + list(twin089["invented_dates"])
    secrets += [f["path"] for f in twin089["files"]] + [twin089["folder"], Path(twin089["folder"]).name]
    for s in secrets:
        assert s not in text and s not in written, s
    assert "ZZTWIN" not in text + written and "SYNTH" not in text + written


def test_profile_from_tool_matches_twin_fixture_and_drives_review(twin089, tmp_path, capsys, monkeypatch):
    from src.services.pixel_deid import detect, verdict
    from app.logic.upload import automated_pixel_review
    from src.xnat_experiment_data import ReviewDecision

    monkeypatch.setattr(detect, "multipass_detect", lambda *a, **k: [])
    monkeypatch.setattr(verdict, "_check_phi_outside_mask", lambda *a, **k: False)
    assert _run(capsys, twin089["folder"], "--boxes", BOXES, "--out-dir", tmp_path)[0] == 0
    fixture = json.loads((REPO / "tests/fixtures/device_profiles_twins" / f"{twin089['device_key']}.json").read_text())
    assert json.loads((tmp_path / f"{twin089['device_key']}.json").read_text())["boxes"] == fixture["boxes"]
    decision, boxes = automated_pixel_review(twin089["folder"], profiles_dir=str(tmp_path))
    assert decision == ReviewDecision.REDACT and boxes


def test_refuses_overwrite_without_force(twin089, tmp_path, capsys):
    assert _run(capsys, twin089["folder"], "--boxes", BOXES, "--out-dir", tmp_path)[0] == 0
    rc, text = _run(capsys, twin089["folder"], "--boxes", "0,0,5,5", "--out-dir", tmp_path)
    assert rc == 2 and "--force" in text
    assert _run(capsys, twin089["folder"], "--boxes", "0,0,5,5", "--out-dir", tmp_path, "--force")[0] == 0
    assert json.loads((tmp_path / f"{twin089['device_key']}.json").read_text())["boxes"] == [[0, 0, 5, 5]]


def test_mixed_machines_stop(tmp_path, capsys):
    twins = tw.make_all_twins(tmp_path / "all")
    folders = [twins["015"]["folder"], twins["089"]["folder"]]
    assert {twins["015"]["device_key"], twins["089"]["device_key"]} == {tw.KEY_SUPERC, tw.KEY_VAS}
    rc, text = _run(capsys, *folders, "--boxes", BOXES, "--out-dir", tmp_path / "out")
    assert rc == 2 and "different machines" in text
    assert not (tmp_path / "out").exists()


def test_boxes_clipped_ordered_and_bad_input_stops(twin089, tmp_path, capsys):
    assert mdp.clean_boxes([(50, 60, 10, 20), (-5, -5, 9999, 3), (3, 3, 3, 9)], (128, 256)) == [
        (10, 20, 50, 60), (0, 0, 256, 3)]
    rc, text = _run(capsys, twin089["folder"], "--boxes", "1,2,3", "--out-dir", tmp_path)
    assert rc == 2 and "four numbers" in text
    rc, text = _run(capsys, tmp_path / "empty_nothing_here", "--boxes", BOXES, "--out-dir", tmp_path)
    assert rc == 2 and "No readable DICOM" in text
