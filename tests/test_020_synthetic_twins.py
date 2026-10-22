"""
Spec 020: synthetic twin fluoro cases with ground truth.

Measures the header de-identification and the pixel review pipeline against a
written manifest of planted fake PHI. Offline, deterministic, no Tesseract.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pydicom
import pytest

from tests import synthetic_twins as tw

REPO = Path(__file__).resolve().parents[1]
TWIN_PROFILES = str(REPO / "tests" / "fixtures" / "device_profiles_twins")
SHIPPED_PROFILES = str(REPO / "data" / "device_profiles")
REDACT = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"
MARKERS = ("SYNTH", "ZZTWIN")


@pytest.fixture(scope="module")
def twins(tmp_path_factory):
    return tw.make_all_twins(tmp_path_factory.mktemp("twins"))


def _dicom_files(m):
    return [f for f in m["files"] if f["role"] in ("dicom", "duplicate")]


def _read(m, name):
    return pydicom.dcmread(str(Path(m["folder"]) / name))


def _tag_of(keyword):
    if keyword == tw.PRIVATE_KEYWORD:
        return pydicom.tag.Tag(*tw.PRIVATE_VALUE_TAG)
    return pydicom.tag.Tag(pydicom.datadict.tag_for_keyword(keyword))


def _all_values(ds):
    out = []
    for el in ds.iterall():
        if el.tag == (0x7FE0, 0x0010):
            continue
        out.append(str(el.value))
    return out


@pytest.fixture
def stub_detectors(monkeypatch):
    from src.services.pixel_deid import detect, verdict

    monkeypatch.setattr(detect, "multipass_detect", lambda *a, **k: [])
    monkeypatch.setattr(verdict, "_check_phi_outside_mask", lambda *a, **k: False)


# T003 -------------------------------------------------------------------
def test_manifest_and_bytes_deterministic(tmp_path):
    a = tw.make_all_twins(tmp_path / "a")
    b = tw.make_all_twins(tmp_path / "b")
    for cid in tw.CASE_IDS:
        ma, mb = dict(a[cid]), dict(b[cid])
        fa, fb = Path(ma.pop("folder")), Path(mb.pop("folder"))
        assert json.dumps(ma, sort_keys=True) == json.dumps(mb, sort_keys=True)
        for f in ma["files"]:
            assert (fa / f["path"]).read_bytes() == (fb / f["path"]).read_bytes()


def test_every_planted_string_is_marked_synthetic(twins):
    for m in twins.values():
        assert m["header_plants"]
        for p in m["header_plants"]:
            val = p["value"]
            if val in m["invented_dates"]:
                continue
            assert any(k in val for k in MARKERS), (m["case_id"], p["tag"])
        for d in m["invented_dates"]:
            assert d.startswith("1900")
        for f in m["files"]:
            if f["role"] == "non_dicom":
                assert "SYNTH" in (Path(m["folder"]) / f["path"]).read_text()


def test_manifest_shape_and_coverage(twins):
    mods = {f["modality"] for m in twins.values() for f in m["files"] if f["modality"]}
    assert {"RF", "OT"} <= mods
    keys = {f["device_key"] for m in twins.values() for f in m["files"] if f["device_key"]}
    assert keys == {tw.KEY_SUPERC, tw.KEY_VAS}
    assert any(f["n_frames"] > 1 for m in twins.values() for f in m["files"])
    m15 = twins["015"]
    assert any(f["role"] == "duplicate" and "." not in f["path"] for f in m15["files"])
    assert any(f["path"].endswith(".json") for f in m15["files"])
    for key in ("case_id", "folder", "files", "header_plants", "control_tags", "pixel_plants",
                "expected_census", "expected_parse_error"):
        assert key in m15
    with pytest.raises(ValueError):
        tw.make_twin_case(Path("."), "999")


# T004 -------------------------------------------------------------------
def test_header_recall_redacted_plants_absent(twins):
    from src.services.deidentify import deidentify_dataset

    checked = 0
    for m in twins.values():
        for f in _dicom_files(m):
            ds = deidentify_dataset(_read(m, f["path"]), REDACT)
            values = _all_values(ds)
            for p in (p for p in m["header_plants"] if p["file"] == f["path"] and p["expected"] == "redacted"):
                checked += 1
                assert not any(p["value"] in v for v in values), (m["case_id"], f["path"], p["tag"])
            assert not [el for el in ds.iterall() if el.tag.is_private]
    assert checked > 0


# T005 -------------------------------------------------------------------
def test_header_precision_only_planted_contract_tags_change(twins):
    from src.services.deidentify import deidentify_dataset

    for m in twins.values():
        for f in _dicom_files(m):
            before = _read(m, f["path"])
            snapshot = {el.tag: (el.value if el.tag != (0x7FE0, 0x0010) else bytes(el.value)) for el in before}
            after = deidentify_dataset(copy.deepcopy(before), REDACT)
            allowed = {_tag_of(p["tag"]) for p in m["header_plants"]
                       if p["file"] == f["path"] and p["expected"] == "redacted"}
            allowed.add(pydicom.tag.Tag(*tw.PRIVATE_CREATOR_TAG))
            changed = set()
            for tag, old in snapshot.items():
                if tag not in after:
                    changed.add(tag)
                    continue
                new = after[tag].value if tag != (0x7FE0, 0x0010) else bytes(after[tag].value)
                if new != old:
                    changed.add(tag)
            assert changed, "nothing changed: de-id did not run"
            assert changed <= allowed, (m["case_id"], f["path"], changed - allowed)
            for c in (c for c in m["control_tags"] if c["file"] == f["path"]):
                assert str(after[_tag_of(c["tag"])].value) == str(c["value"]), (f["path"], c["tag"])


# T006 -------------------------------------------------------------------
def test_known_gap_survivors_pinned_exactly(twins):
    from src.services.deidentify import deidentify_dataset

    survivors, expected = set(), set()
    for m in twins.values():
        for f in _dicom_files(m):
            ds = deidentify_dataset(_read(m, f["path"]), REDACT)
            values = _all_values(ds)
            for p in (p for p in m["header_plants"] if p["file"] == f["path"]):
                key = (m["case_id"], f["path"], p["tag"])
                if any(p["value"] in v for v in values):
                    survivors.add(key)
                if p["expected"] == "known_gap":
                    expected.add(key)
    assert survivors == expected
    assert {k[2] for k in survivors} == set(tw.GAP_TAGS)


# T007 -------------------------------------------------------------------
def _frames(m, name):
    arr = _read(m, name).pixel_array
    return [arr] if arr.ndim == 2 else list(arr)


def _assess(m, name, profiles_dir):
    from src.services.pixel_deid.verdict import assess_case

    ds = _read(m, name)
    frames = _frames(m, name)
    return frames, assess_case(frames, ds, profiles_dir=profiles_dir)


def test_pixel_recall_and_precision_with_twin_profiles(twins, stub_detectors):
    from src.services.pixel_deid.verdict import Verdict

    multi = 0
    for m in twins.values():
        for f in _dicom_files(m):
            frames, a = _assess(m, f["path"], TWIN_PROFILES)
            multi += int(len(frames) > 1)
            assert a.verdict == Verdict.REDACTED, (m["case_id"], f["path"], a.reason)
            assert "profile" in a.tiers_fired
            if len(frames) > 1:
                assert "consensus" in a.tiers_fired
            plants = [p for p in m["pixel_plants"] if p["file"] == f["path"]]
            assert plants
            for p in plants:
                x0, y0, x1, y1 = p["box"]
                for i, mf in enumerate(a.masked_frames):
                    assert frames[i][y0:y1, x0:x1].min() == tw.PLANT_VALUE  # plant really present
                    assert (mf[y0:y1, x0:x1] == 0).all(), (f["path"], i, p["box"])
            outside = np.ones(frames[0].shape, dtype=bool)
            for x0, y0, x1, y1 in a.regions:
                outside[y0:y1, x0:x1] = False
            for src, out in zip(frames, a.masked_frames):
                assert np.array_equal(src[outside], out[outside])
    assert multi >= 2


def test_consensus_alone_covers_multiframe_plants(twins, stub_detectors):
    from src.services.pixel_deid import consensus

    m = twins["086"]
    boxes = consensus.variance_consensus_mask(_frames(m, "rf_multi.dcm"))
    for p in (p for p in m["pixel_plants"] if p["file"] == "rf_multi.dcm"):
        x0, y0, x1, y1 = p["box"]
        assert any(b[0] <= x0 and b[1] <= y0 and b[2] >= x1 and b[3] >= y1 for b in boxes), p


def test_shrunk_profile_box_is_caught(twins, stub_detectors, tmp_path):
    """Mutation guard (FR-008): a too-small profile box must fail the recall assertion."""
    prof = json.loads((Path(TWIN_PROFILES) / f"{tw.KEY_SUPERC}.json").read_text())
    prof["boxes"] = [[0, 4, 40, 22]]
    (tmp_path / "p.json").write_text(json.dumps(prof))
    m = twins["015"]
    frames, a = _assess(m, "rf_001.dcm", str(tmp_path))
    x0, y0, x1, y1 = m["pixel_plants"][0]["box"]
    assert not (a.masked_frames[0][y0:y1, x0:x1] == 0).all()


# T008 -------------------------------------------------------------------
def test_shipped_profiles_leave_ge_unprofiled_and_quarantine(twins, stub_detectors):
    from src.services.pixel_deid import profiles
    from src.services.pixel_deid.verdict import Verdict

    loaded = profiles.load_profiles(SHIPPED_PROFILES)
    assert tw.KEY_SUPERC not in loaded and tw.KEY_VAS not in loaded
    for m in twins.values():
        for f in _dicom_files(m):
            frames, a = _assess(m, f["path"], SHIPPED_PROFILES)
            assert a.verdict != Verdict.CLEAN
            if len(frames) == 1:
                assert a.verdict == Verdict.QUARANTINE
                assert a.reason == "unprofiled device, no positive clean evidence"


# T009 -------------------------------------------------------------------
def test_case_015_filter_and_content_detection(twins):
    from src.xnat_scan_data import ScanFile
    import inspect
    from src import xnat_experiment_data as xed

    m = twins["015"]
    folder = Path(m["folder"])
    src = inspect.getsource(xed)
    assert "f.suffix.lower() == '.dcm' or f.suffix == ''" in src
    picked = sorted(f.name for f in folder.rglob("*") if f.suffix.lower() == ".dcm" or f.suffix == "")
    expected = sorted(f["path"] for f in m["files"] if f["role"] in ("dicom", "duplicate"))
    assert picked == expected
    detect = ScanFile.is_dicom
    for name in picked:
        assert detect(None, folder / name)
    for f in m["files"]:
        if f["role"] == "non_dicom":
            assert not detect(None, folder / f["path"])
    # duplicates are byte-identical to their originals
    assert (folder / "rf_001_copy").read_bytes() == (folder / "rf_001.dcm").read_bytes()


# T010 -------------------------------------------------------------------
def _parse(ds):
    from src.xnat_scan_data import SourceDicomDeIdentified

    obj = object.__new__(SourceDicomDeIdentified)
    obj._metadata, obj._derived_metadata, obj._image = ds, {}, object()
    obj._parse_for_derived_metadata()
    return obj._derived_metadata


def test_missing_date_raises_named_error_with_next_step(twins):
    from src.xnat_scan_data import MissingAcquisitionDateError

    m = twins["015"]
    assert m["expected_parse_error"] == ["sc_nodate.dcm"]
    assert issubclass(MissingAcquisitionDateError, ValueError)
    with pytest.raises(MissingAcquisitionDateError) as ei:
        _parse(_read(m, "sc_nodate.dcm"))
    assert "date" in str(ei.value).lower() and len(str(ei.value)) > 40
    for name in (f["path"] for f in _dicom_files(m)):
        if name not in m["expected_parse_error"]:
            assert "DATETIME" in _parse(_read(m, name))


def test_date_fallback_uses_instance_creation_date(twins):
    d = _parse(_read(twins["015"], "sc_icd_only.dcm"))
    assert "1900-01-04" in d["DATETIME"]


def test_date_fallback_chain_order():
    from pydicom.dataset import Dataset

    ds = Dataset()
    ds.ContentDate, ds.ContentTime = "", ""
    ds.InstanceCreationDate = ""
    ds.SeriesDate, ds.SeriesTime = "19000105", "000000"
    ds.StudyDate = "19000106"
    assert "1900-01-05" in _parse(ds)["DATETIME"]
    ds2 = Dataset()
    ds2.StudyDate = "19000106"  # no time tag at all: empty time allowed
    assert "1900-01-06" in _parse(ds2)["DATETIME"]


def test_content_date_behaviour_unchanged():
    from pydicom.dataset import Dataset

    ds = Dataset()
    ds.ContentDate, ds.ContentTime = "19000102", "010202"
    ds.InstanceCreationDate, ds.InstanceCreationTime = "19000104", "010303"
    assert "1900-01-02" in _parse(ds)["DATETIME"]


# T011 -------------------------------------------------------------------
def test_probes_match_manifest(twins):
    from src.services.phi_safe import probes

    for m in twins.values():
        exp = m["expected_census"]
        c = probes.census(Path(m["folder"]))
        assert c["files"] == exp["files"]
        assert c["dicom"] == exp["dicom"] and c["parsed"] == exp["dicom"] and c["parse_failed"] == 0
        assert c["non_dcm_extension"] == exp["non_dcm_extension"]
        assert c["secondary_capture"] == exp["secondary_capture"]
        assert c["multiframe"] == exp["multiframe"]
        assert c["empty_study_date"] == exp["empty_study_date"]
        assert c["modalities"]["RF"] == exp["rf"] and c["modalities"]["OT"] == exp["ot"]
        shipped = probes.devices(Path(m["folder"]), profiles_dir=SHIPPED_PROFILES)["devices"]
        assert len(shipped) == 1 and shipped[0]["profiled"] is False
        assert shipped[0]["frames_sampled"] == exp["dicom"]
        twin = probes.devices(Path(m["folder"]), profiles_dir=TWIN_PROFILES)["devices"]
        assert len(twin) == 1 and twin[0]["profiled"] is True


def test_two_distinct_unprofiled_devices_across_twins(twins):
    from src.services.phi_safe import probes

    digests = set()
    for m in twins.values():
        for row in probes.devices(Path(m["folder"]), profiles_dir=SHIPPED_PROFILES)["devices"]:
            assert row["profiled"] is False
            digests.add(row["device_digest"])
    assert len(digests) == 2
