"""
Tests for the PHI-safe probes (src/services/phi_safe).

The property under test: no value read from an imaging file can reach the
output. Every test uses synthetic data from tests/synthetic_data.py. The
"poison" strings below are fake, and the assertions are that none of them, nor
any fragment of them, appears anywhere in a rendered report.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pytest

from src.services.phi_safe import gate
from src.services.phi_safe.gate import GateError, SafeReport, check
from src.services.phi_safe import probes
from tests.synthetic_data import make_phi_dicom_dataset

REPO = Path(__file__).resolve().parent.parent

POISON = (
    "DOE^JOHN", "DOE", "JOHN", "MRN-0001234", "0001234", "SMITH^JANE", "ACC-987654",
    "UIOWA HOSPITAL", "super-secret-phi", "20240101", "1.2.840", "STUDY-42",
)


def _assert_clean(text: str) -> None:
    for needle in POISON:
        assert needle not in text, f"poison fragment reached the output"


# ------------------------------------------------------------------ gate ---

@pytest.mark.parametrize("value", [
    "DOE^JOHN", "20240101", "1.2.840.10008.5.1.4.1.1.7", "/Users/x/case/001.dcm",
    "MRN-0001234", "", " ", "quarantine ", "Quarantine", 1.5, None, b"DICM", object(),
    "deadbeef0", "DEADBEEF", "deadbeef", {"PatientName": "DOE^JOHN"}, {"free_key": 1},
    ["confirmed", "DOE^JOHN"], (1, 2, 3, 4.0), {"files": "20240101"}, {"device_digest": "20240101x"},
    {"StudyDate": "deadbeef"},
])
def test_gate_refuses_everything_outside_the_grammar(value):
    with pytest.raises(GateError):
        check(value)


@pytest.mark.parametrize("value", [
    True, 0, 7, "quarantine", "phi-safe-report/1", {"device_digest": "deadbeef"}, [1, 2, 3], {"files": 3},
    {"modalities": {"RF": 2, "other": 0}}, {"boxes": [[0, 0, 10, 10], [5, 5, 6, 6]]},
    {"devices": [{"device_digest": "0123abcd", "profiled": False, "frames_sampled": 2}]},
])
def test_gate_accepts_the_grammar(value):
    assert check(value) is not None


def test_gate_box_requires_four_ints_under_the_boxes_key():
    assert check({"boxes": [[1, 2, 3, 4]]}) == {"boxes": [[1, 2, 3, 4]]}
    with pytest.raises(GateError):
        check({"boxes": [[1, 2, 3]]})
    with pytest.raises(GateError):
        check({"boxes": [[1, 2, 3, "4"]]})
    with pytest.raises(GateError):
        check({"boxes": [[1, 2, 3, True]]})
    with pytest.raises(GateError):
        check({"boxes": [[[1, 2, 3, 4]]]})


def test_gate_unwraps_numpy_ints_and_refuses_numpy_floats():
    assert check(np.int64(3)) == 3
    assert check(np.bool_(True)) is True
    with pytest.raises(GateError):
        check(np.float32(0.5))


def test_gate_error_carries_no_payload():
    try:
        check("DOE^JOHN MRN-0001234")
    except GateError as exc:
        _assert_clean(str(exc))
        _assert_clean(repr(exc))
        assert exc.args == ("value outside the PHI-safe output grammar",)


def test_safe_report_rejects_unknown_key_and_renders_sorted_json():
    report = SafeReport()
    report.put("files", 2)
    with pytest.raises(GateError):
        report.put("PatientNameValue", "x")
    data = json.loads(report.render())
    assert data == {"files": 2, "schema": "phi-safe-report/1"}


def test_blocked_report_names_only_the_exception_class():
    text = gate.blocked(ValueError("DOE^JOHN at /Users/x/MRN-0001234.dcm"))
    _assert_clean(text)
    assert json.loads(text) == {"blocked": True, "error_class": "ValueError", "schema": "phi-safe-report/1"}


def test_digest_is_eight_lowercase_hex_and_deterministic():
    assert gate.digest("ge_healthcare_x") == gate.digest("ge_healthcare_x")
    assert len(gate.digest("a")) == 8 and gate.digest("a") == gate.digest("a").lower()
    check({"device_digest": gate.digest("anything")})
    with pytest.raises(GateError):
        check(gate.digest("anything"))  # a bare digest has no key, so no grammar rule admits it


def test_every_identifying_tag_and_modality_key_is_allowlisted():
    for tag in probes.IDENTIFYING_TAGS:
        assert tag in gate.ALLOWED_KEYS
    for mod in probes.MODALITY_KEYS + ("other",):
        assert mod in gate.ALLOWED_KEYS


# --------------------------------------------------------------- fixtures ---

def _write(ds, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.save_as(str(path))
    return path


def _banner_frames(n: int, rows: int = 96, cols: int = 160, seed: int = 0):
    """Frames with moving noise and one fixed bright banner at a known box."""
    rng = np.random.default_rng(seed)
    frames = []
    for _ in range(n):
        f = rng.integers(0, 600, size=(rows, cols), dtype=np.uint16)
        f[4:14, 6:70] = 3500  # the banner: x0=6, y0=4, x1=70, y1=14
        frames.append(f)
    return frames


@pytest.fixture
def poison_folder(tmp_path: Path) -> Path:
    """A case folder shaped like a real export, every value fake.

    Holds PHI-bearing DICOMs with a fixed banner, an extensionless duplicate,
    a Secondary Capture with an empty StudyDate, a stray json, a text file with
    poison in it, and file names that carry poison.
    """
    case = tmp_path / "DOE^JOHN_case"
    for i, frame in enumerate(_banner_frames(5)):
        ds = make_phi_dicom_dataset(rows=96, cols=160, seed=i)
        ds.Modality = "RF"
        ds.Manufacturer = "ACME"
        ds.ManufacturerModelName = "FLUORO 9000"
        ds.StudyDate = "20240101"
        ds.PixelData = frame.astype(np.uint16).tobytes()
        _write(ds, case / f"MRN-0001234_{i}.dcm")
    ds = make_phi_dicom_dataset(rows=96, cols=160, seed=9)
    ds.Modality = "RF"
    ds.Manufacturer = "ACME"
    ds.ManufacturerModelName = "FLUORO 9000"
    ds.StudyDate = "20240101"
    ds.PixelData = _banner_frames(1, seed=9)[0].astype(np.uint16).tobytes()
    _write(ds, case / "20240101")  # extensionless duplicate
    sc = make_phi_dicom_dataset(rows=8, cols=8, seed=3)  # Secondary Capture, no StudyDate
    sc.Modality = "OT"
    _write(sc, case / "SMITH^JANE.dcm")
    (case / "Results.json").write_text('{"name": "DOE^JOHN"}', encoding="utf-8")
    (case / "notes.txt").write_text("super-secret-phi MRN-0001234", encoding="utf-8")
    return case


# ----------------------------------------------------------------- probes ---

def test_census_counts_without_values(poison_folder: Path):
    out = probes.census(poison_folder)
    _assert_clean(json.dumps(out))
    assert out["files"] == 9
    assert out["dicom"] == 7 and out["parsed"] == 7 and out["parse_failed"] == 0
    assert out["non_dcm_extension"] == 3  # extensionless dicom, json, txt
    assert out["secondary_capture"] == 7  # the synthetic builder uses that SOP class
    assert out["empty_study_date"] == 1
    assert out["modalities"]["RF"] == 6 and out["modalities"]["OT"] == 1
    assert out["identifying_tags"]["PatientName"] == 7
    assert out["identifying_tags"]["StudyDate"] == 6
    assert out["identifying_tags"]["PatientBirthDate"] == 0


def test_devices_are_digests_with_profile_flag(poison_folder: Path):
    out = probes.devices(poison_folder, profiles_dir=str(REPO / "data" / "device_profiles"))
    _assert_clean(json.dumps(out))
    assert len(out["devices"]) == 2  # ACME FLUORO 9000 and the no-manufacturer SC file
    for row in out["devices"]:
        assert set(row) == {"device_digest", "profiled", "frames_sampled"}
        assert row["profiled"] is False
    assert sum(r["frames_sampled"] for r in out["devices"]) == 7


def test_devices_profiled_flag_turns_true_with_a_matching_profile(poison_folder: Path, tmp_path: Path):
    import pydicom
    from src.services.pixel_deid import profiles as prof

    ds = pydicom.dcmread(str(sorted(poison_folder.glob("*.dcm"))[0]), stop_before_pixels=True)
    key = prof.device_id_for(ds)
    pdir = tmp_path / "profiles"
    pdir.mkdir()
    (pdir / "acme.json").write_text(json.dumps({"device_id": key, "boxes": [[6, 4, 70, 14]], "tolerance": 8}))
    out = probes.devices(poison_folder, profiles_dir=str(pdir))
    flags = {r["device_digest"]: r["profiled"] for r in out["devices"]}
    assert flags[gate.digest(key)] is True


def test_detectors_report_booleans_only():
    out = probes.detectors(model_dir=str(REPO / "models" / "craft"))
    assert set(out) == {"tesseract", "onnx_model"}
    assert all(isinstance(v, bool) for v in out.values())


def test_verdicts_use_enum_codes(poison_folder: Path):
    out = probes.verdicts(poison_folder, profiles_dir=str(REPO / "data" / "device_profiles"))
    _assert_clean(json.dumps(out))
    assert out["verdicts"], "one row per device expected"
    for row in out["verdicts"]:
        assert row["verdict"] in ("confirmed", "redact", "abort", "quarantine", "other")
        assert row["reason"] in gate.ENUMS
        assert isinstance(row["tiers_fired"], int) and isinstance(row["regions"], int)


def test_banner_finds_the_fixed_region_from_moving_frames(poison_folder: Path):
    out = probes.banner(poison_folder)
    _assert_clean(json.dumps(out))
    rows = [r for r in out["banner"] if r["frames_used"] >= 3]
    assert len(rows) == 1
    row = rows[0]
    assert (row["rows"], row["cols"]) == (96, 160)
    assert row["boxes"], "banner box expected"
    x0, y0, x1, y1 = row["boxes"][0]
    # Dilation widens the box; it must contain the true banner and stay near it.
    assert x0 <= 6 and y0 <= 4 and x1 >= 70 and y1 >= 14
    assert x0 >= 0 and y0 >= 0 and x1 <= 160 and y1 <= 96
    assert (x1 - x0) <= 64 + 40 and (y1 - y0) <= 10 + 24


def test_banner_with_one_frame_falls_back_to_the_border_rule(tmp_path: Path):
    ds = make_phi_dicom_dataset(rows=100, cols=100, seed=1)
    ds.Manufacturer, ds.ManufacturerModelName = "ACME", "SOLO"
    frame = np.full((100, 100), 200, dtype=np.uint16)
    frame[2:10, 2:60] = 4000  # banner in the top border
    ds.PixelData = frame.tobytes()
    _write(ds, tmp_path / "one.dcm")
    out = probes.banner(tmp_path)
    assert out["banner"][0]["frames_used"] == 1
    x0, y0, x1, y1 = out["banner"][0]["boxes"][0]
    assert x0 <= 2 and y0 <= 2 and x1 >= 60 and y1 >= 10


def test_banner_is_deterministic(poison_folder: Path):
    assert probes.banner(poison_folder) == probes.banner(poison_folder)


def test_intake_rule_counts(poison_folder: Path):
    out = probes.intake(poison_folder)["intake"]
    assert out == {"files": 9, "accepted": 6, "rejected_extension": 3}


def test_run_all_report_is_gated_and_poison_free(poison_folder: Path):
    text = probes.run_all(poison_folder, profiles_dir=str(REPO / "data" / "device_profiles")).render()
    _assert_clean(text)
    data = json.loads(text)
    assert data["schema"] == "phi-safe-report/1"
    assert set(data["probes"]) >= {"files", "devices", "tesseract", "verdicts", "banner", "intake"}
    check(data)  # the rendered document itself re-validates


def test_silenced_swallows_library_output(capsys):
    with probes.silenced():
        print("DOE^JOHN")
        sys.stderr.write("MRN-0001234\n")
        import warnings

        warnings.warn("super-secret-phi")
    captured = capsys.readouterr()
    _assert_clean(captured.out + captured.err)


# -------------------------------------------------------------------- CLI ---

def _run_cli(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(REPO / "scripts" / "phi_safe_report.py"), *args],
        capture_output=True, text=True, cwd=str(REPO), timeout=300,
    )


def test_cli_prints_only_the_gated_report(poison_folder: Path):
    proc = _run_cli(str(poison_folder))
    assert proc.returncode == 0, proc.stdout[-200:]
    _assert_clean(proc.stdout + proc.stderr)
    assert proc.stderr == ""
    data = json.loads(proc.stdout)
    assert data["probes"]["files"] == 9
    assert str(poison_folder) not in proc.stdout


def test_cli_missing_folder_is_blocked_without_a_message(tmp_path: Path):
    proc = _run_cli(str(tmp_path / "DOE^JOHN_missing"))
    assert proc.returncode == 2
    _assert_clean(proc.stdout + proc.stderr)
    assert "DOE" not in proc.stdout and "missing" not in proc.stdout
    assert json.loads(proc.stdout) == {"blocked": True, "error_class": "NotADirectoryError", "schema": "phi-safe-report/1"}


def test_cli_bad_arguments_leak_nothing():
    proc = _run_cli("--bogus-flag", "DOE^JOHN")
    assert proc.returncode == 2
    _assert_clean(proc.stdout + proc.stderr)
    assert "bogus" not in proc.stdout + proc.stderr
