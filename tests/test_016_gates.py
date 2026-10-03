"""Spec 016 US2: the declaration check, validation gate and PHI gate."""
import json

import numpy as np
import pytest

from src.services.analysis_intake import (
    IntakeRefusal, check_declared_outputs, fill_provenance, load_descriptor, load_types, phi_gate, run_intake,
    validate_outputs,
)
from src.services.analysis_intake.gates import pixel_kind
from tests.fakes.analysis_folders import (
    DIRTY_BUILDERS, EXPERIMENT, PROJECT, SUBJECT, SYNTHETIC_NAME, USERNAME, FakeConfigTables,
    clean_angles_folder, consensus_folder, dirty_copied_dicom, dirty_copied_png, dirty_extra_file,
    dirty_missing_output, dirty_notes_name, dirty_short_csv, fake_name_classifier, write_angles,
)
from tests.fakes.fake_xnat import FakeXNAT
from tests.synthetic_data import make_synthetic_dicom

KNEE = load_types()["knee_flexion_angle"]
CONSENSUS = load_types()["segmentation_consensus"]


def _filled(case):
    desc, _ = fill_provenance(load_descriptor(case.output), manifest=case.manifest, username=USERNAME)
    return desc


def _validate(case):
    files = check_declared_outputs(KNEE, case.output)
    validate_outputs(KNEE, _filled(case), case.output, files)


def test_missing_output_refused(tmp_path):
    with pytest.raises(IntakeRefusal) as exc:
        check_declared_outputs(KNEE, dirty_missing_output(tmp_path).output)
    assert "angles.csv" in exc.value.friendly.message


def test_extra_file_refused(tmp_path):
    with pytest.raises(IntakeRefusal) as exc:
        check_declared_outputs(KNEE, dirty_extra_file(tmp_path).output)
    assert "scratch.txt" in exc.value.friendly.message


def test_companion_files_ignored(tmp_path):
    case = clean_angles_folder(tmp_path)
    (case.output / "download_manifest.json").write_text("{}")
    assert check_declared_outputs(KNEE, case.output) == ["angles.csv"]


def test_schema_failure_names_first_bad_row(tmp_path):
    case = clean_angles_folder(tmp_path)
    write_angles(case.output, [e["image_identity_hash"] for e in case.entries()], angles=[10.0, 400.0, 20.0])
    with pytest.raises(IntakeRefusal) as exc:
        _validate(case)
    msg = exc.value.friendly.message
    assert "row 2" in msg and "angle_degrees" in msg


def test_extra_column_refused(tmp_path):
    case = clean_angles_folder(tmp_path)
    text = (case.output / "angles.csv").read_text().splitlines()
    rows = [text[0] + ",comment"] + [line + ",x" for line in text[1:]]
    (case.output / "angles.csv").write_text("\n".join(rows) + "\n")
    with pytest.raises(IntakeRefusal) as exc:
        _validate(case)
    assert "comment" in exc.value.friendly.message


def test_row_count_must_match_inputs(tmp_path):
    with pytest.raises(IntakeRefusal) as exc:
        _validate(dirty_short_csv(tmp_path))
    assert "2 rows" in exc.value.friendly.message and "3 input frames" in exc.value.friendly.message


def test_identity_hash_must_be_an_input(tmp_path):
    case = clean_angles_folder(tmp_path)
    ids = [e["image_identity_hash"] for e in case.entries()]
    ids[1] = "f" * 64
    write_angles(case.output, ids)
    with pytest.raises(IntakeRefusal) as exc:
        _validate(case)
    assert "row 2" in exc.value.friendly.message


def test_clean_folder_validates(tmp_path):
    _validate(clean_angles_folder(tmp_path))


def test_renamed_dicom_and_png_detected(tmp_path):
    assert pixel_kind(dirty_copied_dicom(tmp_path / "a").output / "angles.csv") == "dicom"
    assert pixel_kind(dirty_copied_png(tmp_path / "b").output / "angles.csv") == "png"
    assert pixel_kind(clean_angles_folder(tmp_path / "c").output / "angles.csv") is None


@pytest.mark.parametrize("builder", [dirty_copied_dicom, dirty_copied_png])
def test_no_pixels_refuses_images_and_never_asks_confirmer(tmp_path, builder):
    case = builder(tmp_path)
    asked = []
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(KNEE, {"run": {"source_hashes": []}}, case.output, ["angles.csv"],
                 classifier=fake_name_classifier, confirmer=lambda c: asked.append(c))
    assert "image" in exc.value.friendly.message.lower()
    assert asked == []


def test_notes_name_refused_without_echo(tmp_path):
    case = dirty_notes_name(tmp_path)
    desc = _filled(case)
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(KNEE, desc, case.output, ["angles.csv"], classifier=fake_name_classifier)
    msg = exc.value.friendly.message
    assert "analysis.json" in msg and "notes line 2" in msg and "PERSON" in msg
    assert SYNTHETIC_NAME not in msg and "Synthetic" not in msg


def test_name_in_csv_cell_refused(tmp_path):
    case = clean_angles_folder(tmp_path)
    with open(case.output / "angles.csv", "a") as fh:
        fh.write(f"9,{SYNTHETIC_NAME},10\n")
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(KNEE, _filled(case), case.output, ["angles.csv"], classifier=fake_name_classifier)
    assert "angles.csv" in exc.value.friendly.message and "line 5" in exc.value.friendly.message


def test_classifier_error_fails_closed(tmp_path):
    case = clean_angles_folder(tmp_path)

    def broken(text):
        raise ImportError("presidio missing")
    with pytest.raises(IntakeRefusal):
        phi_gate(KNEE, _filled(case), case.output, ["angles.csv"], classifier=broken)


def test_missing_default_classifier_fails_closed(tmp_path, monkeypatch):
    from src.services.analysis_intake import gates
    case = clean_angles_folder(tmp_path)

    def unavailable():
        raise gates.ClassifierUnavailable("ImportError")
    monkeypatch.setattr(gates, "default_classifier", unavailable)
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(KNEE, _filled(case), case.output, ["angles.csv"])
    assert exc.value.friendly.recourse


def test_machine_tokens_not_sent_to_classifier(tmp_path):
    case = clean_angles_folder(tmp_path)
    seen = []

    def recording(text):
        seen.append(text)
        return False, []
    phi_gate(KNEE, _filled(case), case.output, ["angles.csv"], classifier=recording)
    assert all(not t.replace(".", "").isdigit() for t in seen)
    assert not any(len(t) == 64 for t in seen)


def test_mask_accepted_with_confirmation(tmp_path):
    case = consensus_folder(tmp_path)
    assert phi_gate(CONSENSUS, _filled(case), case.output, ["mask.npz"],
                    confirmer=lambda c: ("confirmed", [])) == "confirmed"


def test_float_npz_is_not_a_mask(tmp_path):
    case = consensus_folder(tmp_path, mask_dtype=np.float32)
    with pytest.raises(IntakeRefusal):
        phi_gate(CONSENSUS, _filled(case), case.output, ["mask.npz"], confirmer=lambda c: ("confirmed", []))


def test_unknown_image_refused_under_source_only(tmp_path):
    case = consensus_folder(tmp_path)
    make_synthetic_dicom(case.output / "other.dcm", seed=77)
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(CONSENSUS, _filled(case), case.output, ["mask.npz", "other.dcm"],
                 confirmer=lambda c: ("confirmed", []))
    assert "other.dcm" in exc.value.friendly.message


@pytest.mark.parametrize("answer", ["abort", "redact", "quarantine", None])
def test_confirmer_refusals(tmp_path, answer):
    case = consensus_folder(tmp_path)
    with pytest.raises(IntakeRefusal):
        phi_gate(CONSENSUS, _filled(case), case.output, ["mask.npz"], confirmer=lambda c: (answer, []))


def test_confirmer_enum_value_accepted(tmp_path):
    from src.xnat_experiment_data import ReviewDecision
    case = consensus_folder(tmp_path)
    confirmed = next(d for d in ReviewDecision if getattr(d, "value", None) == "confirmed")
    assert phi_gate(CONSENSUS, _filled(case), case.output, ["mask.npz"],
                    confirmer=lambda c: (confirmed, [])) == "confirmed"


@pytest.mark.parametrize("name", sorted(DIRTY_BUILDERS))
def test_end_to_end_dirty_folders_write_nothing(tmp_path, name):
    case = DIRTY_BUILDERS[name](tmp_path)
    fx = FakeXNAT(project_name=PROJECT)
    fx.seed_rf_experiment(SUBJECT, EXPERIMENT)
    r = run_intake(case.output, gateway=fx, config_tables=FakeConfigTables(), username=USERNAME,
                   manifest=case.manifest, classifier=fake_name_classifier)
    assert r.status == "refused"
    assert r.friendly.recourse
    assert "Traceback" not in r.friendly.message
    assert not [c for c in fx.calls if c["op"] not in ("list_assessors",)]


def test_dry_run_makes_no_gateway_calls(tmp_path):
    case = clean_angles_folder(tmp_path)
    r = run_intake(case.output, gateway=None, username=USERNAME, manifest=case.manifest,
                   classifier=fake_name_classifier, dry_run=True)
    assert r.status == "dry_run_ok"
    assert "label" not in json.loads((case.output / "analysis.json").read_text())["run"]
