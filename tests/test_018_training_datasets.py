"""
Spec 018: training datasets (multi-case results published on the project).

Everything here is offline and synthetic: synthetic DICOM frames, download
records shaped like the spec 015 manifest, ``FakeXNAT`` and a stub classifier
that flags one made-up name.  No server, no network, no real patient data.
"""
import json
import shutil
from pathlib import Path

import pytest

from app.logic.download_manifest import build_file_entry, sha256_of_file
from src.services.analysis_intake import (
    IntakeRefusal, PublishOutcome, assemble_dataset, find_derived, load_types, publish_analysis,
    record_in_catalog, run_intake,
)
from src.services.analysis_intake.catalog import ANALYSES_TABLE, CATALOG_COLUMNS
from src.services.analysis_intake.cli import assemble_dataset_main
from src.services.analysis_intake.descriptor import check_descriptor
from src.services.analysis_intake.gates import check_declared_outputs, phi_gate
from src.services.analysis_intake.provenance import fill_provenance
from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR, check_type_data
from tests.fakes.analysis_folders import (
    EXPERIMENT, PROJECT as KNEE_PROJECT, SUBJECT, FakeConfigTables, clean_angles_folder,
)
from tests.fakes.fake_xnat import FakeXNAT
from tests.synthetic_data import make_synthetic_dicom

PROJECT = "SYNTH"
PROJECT_QS = f"/project/{PROJECT}"
USERNAME = "student_a"
FLAGGED_NAME = "Jane Q Synthetic"
CASES = {"KNEE_2025": ("S1", "E1", 1000), "HIP_2024": ("S2", "E2", 1100)}  # seeds apart from the 016 fixture frames
WRITE_OPS = {"file.put", "file.insert", "file.delete", "resource.put_zip", "assessor.create", "assessor.file.put"}


# --- fixtures -----------------------------------------------------------------

def stub_classifier(text):
    """Flags the synthetic name only, like the real PHI text classifier would."""
    return (True, ["PERSON"]) if FLAGGED_NAME.lower() in str(text).lower() else (False, [])


def _scan_qs(project, subject, experiment):
    # REST plural form, as the spec 015 download record writes it.
    return f"/projects/{project}/subjects/{subject}/experiments/{experiment}/scans/1"


def write_record(root: Path, case_uid: str, *, project: str = PROJECT, complete: bool = True, n_frames: int = 6,
                 seed_base: int = None, run_id: str = None) -> Path:
    """Write six synthetic frames and a spec 015 download record for one case; return the record path."""
    subject, experiment, base = CASES.get(case_uid, ("S9", "E9", 500))
    base = base if seed_base is None else seed_base
    folder = root / case_uid
    folder.mkdir(parents=True, exist_ok=True)
    frames = [make_synthetic_dicom(folder / f"{i:04d}-{case_uid}.dcm", seed=base + i + 1) for i in range(n_frames)]
    entries = [build_file_entry(p, root, _scan_qs(project, subject, experiment), "DICOM", p.name) for p in frames]
    for p, e in zip(frames, entries):
        assert e["sha256"] == sha256_of_file(p)
    record = {
        "manifest_version": "1.0", "run_id": run_id or (case_uid.lower().ljust(32, "0")[:32].replace("_", "a")),
        "finished_at": "2026-10-03T00:00:00.000000Z", "path": "folder", "scope": None, "project": project,
        "username": None, "server_url": None, "complete": complete,
        "selection": [{"subject": subject, "experiment": experiment, "scan": "1"}],
        "files": entries, "empty_scans": [],
        "server_copy": {"status": "not_attempted", "resource_label": None, "filename": None, "reason": None},
    }
    path = folder / "download_manifest.json"
    path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return path


@pytest.fixture
def records(tmp_path):
    root = tmp_path / "downloads"
    return {
        "knee": write_record(root, "KNEE_2025"),
        "hip": write_record(root, "HIP_2024"),
        "incomplete": write_record(root / "inc", "KNEE_2025", complete=False),
        "other": write_record(root / "oth", "HIP_2024", project="OTHER", seed_base=1300),
    }


@pytest.fixture
def fx():
    return FakeXNAT(project_name=PROJECT)


@pytest.fixture
def ct():
    tables = FakeConfigTables()
    tables.add_new_table(ANALYSES_TABLE, list(CATALOG_COLUMNS), verbose=False)
    return tables


def build(tmp_path, records, name="knee_hip_set", **kw):
    out = tmp_path / name
    summary = assemble_dataset(name, [records["knee"], records["hip"]], out, **kw)
    return out, summary


def publish(folder, fx, ct, classifier=stub_classifier):
    return run_intake(folder, gateway=fx, config_tables=ct, username=USERNAME, classifier=classifier)


def writes(fx):
    return [c for c in fx.calls if c["op"] in WRITE_OPS]


def stored_bytes(fx):
    flat = getattr(fx, "_flat_resources", {})
    return {key: sorted(res._staged_files) for key, res in flat.items()}


# --- Phase 2: type and descriptor (T009) --------------------------------------

def test_every_type_file_loads():
    types = load_types()
    assert {"knee_flexion_angle", "training_dataset"} <= set(types)
    ds = types["training_dataset"]
    assert ds.multi_case and ds.placement == "project_resource" and ds.resource_label == "TRAINING_DATASETS"
    assert not types["knee_flexion_angle"].multi_case


def test_meta_schema_accepts_training_dataset():
    jsonschema = pytest.importorskip("jsonschema")
    meta = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "analysis-type.schema.json").read_text())
    jsonschema.validate(json.loads((ANALYSIS_TYPES_DIR / "training_dataset.json").read_text()), meta)


def test_multi_case_with_assessor_placement_refused():
    data = json.loads((ANALYSIS_TYPES_DIR / "training_dataset.json").read_text())
    data["placement"] = "assessor"
    with pytest.raises(IntakeRefusal) as exc:
        check_type_data(data, Path("training_dataset.json"))
    assert "multi_case" in exc.value.friendly.message


def test_run_cases_on_single_case_type_refused():
    desc = {"descriptor_version": "1", "type_name": "knee_flexion_angle", "type_version": 1,
            "run": {"case_uid": "KNEE_2025", "code_ref": "git:0", "cases": ["KNEE_2025", "HIP_2024"]}}
    with pytest.raises(IntakeRefusal) as exc:
        check_descriptor(desc, load_types())
    assert exc.value.friendly.recourse


# --- US1: assemble (T010) -----------------------------------------------------

def test_assemble_rows_dedup_sorted_and_split(tmp_path, records):
    out, summary = build(tmp_path, records)
    rows = json.loads((out / "dataset_manifest.json").read_text())["rows"]
    assert len(rows) == 12 == summary.rows
    assert [(r["case_uid"], r["relative_path"]) for r in rows] == sorted((r["case_uid"], r["relative_path"]) for r in rows)
    assert len({r["sha256"] for r in rows}) == 12
    splits = json.loads((out / "splits.json").read_text())
    assert splits == {"rule": "every_nth:3:validation", "counts": {"training": 8, "validation": 4, "test": 0}}
    assert [r["split"] for r in rows[:3]] == ["training", "training", "validation"]
    # The same record twice adds no row.
    again = assemble_dataset("dup_set", [records["knee"], records["knee"]], tmp_path / "dup")
    assert again.rows == 6


def test_assemble_merged_record_and_descriptor(tmp_path, records):
    out, _ = build(tmp_path, records)
    merged = json.loads((out / "download_manifest.json").read_text())
    assert merged["complete"] is True and merged["run_id"] is None and len(merged["merged_from"]) == 2
    assert len(merged["files"]) == 12
    desc = json.loads((out / "analysis.json").read_text())
    assert desc["type_name"] == "training_dataset"
    assert desc["run"]["case_uid"] == "knee_hip_set" and desc["run"]["cases"] == ["HIP_2024", "KNEE_2025"]
    assert desc["run"]["parameters"]["source_manifest_run_ids"] == merged["merged_from"]


@pytest.mark.parametrize("problem", ["incomplete", "missing_hash", "two_projects", "bad_name", "existing"])
def test_assemble_refusals_write_nothing(tmp_path, records, problem):
    out = tmp_path / "out"
    manifests = [records["knee"], records["hip"]]
    name = "knee_hip_set"
    if problem == "incomplete":
        manifests = [records["incomplete"], records["hip"]]
    elif problem == "missing_hash":
        data = json.loads(records["knee"].read_text())
        data["files"][0]["sha256"] = None
        records["knee"].write_text(json.dumps(data))
    elif problem == "two_projects":
        manifests = [records["knee"], records["other"]]
    elif problem == "bad_name":
        name = "9 bad name"
    elif problem == "existing":
        out.mkdir()
        (out / "analysis.json").write_text("{}")
    with pytest.raises(IntakeRefusal) as exc:
        assemble_dataset(name, manifests, out)
    assert exc.value.friendly.recourse
    if problem == "existing":
        assert sorted(p.name for p in out.iterdir()) == ["analysis.json"]
    else:
        assert not out.exists()


# --- US1: multi-case provenance (T012) ----------------------------------------

def _dataset_provenance(out):
    desc = json.loads((out / "analysis.json").read_text())
    return fill_provenance(desc, manifest=out / "download_manifest.json", username=USERNAME,
                           atype=load_types()["training_dataset"], folder=out)


def test_multi_case_provenance(tmp_path, records):
    out, _ = build(tmp_path, records)
    desc, _warnings = _dataset_provenance(out)
    run = desc["run"]
    assert len(run["source_hashes"]) == 12
    assert run["project_query_string"] == PROJECT_QS
    assert run["provenance"] == "recorded" and run["producer"] == USERNAME
    assert len({r.split("/experiments/")[1] for r in run["input_refs"]}) == 2   # two sessions, not refused


def test_multi_case_provenance_refuses_unknown_hash_and_case(tmp_path, records):
    out, _ = build(tmp_path, records)
    path = out / "dataset_manifest.json"
    original = path.read_text()
    data = json.loads(original)
    data["rows"][0]["sha256"] = "f" * 64
    path.write_text(json.dumps(data))
    with pytest.raises(IntakeRefusal) as exc:
        _dataset_provenance(out)
    assert "not in" in exc.value.friendly.message
    data = json.loads(original)
    data["rows"][0]["case_uid"] = "ELBOW_2023"
    path.write_text(json.dumps(data))
    with pytest.raises(IntakeRefusal) as exc:
        _dataset_provenance(out)
    assert "run.cases" in exc.value.friendly.message


def test_single_case_provenance_unchanged(tmp_path):
    case = clean_angles_folder(tmp_path)
    desc = json.loads((case.output / "analysis.json").read_text())
    old, _ = fill_provenance(desc, manifest=case.manifest, username=USERNAME)
    new, _ = fill_provenance(desc, manifest=case.manifest, username=USERNAME,
                             atype=load_types()["knee_flexion_angle"], folder=case.output)
    for key in ("source_hashes", "input_refs", "experiment_query_string", "manifest_run_id", "provenance"):
        assert old["run"][key] == new["run"][key]
    assert "project_query_string" not in new["run"]


# --- US1: project_resource publish (T014) -------------------------------------

def _checked(out):
    atype = load_types()["training_dataset"]
    desc, _ = _dataset_provenance(out)
    return desc, atype, check_declared_outputs(atype, out)


def test_project_resource_publish_writes_three_files(tmp_path, records, fx):
    out, _ = build(tmp_path, records)
    desc, atype, files = _checked(out)
    outcome = publish_analysis(desc, out, fx, atype=atype, files=files)
    assert outcome.label == "knee_hip_set__v1" and outcome.placement_used == "project_resource"
    assert outcome.query_string == PROJECT_QS and outcome.resource_label == "knee_hip_set__v1"
    assert outcome.verified, outcome.problems
    assert sorted(fx.list_files(PROJECT_QS, "knee_hip_set__v1")) == ["analysis.json", "dataset_manifest.json", "splits.json"]
    puts = [c for c in writes(fx)]
    assert puts and all(c["op"] == "file.put" for c in puts)


def test_tampered_download_is_not_verified_and_not_cataloged(tmp_path, records, fx, ct, monkeypatch):
    out, _ = build(tmp_path, records)
    real = fx.download_resource

    def tampered(qs, label, dest):
        paths = real(qs, label, dest)
        for p in paths:
            if Path(p).name == "splits.json":
                Path(p).write_text("{}")
        return paths

    monkeypatch.setattr(fx, "download_resource", tampered)
    result = publish(out, fx, ct)
    assert result.status == "not_verified"
    assert ct.tables[ANALYSES_TABLE].rows == []


# --- US1: catalog (T016, T017b) -----------------------------------------------

def _knee_pair(case_uid, label="knee_flexion_angle__v1"):
    desc = {"type_name": "knee_flexion_angle", "type_version": 1,
            "run": {"case_uid": case_uid, "producer": USERNAME, "source_hashes": [{}], "supersedes": None}}
    return PublishOutcome(label, "assessor", f"/project/P/subject/S/experiment/{case_uid}/assessor/{label}",
                          "KNEE_FLEXION_ANGLE"), desc


def test_catalog_row_has_address_columns():
    tables = FakeConfigTables()
    outcome, desc = _knee_pair("CASEA")
    record_in_catalog(outcome, desc, tables)
    row = tables.server_rows[ANALYSES_TABLE][0]
    assert row["NAME"] == "CASEA:KNEE_FLEXION_ANGLE__V1"
    assert row["QUERY_STRING"] == outcome.query_string and row["RESOURCE_LABEL"] == "KNEE_FLEXION_ANGLE"


def test_old_table_gains_missing_columns():
    tables = FakeConfigTables()
    old_columns = [c for c in CATALOG_COLUMNS if c not in ("QUERY_STRING", "RESOURCE_LABEL")]
    tables.add_new_table(ANALYSES_TABLE, old_columns, verbose=False)
    outcome, desc = _knee_pair("CASEA")
    record_in_catalog(outcome, desc, tables)
    assert {"QUERY_STRING", "RESOURCE_LABEL"} <= set(tables.tables[ANALYSES_TABLE].columns)
    assert tables.server_rows[ANALYSES_TABLE][0]["QUERY_STRING"] == outcome.query_string


def test_same_label_on_two_cases_gives_two_rows():
    """FR-020: the 016 catalog dropped the second case's row (research.md C2)."""
    tables = FakeConfigTables()
    for case_uid in ("CASEA", "CASEB"):
        outcome, desc = _knee_pair(case_uid)
        assert record_in_catalog(outcome, desc, tables) is True
    names = [r["NAME"] for r in tables.server_rows[ANALYSES_TABLE]]
    assert names == ["CASEA:KNEE_FLEXION_ANGLE__V1", "CASEB:KNEE_FLEXION_ANGLE__V1"]


@pytest.mark.parametrize("placement", ["project_resource", "assessor", "scan_resource"])
def test_retry_path_fills_address(placement):
    from src.services.analysis_intake import _outcome_from_descriptor
    run = {"case_uid": "C", "label": "x__v1", "placement_used": placement, "project_query_string": PROJECT_QS,
           "experiment_query_string": "/project/P/subject/S/experiment/E",
           "input_refs": ["/projects/P/subjects/S/experiments/E/scans/1"]}
    atype = load_types()["knee_flexion_angle"]
    outcome = _outcome_from_descriptor({"run": run}, atype)
    expected = {
        "project_resource": (PROJECT_QS, "x__v1"),
        "assessor": ("/project/P/subject/S/experiment/E/assessor/x__v1", "KNEE_FLEXION_ANGLE"),
        "scan_resource": ("/project/P/subject/S/experiment/E/scan/1", "x__v1"),
    }[placement]
    assert (outcome.query_string, outcome.resource_label) == expected


# --- US1: end to end and CLI (T019) -------------------------------------------

def test_end_to_end_assemble_then_publish(tmp_path, records, fx, ct):
    out, summary = build(tmp_path, records)
    result = publish(out, fx, ct)
    assert result.status == "done", result.friendly
    assert result.label == "knee_hip_set__v1"
    paths = fx.download_resource(PROJECT_QS, "knee_hip_set__v1", tmp_path / "check")
    server_desc = json.loads(next(Path(p) for p in paths if Path(p).name == "analysis.json").read_text())
    assert len(server_desc["run"]["source_hashes"]) == summary.rows == 12
    assert server_desc["run"]["placement_used"] == "project_resource"
    row = ct.tables[ANALYSES_TABLE].rows[0]
    assert row["SOURCE_HASH_COUNT"] == "12" and row["NAME"] == "KNEE_HIP_SET:KNEE_HIP_SET__V1"
    assert row["QUERY_STRING"] == PROJECT_QS and row["PLACEMENT_USED"] == "project_resource"


def test_cli_assemble_success_and_refusal(tmp_path, records):
    lines = []
    code = assemble_dataset_main([str(tmp_path / "ds"), "--name", "knee_hip_set", "--manifest", str(records["knee"]),
                                  "--manifest", str(records["hip"])], out=lines.append)
    assert code == 0 and "12 frames" in lines[0] and "publish-analysis" in lines[1]
    lines.clear()
    code = assemble_dataset_main([str(tmp_path / "ds2"), "--name", "knee_hip_set", "--manifest",
                                  str(records["incomplete"])], out=lines.append)
    text = "\n".join(lines)
    assert code == 1 and "Traceback" not in text and "did not finish" in text
    lines.clear()
    assert assemble_dataset_main(["--password", "x"], out=lines.append) == 1
    assert "Traceback" not in "\n".join(lines)


def test_main_dispatches_assemble_dataset():
    source = (Path(__file__).resolve().parents[1] / "main.py").read_text()
    assert "sys.argv[1] == 'assemble-dataset'" in source


# --- US2: refusals (T020) -----------------------------------------------------

def test_extra_png_is_refused(tmp_path, records, fx, ct):
    out, _ = build(tmp_path, records)
    (out / "preview.png").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    result = publish(out, fx, ct)
    assert result.status == "refused" and "preview.png" in result.friendly.message
    assert writes(fx) == []


def test_png_bytes_as_splits_json_refused(tmp_path, records, fx, ct):
    out, _ = build(tmp_path, records)
    (out / "splits.json").write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 64)
    result = publish(out, fx, ct)
    assert result.status == "refused" and writes(fx) == []
    # The no-images check on its own also catches it (the validation step runs first in run_intake).
    desc, atype, files = _checked(out)
    with pytest.raises(IntakeRefusal) as exc:
        phi_gate(atype, desc, out, files, classifier=stub_classifier)
    assert "image" in exc.value.friendly.title.lower()


def test_flagged_label_source_refused_without_echo(tmp_path, records, fx, ct):
    out, _ = build(tmp_path, records, label_source=f"drawn by {FLAGGED_NAME}")
    result = publish(out, fx, ct)
    assert result.status == "refused"
    assert FLAGGED_NAME not in result.friendly.message and FLAGGED_NAME not in result.friendly.title
    assert writes(fx) == []


# --- US3: find_derived (T022) -------------------------------------------------

def _three_items(tmp_path, records, fx, ct):
    """A dataset, a second dataset and a knee_flexion_angle result, all cataloged."""
    out, _ = build(tmp_path, records)
    assert publish(out, fx, ct).status == "done"
    out2 = tmp_path / "hip_only"
    assemble_dataset("hip_only", [records["hip"]], out2)
    assert publish(out2, fx, ct).status == "done"
    case = clean_angles_folder(tmp_path / "knee")
    fx.project_name = KNEE_PROJECT
    fx.seed_rf_experiment(SUBJECT, EXPERIMENT)
    result = run_intake(case.output, gateway=fx, config_tables=ct, username=USERNAME, manifest=case.manifest,
                        classifier=stub_classifier)
    assert result.status == "done", (result.outcome.problems if result.outcome else result.friendly)
    return case


def test_find_derived_matches_dataset_and_result(tmp_path, records, fx, ct):
    case = _three_items(tmp_path, records, fx, ct)
    knee_entry = case.entries()[0]
    # Put the knee frame into a third dataset too, so one hash is shared by two types.
    shared = assemble_dataset("shared_set", [case.manifest], tmp_path / "shared")
    assert publish(tmp_path / "shared", fx, ct).status == "done" and shared.rows == 3
    before, n_writes = stored_bytes(fx), len(writes(fx))
    report = find_derived(knee_entry["sha256"], gateway=fx, config_tables=ct)
    assert report.checked == 4 and report.unchecked == [] and len(report.items) == 2
    found = {(i.type_name, i.label, i.placement_used) for i in report.items}
    assert ("shared_set" in {i.label.split("__")[0] for i in report.items})
    assert {t for t, _l, _p in found} == {"knee_flexion_angle", "training_dataset"}
    for item in report.items:
        assert item.query_string and item.resource_label and item.matched_on == "sha256"
    by_identity = find_derived(knee_entry["image_identity_hash"], gateway=fx, config_tables=ct)
    assert len(by_identity.items) == 2 and {i.matched_on for i in by_identity.items} == {"image_identity_hash"}
    assert stored_bytes(fx) == before and len(writes(fx)) == n_writes


def test_find_derived_unused_hash_and_unchecked_rows(tmp_path, records, fx, ct, monkeypatch):
    out, _ = build(tmp_path, records)
    assert publish(out, fx, ct).status == "done"
    report = find_derived("0" * 64, gateway=fx, config_tables=ct)
    assert report.items == [] and report.checked == 1
    # An old row named by label alone, with no address, cannot be checked (FR-020).
    ct.add_new_item(ANALYSES_TABLE, "knee_flexion_angle__v1",
                    extra_columns_values={"TYPE_NAME": "knee_flexion_angle"}, verbose=False)
    real = fx.download_resource
    monkeypatch.setattr(fx, "download_resource",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("gone")))
    report = find_derived("0" * 64, gateway=fx, config_tables=ct)
    reasons = dict(report.unchecked)
    assert report.checked == 0 and len(reasons) == 2
    assert "KNEE_FLEXION_ANGLE__V1" in reasons and "KNEE_HIP_SET:KNEE_HIP_SET__V1" in reasons
    monkeypatch.setattr(fx, "download_resource", real)


def test_find_derived_refuses_bad_hash(ct, fx):
    with pytest.raises(IntakeRefusal):
        find_derived("not-a-hash", gateway=fx, config_tables=ct)


def test_find_derived_source_has_no_write_calls():
    source = (Path(__file__).resolve().parents[1] / "src" / "services" / "analysis_intake" / "derived.py").read_text()
    for forbidden in ("put_file", "insert_file", "create_assessor", "delete_file", ".delete(", "push_to_xnat"):
        assert forbidden not in source


# --- US4: versions (T024) -----------------------------------------------------

def test_second_version_keeps_first(tmp_path, records, fx, ct):
    out, _ = build(tmp_path, records)
    assert publish(out, fx, ct).status == "done"
    v1 = sorted(fx._flat_resources[(PROJECT_QS, "knee_hip_set__v1")]._staged_files)
    out2 = tmp_path / "again"
    assemble_dataset("knee_hip_set", [records["knee"], records["hip"]], out2)
    desc = json.loads((out2 / "analysis.json").read_text())
    desc["run"]["supersedes"] = "knee_hip_set__v1"
    (out2 / "analysis.json").write_text(json.dumps(desc))
    result = publish(out2, fx, ct)
    assert result.status == "done" and result.label == "knee_hip_set__v2"
    assert sorted(fx._flat_resources[(PROJECT_QS, "knee_hip_set__v1")]._staged_files) == v1
    rows = {r["NAME"]: r for r in ct.tables[ANALYSES_TABLE].rows}
    assert rows["KNEE_HIP_SET:KNEE_HIP_SET__V2"]["SUPERSEDES"] == "knee_hip_set__v1"


def test_missing_supersedes_refused_before_upload(tmp_path, records, fx, ct):
    out, _ = build(tmp_path, records)
    desc = json.loads((out / "analysis.json").read_text())
    desc["run"]["supersedes"] = "knee_hip_set__v7"
    (out / "analysis.json").write_text(json.dumps(desc))
    result = publish(out, fx, ct)
    assert result.status == "refused" and "knee_hip_set__v7" in result.friendly.message
    assert writes(fx) == []
