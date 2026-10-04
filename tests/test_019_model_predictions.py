"""
Spec 019: a trained model's predictions stored as one more annotator.

Everything here is offline and synthetic: synthetic DICOM frames, a download
record shaped like the spec 015 manifest, synthetic masks, ``FakeXNAT`` and a
stub classifier that flags one made-up name.  No server, no network, no real
patient data.

Test names carry the user story (us1 to us4) so each story can be run alone
with ``-k us1`` and so on.
"""
import inspect
import json
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pytest

from app.logic.download_manifest import build_file_entry, sha256_of_file
from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation, AnnotationSet
from src.annotations.validate import validate_annotator_id
from src.services.analysis_intake import IntakeRefusal, load_types, run_intake
from src.services.analysis_intake.catalog import ANALYSES_TABLE, CATALOG_COLUMNS
from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR, check_type_data
from tests.fakes.analysis_folders import FakeConfigTables
from tests.fakes.fake_xnat import FakeXNAT
from tests.synthetic_data import make_synthetic_dicom

PROJECT = "SYNTH"
PROJECT_QS = f"/project/{PROJECT}"
CASE = "KNEE_2025"
SUBJECT, EXPERIMENT = "S1", "E1"
SCAN_QS_REST = f"/projects/{PROJECT}/subjects/{SUBJECT}/experiments/{EXPERIMENT}/scans/1"
SCAN_QS = f"/project/{PROJECT}/subject/{SUBJECT}/experiment/{EXPERIMENT}/scan/1"
DATASET_LABEL = "knee_hip_set__v1"
DATASET_QS = f"{PROJECT_QS}/resources/{DATASET_LABEL}"
USERNAME = "student_a"
FLAGGED_NAME = "Jane Q Synthetic"
MODEL_NAME = "knee-seg"
WRITE_OPS = {"file.put", "file.insert", "file.delete", "resource.put_zip", "assessor.create", "assessor.file.put"}


# --- test doubles ---------------------------------------------------------------

def stub_classifier(text):
    """Flags the synthetic name only, like the real PHI text classifier would."""
    return (True, ["PERSON"]) if FLAGGED_NAME.lower() in str(text).lower() else (False, [])


class _ResourceHandle:
    """What pyxnat yields when a scan's resources are iterated: an object with ``label()``."""

    def __init__(self, label: str) -> None:
        self._label = label

    def label(self) -> str:
        return self._label


class _ListingSelectable:
    """Wraps a ``FakeSelectable`` and adds the read-only ``resources()`` listing of real pyxnat."""

    def __init__(self, fake: "ScanFakeXNAT", inner: Any, qs: str) -> None:
        self._fake, self._inner, self._qs = fake, inner, qs

    def resources(self):
        parsed = self._fake.select._parse_resource_qs(self._qs)
        if parsed is None:
            return []
        subj, exp, scan, _ = parsed
        return [_ResourceHandle(label) for (s, e, c, label), res in sorted(self._fake._resources.items())
                if (s, e, c) == (subj, exp, scan) and res.list_files()]

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _ListingSelector:
    def __init__(self, fake: "ScanFakeXNAT", inner: Any) -> None:
        self._fake, self._inner = fake, inner

    def __call__(self, qs: str):
        return _ListingSelectable(self._fake, self._inner(qs), qs)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class ScanFakeXNAT(FakeXNAT):
    """
    ``FakeXNAT`` with scan resources that behave like the real server for this feature.

    The shared fake keeps scan-resource bytes in one map keyed by file name, so two
    resources that both hold ``manifest.json`` would read back each other's bytes, and
    it does not list files written with ``put_file``.  This test-local subclass keeps
    each scan resource's files apart, lists them, refuses ``overwrite=False`` on an
    existing name, and lists a scan's resources the way pyxnat does.  ``tamper`` makes
    the next download of a named file return other bytes (a damaged upload).
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.select = _ListingSelector(self, self.select)
        self.tamper: Dict[str, bytes] = {}

    def put_file(self, querystring, resource_label, filename, ffn, *, content="", format="", tags="", overwrite=None):
        if self.select._parse_resource_qs(querystring) is None:
            return super().put_file(querystring, resource_label, filename, ffn, content=content,
                                    format=format, tags=tags, overwrite=overwrite)
        resource = self.select(querystring)._inner.resource(resource_label)
        if overwrite is False and filename in resource.list_files():
            raise FileExistsError(f"{filename} already exists in {resource_label}")
        data = Path(ffn).read_bytes()
        resource._staged_files = [(n, d) for n, d in resource._staged_files if n != filename]
        resource._staged_files.append((filename, data))
        self.calls.append({"op": "file.put", "args": (str(ffn),),
                           "kwargs": {"_qs": querystring, "_resource": resource_label, "_filename": filename,
                                      "overwrite": overwrite}})

    def get_file_copy(self, querystring, resource_label, filename, dest):
        result = super().get_file_copy(querystring, resource_label, filename, dest)
        if filename in self.tamper:
            Path(dest).write_bytes(self.tamper[filename])
        return result

    def scan_files(self, label: str) -> Dict[str, bytes]:
        parsed = self.select._parse_resource_qs(SCAN_QS)
        res = self._resources.get((parsed[0], parsed[1], parsed[2], label))
        return dict(res._staged_files) if res is not None else {}


# --- fixtures -------------------------------------------------------------------

def write_record(root: Path, n_frames: int = 3) -> Path:
    """Write synthetic frames and a spec 015 download record for the case; return the record path."""
    folder = root / CASE
    folder.mkdir(parents=True, exist_ok=True)
    frames = [make_synthetic_dicom(folder / f"{i:04d}-{CASE}.dcm", seed=1900 + i) for i in range(n_frames)]
    entries = [build_file_entry(p, root, SCAN_QS_REST, "DICOM", p.name) for p in frames]
    record = {
        "manifest_version": "1.0", "run_id": "kneea2025".ljust(32, "0"),
        "finished_at": "2026-10-03T00:00:00.000000Z", "path": "folder", "scope": None, "project": PROJECT,
        "username": None, "server_url": None, "complete": True,
        "selection": [{"subject": SUBJECT, "experiment": EXPERIMENT, "scan": "1"}],
        "files": entries, "empty_scans": [],
        "server_copy": {"status": "not_attempted", "resource_label": None, "filename": None, "reason": None},
    }
    path = folder / "download_manifest.json"
    path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    return path


def frame_hashes(record: Path) -> List[str]:
    return [e["image_identity_hash"] for e in json.loads(record.read_text())["files"]]


def mask(seed: int = 0, shape=(8, 8)) -> np.ndarray:
    arr = np.zeros(shape, dtype=np.uint8)
    arr[2 + seed % 2:6, 2:6 + seed % 2] = 1
    return arr


def rle_payload(arr: np.ndarray) -> Dict[str, Any]:
    """The run-length form of a mask in predictions.json: shape plus [value, length] runs."""
    flat = arr.ravel().tolist()
    runs: List[List[int]] = []
    for v in flat:
        if runs and runs[-1][0] == v:
            runs[-1][1] += 1
        else:
            runs.append([int(v), 1])
    return {"shape": list(arr.shape), "runs": runs}


def make_prediction_folder(root: Path, record: Path, *, version: str = "3", entries=None, model=True,
                           dataset_qs: str = DATASET_QS, notes: str = "", name: str = None) -> Path:
    """Write analysis.json (with run.model) and predictions.json into a new folder."""
    folder = root / (name or f"pred_v{version}")
    folder.mkdir(parents=True, exist_ok=True)
    hashes = frame_hashes(record)
    if entries is None:
        entries = [
            {"image_identity_hash": hashes[0], "annotation_type": "binary_segmentation", "payload": rle_payload(mask())},
            {"image_identity_hash": hashes[1], "annotation_type": "landmark", "payload": {"x": 3, "y": 4}},
        ]
    run = {"case_uid": CASE, "code_ref": "git:abc123", "parameters": {}, "notes": notes, "supersedes": None}
    if model:
        run["model"] = {"name": MODEL_NAME, "version": version, "dataset_query_string": dataset_qs}
    desc = {"descriptor_version": "1", "type_name": "model_predictions", "type_version": 1, "run": run}
    (folder / "analysis.json").write_text(json.dumps(desc, indent=2), encoding="utf-8")
    (folder / "predictions.json").write_text(json.dumps({"entries": entries}), encoding="utf-8")
    return folder


@pytest.fixture
def record(tmp_path):
    return write_record(tmp_path / "downloads")


@pytest.fixture
def fx(tmp_path):
    """A fake server holding the published training dataset the model was trained on."""
    fake = ScanFakeXNAT(project_name=PROJECT)
    ds = tmp_path / "dataset_analysis.json"
    ds.write_text(json.dumps({"type_name": "training_dataset", "run": {"label": DATASET_LABEL}}), encoding="utf-8")
    fake.put_file(PROJECT_QS, DATASET_LABEL, "analysis.json", str(ds), content="TRAINING_DATASETS",
                  format="JSON", overwrite=False)
    fake.reset_calls()
    return fake


@pytest.fixture
def ct():
    tables = FakeConfigTables()
    tables.add_new_table(ANALYSES_TABLE, list(CATALOG_COLUMNS), verbose=False)
    return tables


def publish(folder, fx, ct, record, classifier=stub_classifier):
    return run_intake(folder, gateway=fx, config_tables=ct, username=USERNAME, manifest=record,
                      classifier=classifier)


def writes(fx):
    return [c for c in fx.calls if c["op"] in WRITE_OPS]


def rows(ct) -> Dict[str, Dict[str, Any]]:
    """Catalog rows by name (the test double stores names in upper case, like the real tables)."""
    return {r["NAME"]: r for r in ct.tables[ANALYSES_TABLE].rows}


def row_name(version="3"):
    return f"{CASE}:{label(version)}".upper()


def aid(version="3"):
    return f"model__{MODEL_NAME}__v{version}"


def label(version="3"):
    return f"ANNOTATIONS_{aid(version)}"


# --- Phase 2: annotator name helper (T004) --------------------------------------

def test_model_annotator_id_round_trip():
    from src.annotations.model_identity import (
        MODEL_PREFIX, is_model_annotator, model_annotator_id, parse_model_annotator_id,
    )
    built = model_annotator_id("knee-seg", "3")
    assert built == "model__knee-seg__v3" and built.startswith(MODEL_PREFIX)
    assert parse_model_annotator_id(built) == ("knee-seg", "3")
    assert is_model_annotator(built)
    for name, version in [("a", "1"), ("Knee-Seg-2", "v-2-rc1"), ("x" * 64, "9" * 32)]:
        assert parse_model_annotator_id(model_annotator_id(name, version)) == (name, version)


@pytest.mark.parametrize("name,version", [("knee-seg", "1.2"), ("a_b", "1"), ("", "1"), ("knee seg", "1"),
                                          ("knee-seg", ""), ("x" * 65, "1"), ("knee:seg", "1")])
def test_model_annotator_id_refuses_bad_parts_with_hyphen_hint(name, version):
    from src.annotations.model_identity import model_annotator_id
    with pytest.raises(AnnotationError) as exc:
        model_annotator_id(name, version)
    fe = exc.value.friendly
    assert fe.title and fe.message and any("hyphen" in step for step in fe.recourse)


def test_every_built_name_passes_validate_annotator_id():
    from src.annotations.model_identity import model_annotator_id
    for name, version in [("knee-seg", "3"), ("A1", "B2"), ("m", "0")]:
        validate_annotator_id(model_annotator_id(name, version))


@pytest.mark.parametrize("human", ["student_a", "annotator_1", "worker_A1", "model", "model__x", "model__a_b__v1",
                                   "model__knee-seg__3", None])
def test_human_names_are_not_models(human):
    from src.annotations.model_identity import is_model_annotator, parse_model_annotator_id
    assert parse_model_annotator_id(human) is None
    assert not is_model_annotator(human)


# --- Phase 2: type loader (T009) ------------------------------------------------

def test_every_type_file_loads_with_model_predictions():
    types = load_types()
    mp = types["model_predictions"]
    assert mp.placement == "annotation_set" and mp.resource_label == "ANNOTATIONS"
    assert mp.publish_via_intake and not mp.multi_case
    assert set(mp.phi_policy) == {"no_pixels", "text_scan"}
    assert [o.pattern for o in mp.outputs] == ["predictions.json"]


def test_meta_schemas_accept_model_predictions():
    jsonschema = pytest.importorskip("jsonschema")
    meta = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "analysis-type.schema.json").read_text())
    jsonschema.validate(json.loads((ANALYSIS_TYPES_DIR / "model_predictions.json").read_text()), meta)
    pred = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "model_predictions.predictions.schema.json").read_text())
    jsonschema.validate({"entries": [{"image_identity_hash": "a", "annotation_type": "landmark", "payload": {}}]}, pred)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"entries": []}, pred)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"entries": [{"image_identity_hash": "a", "annotation_type": "x", "payload": 1,
                                          "extra": 1}]}, pred)
    desc_schema = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "analysis-descriptor.schema.json").read_text())
    desc = {"descriptor_version": "1", "type_name": "model_predictions", "type_version": 1,
            "run": {"case_uid": CASE, "code_ref": "git:0", "label": label(), "placement_used": "annotation_set",
                    "model": {"name": MODEL_NAME, "version": "3", "dataset_query_string": DATASET_QS}}}
    jsonschema.validate(desc, desc_schema)
    desc["run"]["model"]["extra"] = "x"
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(desc, desc_schema)


@pytest.mark.parametrize("change", [{"resource_label": "OTHER"}, {"multi_case": True}])
def test_annotation_set_placement_needs_annotations_label_single_case(change):
    data = json.loads((ANALYSIS_TYPES_DIR / "model_predictions.json").read_text())
    data.update(change)
    with pytest.raises(IntakeRefusal) as exc:
        check_type_data(data, Path("model_predictions.json"))
    assert exc.value.friendly.recourse
    assert next(iter(change)) in exc.value.friendly.message


def test_annotations_type_still_not_published_by_intake(tmp_path, record, fx, ct):
    types = load_types()
    assert types["annotations"].publish_via_intake is False
    folder = tmp_path / "ann"
    folder.mkdir()
    (folder / "manifest.json").write_text("{}")
    (folder / "analysis.json").write_text(json.dumps({
        "descriptor_version": "1", "type_name": "annotations", "type_version": 1,
        "run": {"case_uid": CASE, "code_ref": "git:0"}}))
    result = publish(folder, fx, ct, record)
    assert result.status == "refused" and "not published here" in result.friendly.title
    assert writes(fx) == []


# --- US1: publish one case's predictions (T016) ---------------------------------

def test_us1_publish_writes_set_and_catalog_row(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    result = publish(folder, fx, ct, record)
    assert result.status == "done", result.friendly
    assert result.label == label() and result.outcome.placement_used == "annotation_set"
    files = fx.scan_files(label())
    assert set(files) == {"manifest.json", "analysis.json",
                          f"ann__{aid()}__binary_segmentation__v1.rle", f"ann__{aid()}__landmark__v1.json"}
    # No annotation lands on the shared ANNOTATIONS resource and no blob is overwritten.
    assert fx.scan_files("ANNOTATIONS") == {}
    blob_puts = [c for c in writes(fx) if c["kwargs"].get("_filename", "").startswith("ann__")]
    assert blob_puts and all(c["kwargs"]["overwrite"] is False for c in blob_puts)
    desc_put = [c for c in writes(fx) if c["kwargs"].get("_filename") == "analysis.json"]
    assert len(desc_put) == 1 and desc_put[0]["kwargs"]["overwrite"] is False
    published = json.loads(files["analysis.json"])
    run = published["run"]
    assert run["producer"] == USERNAME
    assert run["model"] == {"name": MODEL_NAME, "version": "3", "dataset_query_string": DATASET_QS}
    assert run["label"] == label() and run["placement_used"] == "annotation_set"
    # The local analysis.json is updated so a rerun takes the catalog path.
    assert json.loads((folder / "analysis.json").read_text())["run"]["label"] == label()
    assert list(rows(ct)) == [row_name()]
    row = rows(ct)[row_name()]
    assert row["PLACEMENT_USED"] == "annotation_set" and row["PRODUCER"] == USERNAME
    assert row["QUERY_STRING"] == SCAN_QS and row["RESOURCE_LABEL"] == label()
    assert row["TYPE_NAME"] == "model_predictions"


def test_us1_downloaded_payloads_match(tmp_path, record, fx, ct):
    from src.annotations.io_xnat import download_annotation_set
    from src.annotations.model_identity import is_model_annotator
    folder = make_prediction_folder(tmp_path, record)
    assert publish(folder, fx, ct, record).status == "done"
    got = download_annotation_set(fx, SCAN_QS, tmp_path / "dl", resource_label=label())
    assert got.ok, got.friendly
    by_type = {a.annotation_type: a for a in got.annotation_set.annotations}
    assert np.array_equal(by_type["binary_segmentation"].payload, mask())
    assert by_type["landmark"].payload == {"x": 3, "y": 4}
    for ann in got.annotation_set.annotations:
        assert ann.annotator_id == aid() and is_model_annotator(ann.annotator_id)
        assert ann.tool == "model_predictions" and ann.version == 1 and ann.derived is False


def test_us1_find_derived_lists_the_set(tmp_path, record, fx, ct):
    from src.services.analysis_intake import find_derived
    folder = make_prediction_folder(tmp_path, record)
    assert publish(folder, fx, ct, record).status == "done"
    report = find_derived(frame_hashes(record)[0], gateway=fx, config_tables=ct)
    assert [(i.label, i.placement_used, i.resource_label) for i in report.items] == [
        (label(), "annotation_set", label())]


def test_us1_tampered_download_not_verified_no_row(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    fx.tamper[f"ann__{aid()}__landmark__v1.json"] = b'{"x":99,"y":4}'
    result = publish(folder, fx, ct, record)
    assert result.status == "not_verified"
    assert "not confirmed" in result.friendly.title.lower()
    assert rows(ct) == {}


def test_us1_tampered_descriptor_not_verified(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    fx.tamper["analysis.json"] = b"{}"
    result = publish(folder, fx, ct, record)
    assert result.status == "not_verified" and rows(ct) == {}


def test_us1_catalog_retry_adds_row(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    first = run_intake(folder, gateway=fx, config_tables=None, username=USERNAME, manifest=record,
                       classifier=stub_classifier)
    assert first.status == "catalog_failed"
    before = len(writes(fx))
    again = publish(folder, fx, ct, record)
    assert again.status == "done"
    assert len(writes(fx)) == before            # no second upload
    row = rows(ct)[row_name()]
    assert row["PLACEMENT_USED"] == "annotation_set"
    assert row["QUERY_STRING"] == SCAN_QS and row["RESOURCE_LABEL"] == label()


def test_us1_dry_run_needs_no_server(tmp_path, record):
    folder = make_prediction_folder(tmp_path, record)
    result = run_intake(folder, username=USERNAME, manifest=record, dry_run=True, classifier=stub_classifier)
    assert result.status == "dry_run_ok", result.friendly


# --- US1: refusals write nothing (T017) -----------------------------------------

def _entries(record, change):
    hashes = frame_hashes(record)
    good = [{"image_identity_hash": hashes[0], "annotation_type": "binary_segmentation", "payload": rle_payload(mask())},
            {"image_identity_hash": hashes[1], "annotation_type": "landmark", "payload": {"x": 3, "y": 4}}]
    if change == "unknown_hash":
        good[1]["image_identity_hash"] = "f" * 64
    elif change == "unknown_type":
        good[1]["annotation_type"] = "polygon"
    elif change == "bad_payload":
        good[1]["payload"] = {"x": 3}
    elif change == "bad_mask":
        good[0]["payload"] = {"shape": [8, 8], "runs": [[0, 3]]}
    elif change == "repeated_type":
        good[1] = dict(good[0], image_identity_hash=hashes[1])
    elif change == "flagged_text":
        good[1]["payload"] = {"x": 3, "y": 4, "label": FLAGGED_NAME}
    return good


@pytest.mark.parametrize("change,entry_no", [("unknown_hash", 2), ("unknown_type", 2), ("bad_payload", 2),
                                             ("bad_mask", 1), ("repeated_type", 2)])
def test_us1_entry_checks_refuse_and_name_entry(tmp_path, record, fx, ct, change, entry_no):
    folder = make_prediction_folder(tmp_path, record, entries=_entries(record, change))
    result = publish(folder, fx, ct, record)
    assert result.status == "refused", change
    assert f"entry {entry_no}" in result.friendly.message
    assert result.friendly.recourse
    assert writes(fx) == [] and rows(ct) == {}


def test_us1_flagged_text_refused(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record, entries=_entries(record, "flagged_text"))
    result = publish(folder, fx, ct, record)
    assert result.status == "refused" and "patient" in result.friendly.title.lower()
    assert FLAGGED_NAME not in result.friendly.message
    assert writes(fx) == []


def test_us1_flagged_notes_refused(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record, notes=f"run for {FLAGGED_NAME}")
    result = publish(folder, fx, ct, record)
    assert result.status == "refused" and writes(fx) == []


def test_us1_image_file_in_folder_refused(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    make_synthetic_dicom(folder / "copied.dcm", seed=7)
    result = publish(folder, fx, ct, record)
    assert result.status == "refused" and "copied.dcm" in result.friendly.message
    assert writes(fx) == []


def test_us1_no_classifier_fails_closed(tmp_path, record, fx, ct, monkeypatch):
    from src.services.analysis_intake import gates

    def unavailable():
        raise gates.ClassifierUnavailable("not installed")
    monkeypatch.setattr(gates, "default_classifier", unavailable)
    folder = make_prediction_folder(tmp_path, record)
    result = publish(folder, fx, ct, record, classifier=None)
    assert result.status == "refused" and writes(fx) == []


# --- US2: consensus over humans plus the model (T018, T018b, T019) --------------

def _human_set(image_ref, n=3):
    return AnnotationSet(image_ref=image_ref, annotations=[
        Annotation(f"annotator_{k}", "synthetic_tool", "binary_segmentation", "2026-10-03T00:00:00Z", 1,
                   payload=mask(k)) for k in range(1, n + 1)])


def test_us2_consensus_over_three_humans_and_one_model(tmp_path, record, fx, ct):
    from src.annotations.aggregate import aggregate_set
    from src.annotations.io_xnat import download_annotation_set, upload_annotation_set
    from src.annotations.model_identity import is_model_annotator
    for k in range(1, 4):
        one = AnnotationSet(SCAN_QS, [_human_set(SCAN_QS).annotations[k - 1]])
        assert upload_annotation_set(fx, SCAN_QS, one, resource_label=f"ANNOTATIONS_annotator_{k}").ok
    folder = make_prediction_folder(tmp_path, record)
    assert publish(folder, fx, ct, record).status == "done"
    combined = AnnotationSet(SCAN_QS)
    for res in [f"ANNOTATIONS_annotator_{k}" for k in range(1, 4)] + [label()]:
        got = download_annotation_set(fx, SCAN_QS, tmp_path / res, resource_label=res)
        assert got.ok, got.friendly
        combined.annotations.extend(got.annotation_set.annotations)
    masks = {a: ann for (a, t), ann in combined.latest_per_annotator().items() if t == "binary_segmentation"}
    assert len(masks) == 4 and sum(is_model_annotator(a) for a in masks) == 1
    # STAPLE is a registered-on-demand stub in this repository (it raises
    # NotImplementedError), so consensus runs with the type's default aggregator;
    # see research.md "Open conflicts (implement)".
    result = aggregate_set(combined, "binary_segmentation")
    assert result.payload.shape == mask().shape


def test_us2_loader_merges_every_annotation_resource(tmp_path, record, fx, ct):
    from src.annotations.aggregate import aggregate_set
    from src.annotations.io_xnat import list_annotation_resources, upload_annotation_set
    from src.annotations.model_identity import is_model_annotator
    from app.logic.annotations import list_image_annotations, load_annotation_sets
    assert upload_annotation_set(fx, SCAN_QS, _human_set(SCAN_QS)).ok          # three humans on ANNOTATIONS
    assert publish(make_prediction_folder(tmp_path, record), fx, ct, record).status == "done"
    assert list_annotation_resources(fx, SCAN_QS) == ["ANNOTATIONS", label()]
    for loader in (load_annotation_sets, list_image_annotations):
        merged = loader(fx, SCAN_QS)
        assert isinstance(merged, AnnotationSet), merged
        masks = {a for (a, t) in merged.latest_per_annotator() if t == "binary_segmentation"}
        assert masks == {"annotator_1", "annotator_2", "annotator_3", aid()}
        assert sum(is_model_annotator(a) for a in masks) == 1
        assert aggregate_set(merged, "binary_segmentation").payload.shape == mask().shape


def test_us2_listing_ignores_other_resources_and_loader_falls_back(tmp_path):
    from src.annotations.io_xnat import list_annotation_resources, upload_annotation_set
    from app.logic.annotations import load_annotation_sets
    fake = ScanFakeXNAT(project_name=PROJECT)
    other = tmp_path / "x.txt"
    other.write_text("x")
    fake.put_file(SCAN_QS, "SRC", "x.txt", str(other))
    fake.put_file(SCAN_QS, "ANNOTATIONSX", "x.txt", str(other))
    assert list_annotation_resources(fake, SCAN_QS) == []
    # A server without a resource listing still gets the default resource (old behaviour).
    plain = FakeXNAT(project_name=PROJECT)
    assert upload_annotation_set(plain, SCAN_QS, _human_set(SCAN_QS)).ok
    merged = load_annotation_sets(plain, SCAN_QS)
    assert {a.annotator_id for a in merged.annotations} == {"annotator_1", "annotator_2", "annotator_3"}


def test_us2_review_checks_accept_mixed_set_and_aggregate_signature_unchanged(tmp_path, record, fx, ct):
    from src.annotations import aggregate
    from src.annotations.validate import validate_mask_payload, validate_mask_shape
    from app.logic.annotations import load_annotation_sets
    from src.annotations.io_xnat import upload_annotation_set
    assert upload_annotation_set(fx, SCAN_QS, _human_set(SCAN_QS)).ok
    assert publish(make_prediction_folder(tmp_path, record), fx, ct, record).status == "done"
    merged = load_annotation_sets(fx, SCAN_QS)
    masks = [a.payload for a in merged.annotations if a.annotation_type == "binary_segmentation"]
    for ann in merged.annotations:
        validate_annotator_id(ann.annotator_id)
    for m in masks:
        validate_mask_payload(m)
    validate_mask_shape(masks)
    assert list(inspect.signature(aggregate.aggregate_set).parameters) == ["annotation_set", "type_name",
                                                                         "aggregator_name"]


# --- US3: missing dataset refused (T022) ----------------------------------------

@pytest.mark.parametrize("problem", ["wrong_address", "no_descriptor", "gateway_error", "no_model", "empty_model"])
def test_us3_dataset_and_model_checks_refuse(tmp_path, record, fx, ct, problem):
    kw = {}
    if problem == "wrong_address":
        kw["dataset_qs"] = f"{PROJECT_QS}/resources/knee_hip_set__v9"
    elif problem == "no_descriptor":
        other = tmp_path / "readme.txt"
        other.write_text("x")
        fx.put_file(PROJECT_QS, "half_set__v1", "readme.txt", str(other), overwrite=False)
        fx.reset_calls()
        kw["dataset_qs"] = f"{PROJECT_QS}/resources/half_set__v1"
    elif problem == "no_model":
        kw["model"] = False
    folder = make_prediction_folder(tmp_path, record, **kw)
    if problem == "empty_model":
        desc = json.loads((folder / "analysis.json").read_text())
        desc["run"]["model"] = {"name": "", "version": "", "dataset_query_string": ""}
        (folder / "analysis.json").write_text(json.dumps(desc))
    if problem == "gateway_error":
        original = fx.list_files

        def failing(qs, res):
            if qs == PROJECT_QS:
                raise ConnectionError("synthetic outage")
            return original(qs, res)
        fx.list_files = failing
    result = publish(folder, fx, ct, record)
    assert result.status == "refused", problem
    fe = result.friendly
    assert fe.recourse
    if problem in ("wrong_address", "no_descriptor", "gateway_error"):
        assert "/resources/" in fe.message and "catalog" in " ".join(fe.recourse).lower()
    if problem in ("no_model", "empty_model"):
        for f in ("name", "version", "dataset_query_string"):
            assert f in fe.message
    assert writes(fx) == [] and rows(ct) == {}


def test_us3_rest_style_dataset_address_accepted(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record, dataset_qs=f"/data/projects/{PROJECT}/resources/{DATASET_LABEL}")
    assert publish(folder, fx, ct, record).status == "done"


def test_us3_bad_model_name_refused_with_hint(tmp_path, record, fx, ct):
    folder = make_prediction_folder(tmp_path, record)
    desc = json.loads((folder / "analysis.json").read_text())
    desc["run"]["model"]["version"] = "1.2"
    (folder / "analysis.json").write_text(json.dumps(desc))
    result = publish(folder, fx, ct, record)
    assert result.status == "refused" and any("hyphen" in s for s in result.friendly.recourse)
    assert writes(fx) == []


# --- US4: second model version (T023) -------------------------------------------

def test_us4_versions_side_by_side_and_same_version_refused(tmp_path, record, fx, ct):
    assert publish(make_prediction_folder(tmp_path, record, version="3"), fx, ct, record).status == "done"
    v3_before = fx.scan_files(label("3"))
    assert publish(make_prediction_folder(tmp_path, record, version="4"), fx, ct, record).status == "done"
    assert fx.scan_files(label("3")) == v3_before                    # byte for byte unchanged
    assert set(fx.scan_files(label("4"))) >= {"manifest.json", "analysis.json"}
    assert sorted(rows(ct)) == [row_name("3"), row_name("4")]
    before = len(writes(fx))
    again = publish(make_prediction_folder(tmp_path, record, version="3", name="pred_v3_again"), fx, ct, record)
    assert again.status == "refused"
    assert any("raise the model version in analysis.json" in s for s in again.friendly.recourse)
    assert len(writes(fx)) == before
    assert fx.scan_files(label("3")) == v3_before
    assert not [c for c in fx.calls if "delete" in c["op"]]


def test_us4_no_delete_call_in_any_publish(tmp_path, record, fx, ct):
    publish(make_prediction_folder(tmp_path, record), fx, ct, record)
    assert not [c for c in fx.calls if "delete" in c["op"]]


# --- Polish: --init template (T025) ---------------------------------------------

def test_init_template_has_model_fields(tmp_path):
    from src.services.analysis_intake.cli import main
    out: List[str] = []
    folder = tmp_path / "new"
    assert main(["--init", "model_predictions", str(folder)], out=out.append) == 0
    desc = json.loads((folder / "analysis.json").read_text())
    assert desc["type_name"] == "model_predictions"
    assert desc["run"]["model"] == {"name": "", "version": "", "dataset_query_string": ""}
    assert "run.model" in " ".join(out)
    # Other types keep their 016 template without a model block.
    assert main(["--init", "knee_flexion_angle", str(tmp_path / "knee")], out=out.append) == 0
    assert "model" not in json.loads((tmp_path / "knee" / "analysis.json").read_text())["run"]
