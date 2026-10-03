"""
US1 / KNEE_2025: full round trip against the live XNAT container (spec 014).

Production publishes the RF folder (42 RF + 18 CT DICOM) and the ESV folder
(5 MP4) as two sessions. The harness then reads everything back over the
admin session and checks de-identification, annotations, segmentations, STAPLE,
checksums, traceability and the production browse listing. Tests run in file
order and share one module-scoped state dict.
"""
from __future__ import annotations

import json

import numpy as np
import pydicom
import pytest

from tests.integration.live_xnat import helpers as H
from tests.integration.live_xnat.cases import build_case
from tests.synthetic_data import BRIGHT_TEXT_THRESHOLD

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

CASE = "KNEE_2025"


@pytest.fixture(scope="module")
def knee(live_xnat_server, knee_rf_published):
    # The radiofluoro session is published once per run (shared with the
    # spec 015 manifest module); only the endoscopy session is published here.
    case, work = knee_rf_published["case"], knee_rf_published["work"]
    state = {"case": case, "work": work, "dispositions": {}, "counts": {}, "scan_layout": {}, "absent_tags": {}}
    state["rf"] = knee_rf_published["rf"]
    state["esv"] = H.publish_case_session(live_xnat_server, case, "esv", work)
    H.record_case_subject(live_xnat_server, CASE, state["esv"].uid)
    return state


def _downloaded(live, knee, kind):
    key = f"dl_{kind}"
    if key not in knee:
        knee[key] = H.download_scan_files(live, knee[kind], knee["work"] / f"download_{kind}")
    return knee[key]


def _fluoro_downloads(live, knee):
    """Downloaded fluoroscopy frames keyed by source file name."""
    dl = _downloaded(live, knee, "rf")
    rf = knee["rf"]
    out = {}
    for src_name, new_fn in rf.source_to_new.items():
        if src_name.startswith("knee_fluoro_") and new_fn in dl:
            out[src_name] = dl[new_fn]
    return out


def test_ingestion_through_production(live_xnat_server, knee):
    case, rf, esv = knee["case"], knee["rf"], knee["esv"]
    for name in [p.name for p in case.rf_files]:
        knee["dispositions"][name] = "published" if name in rf.source_to_new else "rejected"
    for name in [p.name for p in case.esv_files]:
        knee["dispositions"][name] = "published" if esv.production_exception is None and esv.publish_exception is None else "failed_recoverable"
    server_rf = H.list_scan_files(live_xnat_server, rf) if rf.scan_uri else []
    server_esv = H.list_scan_files(live_xnat_server, esv) if esv.scan_uri else []
    knee["counts"]["processed"] = len(server_rf) + len(server_esv)
    knee["counts"]["rejected"] = sum(v == "rejected" for v in knee["dispositions"].values())
    status = "success" if rf.production_exception is None and esv.production_exception is None else "failure"
    H.set_phase(knee, "ingestion", status, files=[r["Name"] for r in server_rf + server_esv],
                links=[x for x in (rf.files_uri, esv.files_uri) if x],
                error="; ".join(x for x in (rf.production_exception, rf.publish_exception,
                                            esv.production_exception, esv.publish_exception) if x) or None)
    assert rf.production_exception is None, rf.production_exception
    assert rf.publish_exception is None, rf.publish_exception
    assert esv.production_exception is None, esv.production_exception
    assert esv.publish_exception is None, esv.publish_exception
    assert len(server_rf) == len(rf.published_files), (
        f"server holds {len(server_rf)} RF files, production published {len(rf.published_files)}")
    assert len(server_esv) == len(esv.published_files), (
        f"server holds {len(server_esv)} ESV files, production published {len(esv.published_files)}")
    # SPEC-ISSUE-17: production gives every video after the first the same
    # NEW_FN (src/xnat_experiment_data.py:1247), so the ESV zip keeps 2 of 5.
    assert len(server_rf) + len(server_esv) == 65, (
        f"server holds {len(server_rf)} RF + {len(server_esv)} ESV files; "
        f"ESV NEW_FN map {esv.source_to_new}")


def _expected_patient_id():
    """PatientID the production de-identifier writes (src/services/deidentify.py)."""
    from src.services.deidentify import deidentify_dataset
    from src.utilities import UIDandMetaInfo
    from tests.synthetic_data import make_phi_dicom_dataset

    ds = make_phi_dicom_dataset(rows=8, cols=8, seed=0)
    deidentify_dataset(ds, UIDandMetaInfo().redacted_string)
    return ds.PatientID


def test_header_deidentification(live_xnat_server, knee):
    from src.utilities import UIDandMetaInfo

    redacted = UIDandMetaInfo().redacted_string
    patient_id = _expected_patient_id()
    dl = _downloaded(live_xnat_server, knee, "rf")
    assert len(dl) > 0, "no DICOM downloaded"
    bad = []
    for name, path in dl.items():
        ds = pydicom.dcmread(str(path))
        for elem in ds.iterall():
            if elem.VR == "PN" and elem.value not in (None, "") and str(elem.value) != redacted:
                bad.append(f"{name}:{elem.keyword}={elem.value}")
        if str(ds.get("PatientID", "")) != patient_id:
            bad.append(f"{name}:PatientID={ds.get('PatientID')}")
    H.set_phase(knee, "deidentification", "success" if not bad else "failure",
                files=list(dl), error="; ".join(bad[:5]) or None)
    assert not bad, bad[:10]


@pytest.mark.known_issue
@pytest.mark.xfail(strict=True, reason="FR-022 known gap: SPEC-ISSUE-4; https://github.com/domattioli/XNAT-Interact/issues/64")
def test_patient_birth_date_removed(live_xnat_server, knee):
    dl = _downloaded(live_xnat_server, knee, "rf")
    assert dl
    kept = [n for n, p in dl.items() if str(pydicom.dcmread(str(p)).get("PatientBirthDate", "") or "")]
    assert not kept, f"PatientBirthDate kept in {len(kept)} files"


@pytest.mark.known_issue
@pytest.mark.xfail(strict=True, reason="FR-022 known gap: SPEC-ISSUE-3; https://github.com/domattioli/XNAT-Interact/issues/63")
def test_burned_in_phi_redacted(live_xnat_server, knee):
    case = knee["case"]
    frames = _fluoro_downloads(live_xnat_server, knee)
    assert len(frames) == 42
    leaks = []
    for src_name, path in frames.items():
        x0, y0, x1, y1 = case.phi_boxes[src_name]
        after = pydicom.dcmread(str(path)).pixel_array[y0:y1, x0:x1]
        before = pydicom.dcmread(str(case.rf_dir / src_name)).pixel_array[y0:y1, x0:x1]
        if np.array_equal(after, before) or np.count_nonzero(after > BRIGHT_TEXT_THRESHOLD) > 0:
            leaks.append(src_name)
    assert not leaks, f"burned-in PHI unchanged in {len(leaks)} frames"


def test_series_split_and_scan_layout(live_xnat_server, knee):
    dl = _downloaded(live_xnat_server, knee, "rf")
    series = {}
    for path in dl.values():
        uid = pydicom.dcmread(str(path), stop_before_pixels=True).SeriesInstanceUID
        series[uid] = series.get(uid, 0) + 1
    knee["scan_layout"].update(H.scan_layout(live_xnat_server, knee["rf"]))
    knee["scan_layout"].update(H.scan_layout(live_xnat_server, knee["esv"]))
    assert sorted(series.values()) == [18, 42], series


def test_annotations_round_trip(live_xnat_server, knee, tmp_path):
    from src.annotations.io_xnat import download_annotation_set, upload_annotation_set
    from src.annotations.model import Annotation, AnnotationSet

    rf = knee["rf"]
    frames = _fluoro_downloads(live_xnat_server, knee)
    first = pydicom.dcmread(str(frames[sorted(frames)[0]])).pixel_array
    gateway = live_xnat_server["connection"].gateway  # SPEC-ISSUE-13: production calls put_file
    image_ref = rf.scan_uri
    errors, links = [], []
    for k in range(3):
        annotator = f"annotator_{k}"
        aset = AnnotationSet(image_ref=image_ref, annotations=[
            Annotation(annotator, "live_it", "landmark", H.now_iso(), 1, payload={"x": 10 + k, "y": 20 + k}),
            Annotation(annotator, "live_it", "bbox", H.now_iso(), 1, payload={"x": 5, "y": 6, "w": 30 + k, "h": 40}),
            Annotation(annotator, "live_it", "binary_segmentation", H.now_iso(), 1,
                       payload=(first > first.mean() + k).astype(np.uint8)),
        ])
        label = f"ANNOTATIONS_{annotator}"
        up = upload_annotation_set(gateway, image_ref, aset, resource_label=label)
        if not up.ok:
            errors.append(f"{annotator} upload: {up.friendly}")
            continue
        down = download_annotation_set(gateway, image_ref, tmp_path / annotator, resource_label=label)
        if not down.ok:
            errors.append(f"{annotator} download: {down.friendly}")
            continue
        got = down.annotation_set
        assert got.image_ref == image_ref
        assert len(got.annotations) == 3
        for a, b in zip(aset.annotations, got.annotations):
            assert (a.annotator_id, a.annotation_type) == (b.annotator_id, b.annotation_type)
            if isinstance(a.payload, np.ndarray):
                assert np.array_equal(a.payload, np.asarray(b.payload))
            else:
                assert a.payload == b.payload
        links.append(f"{image_ref.rstrip('/')}/resources/{label}")
    knee["annotation_links"] = links
    exists = H.get_json(live_xnat_server, f"{image_ref.rstrip('/')}?format=json")
    H.set_phase(knee, "annotation", "success" if not errors else "failure", links=links,
                error="; ".join(errors) or None)
    assert not errors, errors
    assert exists


def test_segmentations_round_trip(live_xnat_server, knee):
    rf = knee["rf"]
    frames = _fluoro_downloads(live_xnat_server, knee)
    stack = np.stack([pydicom.dcmread(str(frames[n])).pixel_array for n in sorted(frames)])
    masks, links = {}, []
    for seg in H.SEGMENTERS:
        m = H.make_masks(stack, seg)
        files = {
            "masks.npz": H.npz_bytes(masks=m),
            "confidence.npz": H.npz_bytes(confidence=m.astype(np.float32)),
            "labels.json": json.dumps({"1": "region"}).encode(),
            "source.json": json.dumps({"source_scan": rf.scan_uri}).encode(),
        }
        H.put_derived_resource(live_xnat_server, rf, f"SEG_{seg}", files)
        links.append(f"{rf.scan_uri.rstrip('/')}/resources/SEG_{seg}")
        got = H.npz_load(H.get_derived_resource(live_xnat_server, rf, f"SEG_{seg}")["masks.npz"])["masks"]
        assert got.shape == stack.shape, (got.shape, stack.shape)
        assert got.shape[0] == 42
        masks[seg] = got
    knee["masks"], knee["seg_links"] = masks, links
    H.set_phase(knee, "segmentation", "success", links=links)


def test_staple_round_trip(live_xnat_server, knee):
    from src.annotations.aggregate import get_aggregator

    rf = knee["rf"]
    masks = knee["masks"]
    local = get_aggregator("staple").aggregate([masks[s] for s in H.SEGMENTERS], annotator_ids=list(H.SEGMENTERS))
    files = {
        "staple.npz": H.npz_bytes(consensus_mask=np.asarray(local.payload["consensus_mask"]),
                                  uncertainty_map=np.asarray(local.payload["uncertainty_map"])),
        "scores.json": json.dumps({k: float(v) for k, v in dict(local.payload["scores"]).items()}).encode(),
        "source.json": json.dumps({"source_scan": rf.scan_uri, "inputs": knee["seg_links"]}).encode(),
    }
    H.put_derived_resource(live_xnat_server, rf, "STAPLE", files)
    got = H.get_derived_resource(live_xnat_server, rf, "STAPLE")
    arrs = H.npz_load(got["staple.npz"])
    scores = json.loads(got["scores.json"])
    assert np.array_equal(arrs["consensus_mask"], np.asarray(local.payload["consensus_mask"]))
    assert np.array_equal(arrs["uncertainty_map"], np.asarray(local.payload["uncertainty_map"]))
    assert len(scores) == 3
    H.set_phase(knee, "staple", "success", links=[f"{rf.scan_uri.rstrip('/')}/resources/STAPLE"])


def test_checksums_match_local_deidentified(live_xnat_server, knee):
    passed, failed = 0, []
    for kind in ("rf", "esv"):
        dl = _downloaded(live_xnat_server, knee, kind)
        local = knee[kind].published_files
        for name, path in dl.items():
            if name in local and H.sha256(path) == H.sha256(local[name]):
                passed += 1
            else:
                failed.append(name)
    knee["counts"]["checksum_pass"], knee["counts"]["checksum_fail"] = passed, len(failed)
    H.set_phase(knee, "integrity_validation", "success" if passed == 65 and not failed else "failure",
                error=f"mismatch: {failed[:5]}" if failed else None)
    assert passed > 0, "zero files compared"
    assert not failed, failed[:10]
    assert passed == 65


def test_traceability(live_xnat_server, knee):
    rf = knee["rf"]
    for label in [f"SEG_{s}" for s in H.SEGMENTERS] + ["STAPLE"]:
        src = json.loads(H.get_derived_resource(live_xnat_server, rf, label)["source.json"])
        assert H.get_json(live_xnat_server, f"{src['source_scan'].rstrip('/')}?format=json")
        if label == "STAPLE":
            assert sorted(src["inputs"]) == sorted(knee["seg_links"])
    for k in range(3):
        rows = H.get_json(live_xnat_server, f"{rf.scan_uri.rstrip('/')}/resources/ANNOTATIONS_annotator_{k}/files?format=json")
        assert rows["ResultSet"]["Result"], f"annotator_{k} resource empty"


def test_browse_lists_case(live_xnat_server, knee):
    from app.logic.browse import fetch_data_table

    rows = fetch_data_table(live_xnat_server["connection"].server, live_xnat_server["project_name"])
    assert isinstance(rows, list), rows
    for kind in ("rf", "esv"):
        res = knee[kind]
        server_count = len(H.list_scan_files(live_xnat_server, res))
        match = [r for r in rows if r.get("subject") == res.subject_label and r.get("experiment") == res.experiment_label]
        assert match, f"{kind} {res.subject_label}/{res.experiment_label} missing from browse listing"
        assert sum(int(r.get("num_files", 0)) for r in match) == server_count


def test_outcome_summary(live_xnat_server, knee, outcome_writer):
    path = H.write_case_summary(knee, outcome_writer, CASE)
    H.validate_summary(path)
    assert len(json.loads(path.read_text())["phases"]) == 6
