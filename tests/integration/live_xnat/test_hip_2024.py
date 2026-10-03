"""
US2 / HIP_2024: multi-modality case against the live XNAT container (spec 014).

Production publishes 18 faint-PHI XA frames, one 8-frame XA and one 9-frame US
instance as one RF session. The harness checks modality survival, absent tags,
per-frame annotation and segmentation outputs, STAPLE, checksums and
traceability. Tests run in file order and share one module-scoped state dict.
"""
from __future__ import annotations

import json

import numpy as np
import pydicom
import pytest

from tests.integration.live_xnat import helpers as H
from tests.integration.live_xnat.cases import build_case

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

CASE = "HIP_2024"


@pytest.fixture(scope="module")
def hip(live_xnat_server, tmp_path_factory):
    work = tmp_path_factory.mktemp("hip_2024")
    case = build_case(CASE, work / "src")
    state = {"case": case, "work": work, "dispositions": {}, "counts": {}, "scan_layout": {}, "absent_tags": {}}
    state["rf"] = H.publish_case_session(live_xnat_server, case, "rf", work)
    H.record_case_subject(live_xnat_server, CASE, state["rf"].uid)
    return state


def _downloads(live, hip):
    """Downloaded files keyed by source file name."""
    if "dl" not in hip:
        dl = H.download_scan_files(live, hip["rf"], hip["work"] / "download")
        hip["dl"] = {src: dl[new] for src, new in hip["rf"].source_to_new.items() if new in dl}
        hip["dl_all"] = dl
    return hip["dl"]


def test_ingestion_through_production(live_xnat_server, hip):
    case, rf = hip["case"], hip["rf"]
    for p in case.files:
        hip["dispositions"][p.name] = "published" if p.name in rf.source_to_new else "rejected"
    server = H.list_scan_files(live_xnat_server, rf) if rf.scan_uri else []
    hip["counts"]["processed"] = len(server)
    hip["counts"]["rejected"] = sum(v == "rejected" for v in hip["dispositions"].values())
    err = "; ".join(x for x in (rf.production_exception, rf.publish_exception) if x) or None
    H.set_phase(hip, "ingestion", "success" if err is None else "failure",
                files=[r["Name"] for r in server], links=[rf.files_uri] if rf.files_uri else [], error=err)
    assert rf.production_exception is None, rf.production_exception
    assert rf.publish_exception is None, rf.publish_exception
    assert len(server) == len(rf.published_files)
    us_name = "hip_multiframe_us.dcm"
    if len(server) == 19:
        assert us_name in rf.rejections, f"19 files on server but no recorded US rejection: {rf.rejections}"
    else:
        assert len(server) == 20, f"server holds {len(server)} files; rejections {rf.rejections}"


def test_modality_preserved_and_scan_layout(live_xnat_server, hip):
    case = hip["case"]
    dl = _downloads(live_xnat_server, hip)
    assert dl, "no files downloaded"
    wrong = []
    for src, path in dl.items():
        before = pydicom.dcmread(str(case.rf_dir / src), stop_before_pixels=True).Modality
        after = pydicom.dcmread(str(path), stop_before_pixels=True).Modality
        if before != after:
            wrong.append(f"{src}: {before} became {after}")
    hip["scan_layout"].update(H.scan_layout(live_xnat_server, hip["rf"]))
    assert not wrong, wrong


@pytest.mark.known_issue
@pytest.mark.xfail(strict=True, reason="FR-022 known gap: SPEC-ISSUE-3; https://github.com/domattioli/XNAT-Interact/issues/63")
def test_faint_burned_in_phi_redacted(live_xnat_server, hip):
    case = hip["case"]
    dl = _downloads(live_xnat_server, hip)
    singles = [s for s in dl if s.startswith("hip_fluoro_faint_")]
    assert len(singles) == 18
    unchanged = []
    for src in singles:
        x0, y0, x1, y1 = case.phi_boxes[src]
        after = pydicom.dcmread(str(dl[src])).pixel_array[y0:y1, x0:x1]
        before = pydicom.dcmread(str(case.rf_dir / src)).pixel_array[y0:y1, x0:x1]
        if np.array_equal(after, before):
            unchanged.append(src)
    assert not unchanged, f"faint PHI box unchanged in {len(unchanged)} frames"


def test_absent_tags_tolerated(live_xnat_server, hip):
    from src.utilities import UIDandMetaInfo

    case = hip["case"]
    redacted = UIDandMetaInfo().redacted_string
    dl = _downloads(live_xnat_server, hip)
    observed, holding = {}, []
    for src, tags in case.expected_absent_tags.items():
        if src not in dl:
            continue
        ds = pydicom.dcmread(str(dl[src]), stop_before_pixels=True)
        observed[src] = [t for t in tags if t not in ds or str(ds.get(t) or "") in ("", redacted)]
        holding += [f"{src}:{t}={ds.get(t)}" for t in tags if t in ds and str(ds.get(t) or "") not in ("", redacted)]
    hip["absent_tags"] = observed
    assert observed == case.expected_absent_tags
    assert not holding, holding


def _multiframes(live, hip):
    dl = _downloads(live, hip)
    out = {}
    for name in hip["case"].expected_frames:
        if name in dl:
            out[name] = pydicom.dcmread(str(dl[name])).pixel_array
    return out


def test_per_frame_annotations_and_segmentations(live_xnat_server, hip, tmp_path):
    from src.annotations.io_xnat import download_annotation_set, upload_annotation_set
    from src.annotations.model import Annotation, AnnotationSet

    rf = hip["rf"]
    gateway = live_xnat_server["connection"].gateway  # SPEC-ISSUE-13
    frames = _multiframes(live_xnat_server, hip)
    assert sorted(frames) == sorted(hip["case"].expected_frames)
    links, masks = [], {}
    for name, arr in frames.items():
        n = hip["case"].expected_frames[name]
        assert arr.shape[0] == n, (name, arr.shape)
        tag = name.replace(".dcm", "")
        aset = AnnotationSet(image_ref=rf.scan_uri, annotations=[
            Annotation(f"annotator_{tag}", "live_it", "landmark", H.now_iso(), i + 1,
                       payload={"x": 10 + i, "y": 12 + i}) for i in range(n)
        ])
        label = f"ANNOTATIONS_{tag}"
        up = upload_annotation_set(gateway, rf.scan_uri, aset, resource_label=label)
        assert up.ok, up.friendly
        down = download_annotation_set(gateway, rf.scan_uri, tmp_path / tag, resource_label=label)
        assert down.ok, down.friendly
        assert len(down.annotation_set.annotations) == n
        assert [a.payload for a in down.annotation_set.annotations] == [a.payload for a in aset.annotations]
        links.append(f"{rf.scan_uri.rstrip('/')}/resources/{label}")
        for seg in H.SEGMENTERS:
            m = H.make_masks(arr, seg)
            res = f"SEG_{seg}_{tag}"
            H.put_derived_resource(live_xnat_server, rf, res, {
                "masks.npz": H.npz_bytes(masks=m),
                "source.json": json.dumps({"source_scan": rf.scan_uri, "source_file": name}).encode(),
            })
            got = H.npz_load(H.get_derived_resource(live_xnat_server, rf, res)["masks.npz"])["masks"]
            assert got.shape == arr.shape
            assert got.shape[0] == n
            masks.setdefault(tag, {})[seg] = got
    hip["masks"], hip["annotation_links"] = masks, links
    H.set_phase(hip, "annotation", "success", links=links)
    H.set_phase(hip, "segmentation", "success",
                links=[f"{rf.scan_uri.rstrip('/')}/resources/SEG_{s}_{t}" for t in masks for s in H.SEGMENTERS])


def test_staple_checksums_traceability(live_xnat_server, hip):
    from src.annotations.aggregate import get_aggregator

    rf = hip["rf"]
    staple_links = []
    for tag, by_seg in hip["masks"].items():
        local = get_aggregator("staple").aggregate([by_seg[s] for s in H.SEGMENTERS], annotator_ids=list(H.SEGMENTERS))
        inputs = [f"{rf.scan_uri.rstrip('/')}/resources/SEG_{s}_{tag}" for s in H.SEGMENTERS]
        res = f"STAPLE_{tag}"
        H.put_derived_resource(live_xnat_server, rf, res, {
            "staple.npz": H.npz_bytes(consensus_mask=np.asarray(local.payload["consensus_mask"]),
                                      uncertainty_map=np.asarray(local.payload["uncertainty_map"])),
            "scores.json": json.dumps({k: float(v) for k, v in dict(local.payload["scores"]).items()}).encode(),
            "source.json": json.dumps({"source_scan": rf.scan_uri, "inputs": inputs}).encode(),
        })
        got = H.get_derived_resource(live_xnat_server, rf, res)
        arrs = H.npz_load(got["staple.npz"])
        assert np.array_equal(arrs["consensus_mask"], np.asarray(local.payload["consensus_mask"]))
        assert np.array_equal(arrs["uncertainty_map"], np.asarray(local.payload["uncertainty_map"]))
        assert len(json.loads(got["scores.json"])) == 3
        src = json.loads(got["source.json"])
        assert H.get_json(live_xnat_server, f"{src['source_scan'].rstrip('/')}?format=json")
        assert sorted(src["inputs"]) == sorted(inputs)
        for s in H.SEGMENTERS:
            seg_src = json.loads(H.get_derived_resource(live_xnat_server, rf, f"SEG_{s}_{tag}")["source.json"])
            assert H.get_json(live_xnat_server, f"{seg_src['source_scan'].rstrip('/')}?format=json")
        staple_links.append(f"{rf.scan_uri.rstrip('/')}/resources/{res}")
    H.set_phase(hip, "staple", "success", links=staple_links)

    _downloads(live_xnat_server, hip)
    passed, failed = 0, []
    for name, path in hip["dl_all"].items():
        local = rf.published_files.get(name)
        if local is not None and H.sha256(path) == H.sha256(local):
            passed += 1
        else:
            failed.append(name)
    hip["counts"]["checksum_pass"], hip["counts"]["checksum_fail"] = passed, len(failed)
    H.set_phase(hip, "integrity_validation", "success" if passed and not failed else "failure",
                error=f"mismatch: {failed[:5]}" if failed else None)
    assert passed > 0, "zero files compared"
    assert not failed, failed[:10]
    assert passed == len(rf.published_files)


def test_deidentification_headers(live_xnat_server, hip):
    from src.utilities import UIDandMetaInfo

    redacted = UIDandMetaInfo().redacted_string
    dl = _downloads(live_xnat_server, hip)
    bad = []
    for src, path in dl.items():
        for elem in pydicom.dcmread(str(path), stop_before_pixels=True).iterall():
            if elem.VR == "PN" and elem.value not in (None, "") and str(elem.value) != redacted:
                bad.append(f"{src}:{elem.keyword}")
    H.set_phase(hip, "deidentification", "success" if not bad else "failure", files=list(dl),
                error="; ".join(bad[:5]) or None)
    assert not bad, bad[:10]


def test_outcome_summary(live_xnat_server, hip, outcome_writer):
    path = H.write_case_summary(hip, outcome_writer, CASE)
    H.validate_summary(path)
    assert len(json.loads(path.read_text())["phases"]) == 6
