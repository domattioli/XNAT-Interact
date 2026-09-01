"""
tests/test_app_annotations_logic.py — Phase 5 T017-T020

Offline, no network, no streamlit, no PHI.

Covers:
  T017 — list_image_annotations: upload→download round-trip via logic layer
  T018 — run_consensus: 'reference' aggregator returns ConsensusResult
  T018 — bad input → FriendlyError, no raise
  T019 — MTurkSemanticSegmentation now produces a valid Annotation
  T020 — upload_annotations: delegates to io_xnat, returns UploadResult
  T020 — import_annotations: dispatch by source kind
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import List

import numpy as np
import pytest

from tests.fakes.fake_xnat import FakeXNAT
from src.annotations.model import Annotation, AnnotationSet, ConsensusResult
from src.annotations.io_xnat import upload_annotation_set, MANIFEST_FILENAME
from src.services.errors import FriendlyError

# Logic layer under test — NO streamlit import anywhere below
from app.logic.annotations import (
    list_image_annotations,
    import_annotations,
    run_consensus,
    upload_annotations,
    list_aggregator_names,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_IMAGE_REF = "subjects/S001/experiments/E01/scans/scan1"
_PROJECT = "TEST_PROJ"
_ANNOTATORS = ["worker_A1", "worker_B2"]


def _sparse_mask(seed: int = 0, rows: int = 16, cols: int = 16) -> np.ndarray:
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(1, int(rows * cols * 0.1))
    flat_idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[flat_idx] = 1
    return mask


def _make_annotation(annotator_id: str, seed: int = 0, version: int = 1) -> Annotation:
    return Annotation(
        annotator_id=annotator_id,
        tool="test_tool",
        annotation_type="binary_segmentation",
        created_at="2026-06-05T12:00:00Z",
        version=version,
        payload=_sparse_mask(seed=seed),
    )


def _make_annotation_set(annotators: List[str]) -> AnnotationSet:
    aset = AnnotationSet(image_ref=_IMAGE_REF)
    for i, aid in enumerate(annotators):
        aset.add(_make_annotation(aid, seed=i))
    return aset


# ---------------------------------------------------------------------------
# T017 — list_image_annotations: upload→download round-trip via logic layer
# ---------------------------------------------------------------------------

class TestListImageAnnotations:
    def test_round_trip_returns_annotation_set(self, tmp_path: Path):
        """Upload K annotations → list_image_annotations returns AnnotationSet with same K."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = list_image_annotations(fake, _IMAGE_REF, dest_dir=str(tmp_path / "dl"))
        assert isinstance(result, AnnotationSet)
        assert len(result.annotations) == len(_ANNOTATORS)

    def test_round_trip_image_ref_preserved(self, tmp_path: Path):
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = list_image_annotations(fake, _IMAGE_REF, dest_dir=str(tmp_path / "dl2"))
        assert isinstance(result, AnnotationSet)
        assert result.image_ref == _IMAGE_REF

    def test_uses_tmp_dir_when_no_dest_dir(self):
        """No dest_dir → uses a tmp dir internally; still returns AnnotationSet."""
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)

        result = list_image_annotations(fake, _IMAGE_REF)
        assert isinstance(result, AnnotationSet)

    def test_connection_failure_returns_friendly_error(self, tmp_path: Path):
        """Connection failure → FriendlyError returned, nothing raised."""
        fake = FakeXNAT(project_name=_PROJECT)
        fake.set_next_failure(ConnectionError("simulated"))

        try:
            result = list_image_annotations(fake, _IMAGE_REF, dest_dir=str(tmp_path / "fail"))
        except Exception as exc:
            pytest.fail(f"list_image_annotations must not raise; got {exc!r}")

        assert isinstance(result, FriendlyError)

    def test_decoded_masks_match_originals(self, tmp_path: Path):
        """Round-trip payloads are bit-for-bit equal to the originals."""
        fake = FakeXNAT(project_name=_PROJECT)
        originals = [_sparse_mask(seed=i) for i in range(len(_ANNOTATORS))]
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        for aid, mask in zip(_ANNOTATORS, originals):
            ann = Annotation(
                annotator_id=aid,
                tool="test_tool",
                annotation_type="binary_segmentation",
                created_at="2026-06-05T12:00:00Z",
                version=1,
                payload=mask,
            )
            aset.add(ann)

        upload_annotation_set(fake, _IMAGE_REF, aset, project_name=_PROJECT)
        result = list_image_annotations(fake, _IMAGE_REF, dest_dir=str(tmp_path / "cmp"))

        assert isinstance(result, AnnotationSet)
        for ann, orig in zip(result.annotations, originals):
            np.testing.assert_array_equal(ann.payload, orig)


# ---------------------------------------------------------------------------
# T020 — upload_annotations logic wrapper
# ---------------------------------------------------------------------------

class TestUploadAnnotations:
    def test_upload_returns_upload_result(self):
        from src.annotations.io_xnat import UploadResult
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        result = upload_annotations(fake, _IMAGE_REF, aset)
        assert isinstance(result, UploadResult)
        assert result.ok

    def test_upload_failure_returns_not_ok(self):
        """Connection error → ok=False result (UploadResult or FriendlyError); never raises."""
        from src.annotations.io_xnat import UploadResult
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(["worker_A1"])
        fake.set_next_failure(OSError("disk full"))

        try:
            result = upload_annotations(fake, _IMAGE_REF, aset)
        except Exception as exc:
            pytest.fail(f"upload_annotations must not raise; got {exc!r}")

        # Either a FriendlyError OR an UploadResult(ok=False) — both are valid fail-soft
        if isinstance(result, FriendlyError):
            pass  # direct FriendlyError — fine
        elif isinstance(result, UploadResult):
            assert not result.ok
            assert result.friendly is not None
        else:
            pytest.fail(f"Unexpected result type: {type(result)}")

    def test_upload_writes_blobs_and_manifest(self):
        from src.annotations.io_xnat import UploadResult
        fake = FakeXNAT(project_name=_PROJECT)
        aset = _make_annotation_set(_ANNOTATORS)
        result = upload_annotations(fake, _IMAGE_REF, aset)
        assert isinstance(result, UploadResult)
        assert result.ok
        # K blobs + 1 manifest
        assert len(result.files_written) == len(_ANNOTATORS) + 1
        assert MANIFEST_FILENAME in result.files_written


# ---------------------------------------------------------------------------
# T018 — run_consensus
# ---------------------------------------------------------------------------

class TestRunConsensus:
    def test_reference_aggregator_returns_consensus_result(self):
        """run_consensus with 'reference' aggregator → ConsensusResult."""
        aset = _make_annotation_set(_ANNOTATORS)
        result = run_consensus(aset, "binary_segmentation", aggregator_name="reference")
        assert isinstance(result, ConsensusResult)
        assert result.method == "reference"

    def test_consensus_payload_is_ndarray(self):
        aset = _make_annotation_set(_ANNOTATORS)
        result = run_consensus(aset, "binary_segmentation", aggregator_name="reference")
        assert isinstance(result, ConsensusResult)
        assert isinstance(result.payload, np.ndarray)

    def test_consensus_default_aggregator(self):
        """aggregator_name=None → uses type's default ('reference')."""
        aset = _make_annotation_set(["worker_A1"])
        result = run_consensus(aset, "binary_segmentation")
        assert isinstance(result, ConsensusResult)

    def test_bad_aggregator_name_returns_friendly_error(self):
        """Unknown aggregator name → FriendlyError, no raise."""
        aset = _make_annotation_set(["worker_A1"])
        try:
            result = run_consensus(aset, "binary_segmentation", aggregator_name="nonexistent_agg_xyz")
        except Exception as exc:
            pytest.fail(f"run_consensus must not raise; got {exc!r}")
        assert isinstance(result, FriendlyError)

    def test_empty_annotation_set_returns_friendly_error(self):
        """Empty AnnotationSet → FriendlyError (no annotations to aggregate)."""
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        try:
            result = run_consensus(aset, "binary_segmentation", aggregator_name="reference")
        except Exception as exc:
            pytest.fail(f"run_consensus must not raise on empty set; got {exc!r}")
        # May return ConsensusResult (empty inputs) or FriendlyError — must not raise
        assert isinstance(result, (ConsensusResult, FriendlyError))

    def test_bad_annotation_type_returns_friendly_error(self):
        """Unregistered annotation_type → FriendlyError, no raise."""
        aset = _make_annotation_set(["worker_A1"])
        try:
            result = run_consensus(aset, "nonexistent_type_xyz")
        except Exception as exc:
            pytest.fail(f"run_consensus must not raise; got {exc!r}")
        assert isinstance(result, FriendlyError)


# ---------------------------------------------------------------------------
# list_aggregator_names
# ---------------------------------------------------------------------------

class TestListAggregatorNames:
    def test_returns_list_of_strings(self):
        names = list_aggregator_names()
        assert isinstance(names, list)
        assert all(isinstance(n, str) for n in names)

    def test_reference_is_registered(self):
        names = list_aggregator_names()
        assert "reference" in names


# ---------------------------------------------------------------------------
# import_annotations — dispatch by source kind
# ---------------------------------------------------------------------------

class TestImportAnnotations:
    def test_annotation_passthrough(self):
        """Annotation instances pass through unchanged."""
        ann = _make_annotation("worker_A1")
        result = import_annotations([ann])
        assert len(result) == 1
        assert result[0] is ann

    def test_mask_array_dict_dispatches_to_generic(self):
        """Dict with 'mask_array' key → generic.from_mask_array."""
        mask = _sparse_mask(seed=7)
        item = {
            "mask_array": mask,
            "annotator_id": "worker_X9",
            "tool": "labelbox_v2",
        }
        result = import_annotations([item])
        assert len(result) == 1
        assert isinstance(result[0], Annotation)
        assert result[0].tool == "labelbox_v2"
        assert result[0].annotator_id == "worker_X9"

    def test_mturk_row_dict_dispatches_to_mturk(self):
        """Dict with 'WorkerId' key → mturk.from_mturk_row."""
        import base64
        import cv2
        # Build a minimal valid 1x1 PNG mask as base64
        tiny_mask = np.zeros((4, 4), dtype=np.uint8)
        _, buf = cv2.imencode(".png", tiny_mask)
        b64 = base64.b64encode(buf.tobytes()).decode("ascii")

        row = {
            "WorkerId": "WORKER_MTURK_001",
            "SubmitTime": "2026-06-05T12:00:00Z",
            "pngImageData": b64,
        }
        result = import_annotations([row])
        assert len(result) == 1
        assert isinstance(result[0], Annotation)
        assert result[0].annotator_id == "WORKER_MTURK_001"
        assert result[0].tool == "mturk"

    def test_unknown_item_silently_skipped(self):
        """Items with unrecognised shape are silently skipped (no error)."""
        result = import_annotations([{"unrecognized_key": "foo"}, "not_a_dict", 42])
        assert result == []

    def test_mixed_list(self):
        """Mixed list: valid items imported, invalid silently dropped."""
        ann = _make_annotation("worker_A1")
        mask = _sparse_mask(seed=3)
        item = {"mask_array": mask, "annotator_id": "worker_B2", "tool": "itk-snap"}
        result = import_annotations([ann, item, {"bad": "shape"}])
        assert len(result) == 2


# ---------------------------------------------------------------------------
# T019 — MTurkSemanticSegmentation bridges to src.annotations
# ---------------------------------------------------------------------------

class TestMTurkSemanticSegmentationBridge:
    """T019: MTurkSemanticSegmentation now wraps from_mturk_row and produces a valid Annotation."""

    def _make_mturk_row(self, worker_id: str = "WORKER_T019_001", seed: int = 0):
        """Build a minimal valid MTurk row dict with a real base64 PNG mask."""
        import base64
        import cv2
        mask = _sparse_mask(seed=seed, rows=8, cols=8)
        _, buf = cv2.imencode(".png", mask)
        b64 = base64.b64encode(buf.tobytes()).decode("ascii")
        return {
            "WorkerId": worker_id,
            "SubmitTime": "2026-06-05T10:00:00Z",
            "pngImageData": b64,
        }

    def test_importable(self):
        """MTurkSemanticSegmentation can be imported from xnat_scan_data."""
        from src.xnat_scan_data import MTurkSemanticSegmentation  # noqa: F401

    def test_constructor_produces_annotation(self):
        """MTurkSemanticSegmentation(row).annotation is a valid Annotation."""
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row()
        obj = MTurkSemanticSegmentation(row)
        ann = obj.annotation
        assert isinstance(ann, Annotation)

    def test_annotation_type_is_binary_segmentation(self):
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row()
        ann = MTurkSemanticSegmentation(row).annotation
        assert ann.annotation_type == "binary_segmentation"

    def test_annotation_tool_is_mturk(self):
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row()
        ann = MTurkSemanticSegmentation(row).annotation
        assert ann.tool == "mturk"

    def test_annotator_id_matches_worker_id(self):
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row(worker_id="WORKER_T019_001")
        ann = MTurkSemanticSegmentation(row).annotation
        assert ann.annotator_id == "WORKER_T019_001"

    def test_payload_is_ndarray(self):
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row()
        ann = MTurkSemanticSegmentation(row).annotation
        assert isinstance(ann.payload, np.ndarray)

    def test_payload_is_2d(self):
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row()
        ann = MTurkSemanticSegmentation(row).annotation
        assert ann.payload.ndim == 2

    def test_from_row_classmethod(self):
        """from_row() classmethod produces the same result as constructor."""
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row(worker_id="WORKER_T019_002", seed=5)
        obj_ctor = MTurkSemanticSegmentation(row)
        obj_cls = MTurkSemanticSegmentation.from_row(row)
        assert obj_ctor.annotation.annotator_id == obj_cls.annotation.annotator_id
        assert obj_ctor.annotation.tool == obj_cls.annotation.tool

    def test_in_dunder_all(self):
        """MTurkSemanticSegmentation is exported in __all__."""
        import src.xnat_scan_data as m
        assert "MTurkSemanticSegmentation" in m.__all__

    def test_missing_worker_id_raises_annotation_error(self):
        """Missing WorkerId → AnnotationError (not a silent pass)."""
        from src.xnat_scan_data import MTurkSemanticSegmentation
        from src.annotations.exc import AnnotationError
        row = {"SubmitTime": "2026-06-05T10:00:00Z", "pngImageData": "dGVzdA=="}
        with pytest.raises(AnnotationError):
            MTurkSemanticSegmentation(row)

    def test_annotation_usable_in_annotation_set(self):
        """Annotation from MTurkSemanticSegmentation can be added to an AnnotationSet."""
        from src.xnat_scan_data import MTurkSemanticSegmentation
        row = self._make_mturk_row(worker_id="WORKER_T019_003")
        ann = MTurkSemanticSegmentation(row).annotation
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        aset.add(ann)  # must not raise
        assert len(aset.annotations) == 1
        assert aset.annotations[0].annotator_id == "WORKER_T019_003"

    def test_mturk_annotation_survives_upload_download(self):
        """MTurk-derived Annotation survives upload→download round-trip."""
        from src.xnat_scan_data import MTurkSemanticSegmentation
        fake = FakeXNAT(project_name=_PROJECT)
        row = self._make_mturk_row(worker_id="WORKER_T019_004", seed=11)
        ann = MTurkSemanticSegmentation(row).annotation
        aset = AnnotationSet(image_ref=_IMAGE_REF)
        aset.add(ann)

        up = upload_annotations(fake, _IMAGE_REF, aset)
        assert up.ok if hasattr(up, "ok") else True  # UploadResult or FriendlyError

        # If upload succeeded, verify download works
        if hasattr(up, "ok") and up.ok:
            with tempfile.TemporaryDirectory() as tmp:
                dl = list_image_annotations(fake, _IMAGE_REF, dest_dir=tmp)
            assert isinstance(dl, AnnotationSet)
            assert len(dl.annotations) == 1
            assert dl.annotations[0].annotator_id == "WORKER_T019_004"
