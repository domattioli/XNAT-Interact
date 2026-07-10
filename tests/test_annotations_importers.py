"""
tests/test_annotations_importers.py — Phase 5 Task C (T010-T013)

Offline, no network, no PHI.

Covers:
  T010 — generic.from_mask_array: type inference, valid Annotation, errors
  T011 — mturk.from_mturk_row: base64 synth mask, annotator_id=WorkerId, decoded mask match
  T012 — dicom_seg round-trip: 2-segment SEG → from_dicom_seg → 2 Annotations → to_dicom_seg → 2 segments
  T013 — error paths: garbage/unknown input → AnnotationError (no raw traceback), human-name annotator_id rejected
"""
from __future__ import annotations

import base64
import io
from typing import Any

import cv2
import numpy as np
import pytest
import pydicom
from pydicom.dataset import Dataset, FileDataset, FileMetaDataset
from pydicom.sequence import Sequence
from pydicom.uid import (
    ExplicitVRLittleEndian,
    SegmentationStorage,
    generate_uid,
)

from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation
from src.annotations.importers.generic import from_mask_array
from src.annotations.importers.mturk import from_mturk_row
from src.annotations.importers.dicom_seg import from_dicom_seg, to_dicom_seg, _pack_binary_frames, _unpack_binary_frame


# ---------------------------------------------------------------------------
# Synthetic data helpers (added here per spec — only additive)
# ---------------------------------------------------------------------------

def _make_binary_mask(rows: int = 32, cols: int = 32, seed: int = 0) -> np.ndarray:
    """Sparse binary uint8 mask (0/1) for tests."""
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(4, rows * cols // 10)
    idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[idx] = 1
    return mask


def _make_label_mask(rows: int = 32, cols: int = 32, n_labels: int = 4, seed: int = 1) -> np.ndarray:
    """Multi-label int32 mask with n_labels distinct non-zero values."""
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.int32)
    for label in range(1, n_labels + 1):
        n_px = max(2, rows * cols // 20)
        idx = rng.choice(rows * cols, size=n_px, replace=False)
        mask.ravel()[idx] = label
    return mask


def _encode_mask_as_base64_png(mask: np.ndarray) -> str:
    """Encode a 2-D uint8 numpy array as a base64 PNG string (like MTurk pngImageData)."""
    ok, buf = cv2.imencode(".png", mask)
    assert ok, "cv2.imencode failed in test helper"
    return base64.b64encode(buf.tobytes()).decode("ascii")


def _make_minimal_dicom_seg(
    masks: list[np.ndarray],
    labels: list[str],
    rows: int = 32,
    cols: int = 32,
) -> Dataset:
    """
    Build a minimal DICOM SEG Dataset with one frame per label.

    Uses to_dicom_seg internally so the round-trip test is symmetric.
    This helper is separate so unit tests can also build SEGs directly.
    """
    annotations = []
    for i, (mask, label) in enumerate(zip(masks, labels), start=1):
        annotations.append(Annotation(
            annotator_id=label,
            tool="test",
            annotation_type="binary_segmentation",
            created_at="2026-06-05T00:00:00Z",
            version=i,
            payload=mask,
        ))

    ref = Dataset()
    ref.Rows = rows
    ref.Columns = cols
    return to_dicom_seg(annotations, ref)


# ===========================================================================
# T010 — generic.from_mask_array
# ===========================================================================

class TestGenericFromMaskArray:

    def test_binary_mask_infers_binary_segmentation(self):
        mask = _make_binary_mask()
        ann = from_mask_array(mask, annotator_id="worker_A1", tool="itk-snap")
        assert ann.annotation_type == "binary_segmentation"

    def test_label_mask_infers_label_map(self):
        mask = _make_label_mask()
        ann = from_mask_array(mask, annotator_id="worker_A1", tool="itk-snap")
        assert ann.annotation_type == "label_map"

    def test_single_value_mask_infers_binary_segmentation(self):
        # All zeros — 1 unique value ≤ 2 → binary_segmentation
        mask = np.zeros((8, 8), dtype=np.uint8)
        ann = from_mask_array(mask, annotator_id="worker_A1", tool="test")
        assert ann.annotation_type == "binary_segmentation"

    def test_explicit_type_respected(self):
        mask = _make_binary_mask()
        ann = from_mask_array(
            mask,
            annotator_id="worker_A1",
            tool="test",
            annotation_type="binary_segmentation",
        )
        assert ann.annotation_type == "binary_segmentation"

    def test_returns_annotation_with_correct_fields(self):
        mask = _make_binary_mask()
        ann = from_mask_array(
            mask,
            annotator_id="worker_X9",
            tool="itk-snap",
            created_at="2026-06-05T12:00:00Z",
            version=3,
        )
        assert isinstance(ann, Annotation)
        assert ann.annotator_id == "worker_X9"
        assert ann.tool == "itk-snap"
        assert ann.created_at == "2026-06-05T12:00:00Z"
        assert ann.version == 3
        assert ann.payload is mask

    def test_default_created_at_set(self):
        mask = _make_binary_mask()
        ann = from_mask_array(mask, annotator_id="worker_A1", tool="t")
        assert ann.created_at is not None
        assert "T" in ann.created_at  # ISO-8601

    def test_float_array_raises(self):
        mask = np.ones((8, 8), dtype=np.float32)
        with pytest.raises(AnnotationError) as exc_info:
            from_mask_array(mask, annotator_id="worker_A1", tool="t")
        assert "dtype" in str(exc_info.value).lower() or "integer" in str(exc_info.value).lower()

    def test_3d_array_raises(self):
        mask = np.zeros((8, 8, 3), dtype=np.uint8)
        with pytest.raises(AnnotationError):
            from_mask_array(mask, annotator_id="worker_A1", tool="t")

    def test_bad_annotator_id_raises(self):
        mask = _make_binary_mask()
        with pytest.raises(AnnotationError):
            from_mask_array(mask, annotator_id="John Doe", tool="t")

    def test_unknown_annotation_type_raises(self):
        mask = _make_binary_mask()
        with pytest.raises(AnnotationError):
            from_mask_array(mask, annotator_id="worker_A1", tool="t", annotation_type="nonexistent_type")

    def test_non_mask_type_for_mask_array_raises(self):
        mask = _make_binary_mask()
        with pytest.raises(AnnotationError) as exc_info:
            from_mask_array(mask, annotator_id="worker_A1", tool="t", annotation_type="bbox")
        assert "codec" in str(exc_info.value).lower() or "rle" in str(exc_info.value).lower()


# ===========================================================================
# T011 — mturk.from_mturk_row
# ===========================================================================

class TestMturkFromRow:

    def _make_row(
        self,
        *,
        worker_id: str = "WORKER123",
        submit_time: str = "2026-06-05T10:00:00Z",
        mask: np.ndarray | None = None,
        png_key: str = "Answer.annotatedResult.pngImageData",
    ) -> dict:
        if mask is None:
            mask = _make_binary_mask(seed=5)
        b64 = _encode_mask_as_base64_png(mask)
        return {
            "WorkerId": worker_id,
            "HITId": "HIT_OPAQUE_001",
            "AssignmentId": "ASSIGN_OPAQUE_001",
            "SubmitTime": submit_time,
            png_key: b64,
        }

    def test_returns_annotation_with_worker_id_as_annotator(self):
        row = self._make_row(worker_id="WORKER_ABC")
        ann = from_mturk_row(row)
        assert ann.annotator_id == "WORKER_ABC"

    def test_tool_is_mturk(self):
        ann = from_mturk_row(self._make_row())
        assert ann.tool == "mturk"

    def test_annotation_type_is_binary_segmentation(self):
        ann = from_mturk_row(self._make_row())
        assert ann.annotation_type == "binary_segmentation"

    def test_created_at_from_submit_time(self):
        ann = from_mturk_row(self._make_row(submit_time="2026-06-05T10:00:00Z"))
        assert ann.created_at == "2026-06-05T10:00:00Z"

    def test_decoded_mask_matches_original(self):
        original_mask = _make_binary_mask(seed=7)
        row = self._make_row(mask=original_mask)
        ann = from_mturk_row(row)
        # cv2.imdecode on a binary mask may expand 0/1 → 0/255 or keep as-is.
        # Binarise both for comparison.
        decoded = (ann.payload > 0).astype(np.uint8)
        expected = (original_mask > 0).astype(np.uint8)
        np.testing.assert_array_equal(decoded, expected)

    def test_exact_pngImageData_key_accepted(self):
        mask = _make_binary_mask()
        b64 = _encode_mask_as_base64_png(mask)
        row = {"WorkerId": "WORKER_XY", "SubmitTime": "2026-06-05T00:00:00Z", "pngImageData": b64}
        ann = from_mturk_row(row)
        assert ann.annotator_id == "WORKER_XY"

    def test_missing_worker_id_raises(self):
        mask = _make_binary_mask()
        b64 = _encode_mask_as_base64_png(mask)
        row = {"SubmitTime": "2026-06-05T00:00:00Z", "pngImageData": b64}
        with pytest.raises(AnnotationError) as exc_info:
            from_mturk_row(row)
        assert "WorkerId" in str(exc_info.value)

    def test_missing_png_field_raises(self):
        row = {"WorkerId": "WORKER_XY", "SubmitTime": "2026-06-05T00:00:00Z"}
        with pytest.raises(AnnotationError) as exc_info:
            from_mturk_row(row)
        assert "pngImageData" in str(exc_info.value)

    def test_bad_base64_raises(self):
        row = {
            "WorkerId": "WORKER_XY",
            "SubmitTime": "2026-06-05T00:00:00Z",
            "pngImageData": "NOT_VALID_BASE64!!!",
        }
        with pytest.raises(AnnotationError) as exc_info:
            from_mturk_row(row)
        assert "base64" in str(exc_info.value).lower() or "decode" in str(exc_info.value).lower()

    def test_invalid_png_bytes_raises(self):
        # Valid base64 but not a PNG
        garbage_b64 = base64.b64encode(b"this is not a png").decode()
        row = {
            "WorkerId": "WORKER_XY",
            "SubmitTime": "2026-06-05T00:00:00Z",
            "pngImageData": garbage_b64,
        }
        with pytest.raises(AnnotationError):
            from_mturk_row(row)

    def test_human_name_worker_id_raises(self):
        mask = _make_binary_mask()
        b64 = _encode_mask_as_base64_png(mask)
        row = {
            "WorkerId": "John Doe",  # space → rejected
            "SubmitTime": "2026-06-05T00:00:00Z",
            "pngImageData": b64,
        }
        with pytest.raises(AnnotationError) as exc_info:
            from_mturk_row(row)
        # Should mention PHI or annotator_id
        err_str = str(exc_info.value)
        assert "annotator_id" in err_str.lower() or "phi" in err_str.lower() or "whitespace" in err_str.lower()


# ===========================================================================
# T012 — dicom_seg round-trip
# ===========================================================================

class TestDicomSegRoundTrip:

    def _make_two_segment_seg(self) -> tuple[Dataset, list[np.ndarray], list[str]]:
        """Build a 2-segment SEG and return (ds, original_masks, labels)."""
        mask_a = _make_binary_mask(seed=10)
        mask_b = _make_binary_mask(seed=11)
        labels = ["worker_A1", "worker_B2"]
        ds = _make_minimal_dicom_seg([mask_a, mask_b], labels)
        return ds, [mask_a, mask_b], labels

    def test_from_dicom_seg_returns_two_annotations(self):
        ds, _, _ = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        assert len(anns) == 2

    def test_annotator_ids_match_segment_labels(self):
        ds, _, labels = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        result_ids = [a.annotator_id for a in anns]
        assert result_ids == labels

    def test_annotation_type_binary_segmentation(self):
        ds, _, _ = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        for ann in anns:
            assert ann.annotation_type == "binary_segmentation"

    def test_tool_is_dicom_seg(self):
        ds, _, _ = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        for ann in anns:
            assert ann.tool == "dicom_seg"

    def test_masks_binarised_correctly(self):
        ds, original_masks, _ = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        for ann, orig in zip(anns, original_masks):
            decoded = (ann.payload > 0).astype(np.uint8)
            expected = (orig > 0).astype(np.uint8)
            np.testing.assert_array_equal(decoded, expected)

    def test_to_dicom_seg_produces_correct_segment_count(self):
        ds, _, _ = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        ref = Dataset()
        ref.Rows = 32
        ref.Columns = 32
        out_ds = to_dicom_seg(anns, ref)
        assert len(out_ds.SegmentSequence) == 2

    def test_round_trip_segment_labels_preserved(self):
        ds, _, labels = self._make_two_segment_seg()
        anns = from_dicom_seg(ds)
        ref = Dataset()
        ref.Rows = 32
        ref.Columns = 32
        out_ds = to_dicom_seg(anns, ref)
        round_trip_labels = [str(seg.SegmentLabel) for seg in out_ds.SegmentSequence]
        assert round_trip_labels == labels

    def test_full_round_trip_masks(self):
        """to_dicom_seg → from_dicom_seg → masks match originals."""
        mask_a = _make_binary_mask(seed=20)
        mask_b = _make_binary_mask(seed=21)
        labels = ["annotator_X", "annotator_Y"]
        ds = _make_minimal_dicom_seg([mask_a, mask_b], labels, rows=32, cols=32)
        anns = from_dicom_seg(ds)
        assert len(anns) == 2
        for ann, orig in zip(anns, [mask_a, mask_b]):
            decoded = (ann.payload > 0).astype(np.uint8)
            expected = (orig > 0).astype(np.uint8)
            np.testing.assert_array_equal(decoded, expected)

    def test_to_dicom_seg_empty_list_raises(self):
        ref = Dataset()
        ref.Rows = 32
        ref.Columns = 32
        with pytest.raises(AnnotationError) as exc_info:
            to_dicom_seg([], ref)
        assert "empty" in str(exc_info.value).lower()

    def test_to_dicom_seg_wrong_type_raises(self):
        ann = Annotation(
            annotator_id="worker_A1",
            tool="test",
            annotation_type="label_map",
            created_at="2026-06-05T00:00:00Z",
            version=1,
            payload=np.zeros((8, 8), dtype=np.int32),
        )
        ref = Dataset()
        ref.Rows = 8
        ref.Columns = 8
        with pytest.raises(AnnotationError) as exc_info:
            to_dicom_seg([ann], ref)
        assert "binary_segmentation" in str(exc_info.value)

    def test_from_dicom_seg_no_segment_sequence_raises(self):
        ds = Dataset()
        ds.Rows = 32
        ds.Columns = 32
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        assert "SegmentSequence" in str(exc_info.value)

    def test_from_dicom_seg_fractional_type_raises(self):
        ds, _, _ = self._make_two_segment_seg()
        ds.SegmentationType = "FRACTIONAL"
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        assert "FRACTIONAL" in str(exc_info.value)

    def test_from_dicom_seg_empty_segment_label_raises(self):
        mask_a = _make_binary_mask(seed=30)
        # Build SEG manually with an empty SegmentLabel
        ref = Dataset()
        ref.Rows = 32
        ref.Columns = 32
        ann = Annotation(
            annotator_id="worker_A1",
            tool="test",
            annotation_type="binary_segmentation",
            created_at="2026-06-05T00:00:00Z",
            version=1,
            payload=mask_a,
        )
        ds = to_dicom_seg([ann], ref)
        # Tamper: blank out the first SegmentLabel
        ds.SegmentSequence[0].SegmentLabel = ""
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        assert "SegmentLabel" in str(exc_info.value)


# ===========================================================================
# T013 — error paths: garbage input, human-name annotator_id
# ===========================================================================

class TestErrorPaths:

    def test_generic_garbage_input_raises_annotation_error_not_raw(self):
        """Non-array input → AnnotationError, not TypeError/ValueError."""
        with pytest.raises(AnnotationError):
            from_mask_array("not an array", annotator_id="worker_A1", tool="t")  # type: ignore

    def test_mturk_none_row_raises_annotation_error(self):
        with pytest.raises((AnnotationError, AttributeError, TypeError)):
            # None is not a Mapping — implementation may raise AttributeError or AnnotationError
            from_mturk_row(None)  # type: ignore

    def test_mturk_empty_dict_raises_annotation_error(self):
        with pytest.raises(AnnotationError):
            from_mturk_row({})

    def test_dicom_seg_non_dataset_raises(self):
        """Passing a non-Dataset (e.g. dict) → AnnotationError."""
        with pytest.raises(AnnotationError):
            from_dicom_seg({})  # type: ignore

    def test_generic_human_name_annotator_id_rejected(self):
        mask = _make_binary_mask()
        with pytest.raises(AnnotationError) as exc_info:
            from_mask_array(mask, annotator_id="Jane Smith", tool="t")
        err = str(exc_info.value)
        assert "annotator_id" in err.lower() or "phi" in err.lower() or "whitespace" in err.lower()

    def test_dicom_seg_human_name_segment_label_rejected(self):
        mask = _make_binary_mask()
        ref = Dataset()
        ref.Rows = 32
        ref.Columns = 32
        ann = Annotation(
            annotator_id="worker_A1",
            tool="test",
            annotation_type="binary_segmentation",
            created_at="2026-06-05T00:00:00Z",
            version=1,
            payload=mask,
        )
        ds = to_dicom_seg([ann], ref)
        # Tamper: inject a human name as SegmentLabel
        ds.SegmentSequence[0].SegmentLabel = "John Smith"
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        err = str(exc_info.value)
        assert "annotator_id" in err.lower() or "phi" in err.lower() or "whitespace" in err.lower()

    def test_annotation_error_no_raw_traceback_in_message(self):
        """AnnotationError.message must not expose raw Python tracebacks."""
        mask = _make_binary_mask()
        try:
            from_mask_array(mask, annotator_id="Bad Name", tool="t")
        except AnnotationError as e:
            # FriendlyError.message should not contain 'Traceback'
            assert "Traceback" not in e.message
            assert "Traceback" not in e.title


# ===========================================================================
# T010-extra — pack/unpack helpers
# ===========================================================================

class TestPackUnpackHelpers:

    def test_pack_unpack_roundtrip(self):
        mask = _make_binary_mask(rows=8, cols=8, seed=99)
        packed = _pack_binary_frames([mask], 8, 8)
        unpacked = _unpack_binary_frame(packed, 8, 8, 0)
        np.testing.assert_array_equal(unpacked, (mask > 0).astype(np.uint8))

    def test_pack_unpack_two_frames(self):
        mask_a = _make_binary_mask(rows=8, cols=8, seed=3)
        mask_b = _make_binary_mask(rows=8, cols=8, seed=4)
        packed = _pack_binary_frames([mask_a, mask_b], 8, 8)
        up_a = _unpack_binary_frame(packed, 8, 8, 0)
        up_b = _unpack_binary_frame(packed, 8, 8, 1)
        np.testing.assert_array_equal(up_a, (mask_a > 0).astype(np.uint8))
        np.testing.assert_array_equal(up_b, (mask_b > 0).astype(np.uint8))


# ===========================================================================
# #55 — SegmentationType read per-segment, not only at the dataset root.
# Per the DICOM-SEG IOD SegmentationType may vary per segment; a dataset-level
# only read misses a per-segment FRACTIONAL type and would return a garbage
# binary mask instead of refusing.
# ===========================================================================

class TestPerSegmentFractionalGuard:

    def _two_segment_seg(self) -> Dataset:
        masks = [_make_binary_mask(seed=20), _make_binary_mask(seed=21)]
        return _make_minimal_dicom_seg(masks, ["worker_A1", "worker_B2"])

    def test_per_segment_fractional_raises(self):
        """A per-segment FRACTIONAL override must be caught even when the dataset
        root has no (or a BINARY) SegmentationType — the #55 regression."""
        ds = self._two_segment_seg()
        # Dataset root stays BINARY / unset; only the second segment is FRACTIONAL.
        ds.SegmentSequence[1].SegmentationType = "FRACTIONAL"
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        assert "FRACTIONAL" in str(exc_info.value)

    def test_dataset_level_fractional_still_raises(self):
        """Regression guard: a dataset-level FRACTIONAL type (segments carry
        none → fall back to the root value) must still be refused."""
        ds = self._two_segment_seg()
        ds.SegmentationType = "FRACTIONAL"
        with pytest.raises(AnnotationError) as exc_info:
            from_dicom_seg(ds)
        assert "FRACTIONAL" in str(exc_info.value)

    def test_per_segment_binary_override_allows_import(self):
        """A segment that explicitly declares BINARY is honoured over a
        FRACTIONAL dataset root (the fallback only applies when unset)."""
        ds = self._two_segment_seg()
        ds.SegmentationType = "FRACTIONAL"
        for seg in ds.SegmentSequence:
            seg.SegmentationType = "BINARY"
        anns = from_dicom_seg(ds)
        assert len(anns) == 2
