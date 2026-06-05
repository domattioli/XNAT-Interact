"""
tests/test_annotations_core.py — Phase 5 Task A (T005)

Offline, no network, no PHI.

Covers:
  - RLE round-trip exact for binary and multi-label masks
  - RLE blob smaller than raw nbytes (sparse mask)
  - JsonScalar round-trip for landmark and bbox
  - Registry returns the 4 built-in types
  - validate_annotator_id rejects "John Doe"-style; accepts opaque tokens
  - validate_mask_shape rejects shape mismatch
  - AnnotationSet.latest_per_annotator returns highest version per (annotator, type)
"""
from __future__ import annotations

import numpy as np
import pytest

from src.annotations.codecs import RLECodec, JsonScalarCodec, get_codec
from src.annotations.validate import (
    validate_annotator_id,
    validate_mask_shape,
    validate_mask_payload,
    validate_landmark_payload,
    validate_bbox_payload,
)
from src.annotations.registry import list_types, get_type
from src.annotations.model import Annotation, AnnotationSet, ConsensusResult
from src.annotations.exc import AnnotationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sparse_binary_mask(rows: int = 64, cols: int = 64) -> np.ndarray:
    """Sparse binary mask: 5% foreground, rest background."""
    rng = np.random.default_rng(42)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(1, int(rows * cols * 0.05))
    flat_idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[flat_idx] = 1
    return mask


def _multi_label_mask(rows: int = 32, cols: int = 32, n_labels: int = 5) -> np.ndarray:
    """Multi-label mask with n_labels distinct values, moderately sparse."""
    rng = np.random.default_rng(7)
    mask = np.zeros((rows, cols), dtype=np.int32)
    for label in range(1, n_labels + 1):
        n_px = max(1, int(rows * cols * 0.04))
        flat_idx = rng.choice(rows * cols, size=n_px, replace=False)
        mask.ravel()[flat_idx] = label
    return mask


# ---------------------------------------------------------------------------
# RLE codec — round-trip exact
# ---------------------------------------------------------------------------

class TestRLECodecBinary:
    def test_encode_decode_exact(self):
        mask = _sparse_binary_mask()
        blob = RLECodec.encode(mask)
        recovered = RLECodec.decode(blob)
        assert recovered.shape == mask.shape
        assert recovered.dtype == mask.dtype
        np.testing.assert_array_equal(recovered, mask)

    def test_blob_smaller_than_raw(self):
        mask = _sparse_binary_mask()
        blob = RLECodec.encode(mask)
        raw_nbytes = mask.nbytes
        assert len(blob) < raw_nbytes, (
            f"RLE blob ({len(blob)} B) not smaller than raw ({raw_nbytes} B)"
        )

    def test_all_zeros_mask(self):
        mask = np.zeros((16, 16), dtype=np.uint8)
        recovered = RLECodec.decode(RLECodec.encode(mask))
        np.testing.assert_array_equal(recovered, mask)

    def test_all_ones_mask(self):
        mask = np.ones((8, 8), dtype=np.uint8)
        recovered = RLECodec.decode(RLECodec.encode(mask))
        np.testing.assert_array_equal(recovered, mask)

    def test_single_pixel_mask(self):
        mask = np.array([[1]], dtype=np.uint8)
        recovered = RLECodec.decode(RLECodec.encode(mask))
        np.testing.assert_array_equal(recovered, mask)


class TestRLECodecMultiLabel:
    def test_encode_decode_exact(self):
        mask = _multi_label_mask()
        blob = RLECodec.encode(mask)
        recovered = RLECodec.decode(blob)
        assert recovered.shape == mask.shape
        assert recovered.dtype == mask.dtype
        np.testing.assert_array_equal(recovered, mask)

    def test_blob_smaller_than_raw(self):
        mask = _multi_label_mask()
        blob = RLECodec.encode(mask)
        assert len(blob) < mask.nbytes, (
            f"RLE blob ({len(blob)} B) not smaller than raw ({mask.nbytes} B)"
        )

    def test_roundtrip_preserves_label_values(self):
        mask = _multi_label_mask(n_labels=10)
        recovered = RLECodec.decode(RLECodec.encode(mask))
        unique_orig = set(np.unique(mask).tolist())
        unique_rec = set(np.unique(recovered).tolist())
        assert unique_orig == unique_rec

    def test_non_2d_raises(self):
        arr3d = np.zeros((4, 4, 4), dtype=np.uint8)
        with pytest.raises(ValueError, match="2-D"):
            RLECodec.encode(arr3d)

    def test_non_ndarray_raises(self):
        with pytest.raises(ValueError, match="np.ndarray"):
            RLECodec.encode([[1, 0], [0, 1]])


# ---------------------------------------------------------------------------
# JsonScalar codec — round-trip for landmark + bbox
# ---------------------------------------------------------------------------

class TestJsonScalarCodecLandmark:
    def test_roundtrip_int_coords(self):
        lm = {"x": 10, "y": 20}
        recovered = JsonScalarCodec.decode(JsonScalarCodec.encode(lm))
        assert recovered == lm

    def test_roundtrip_float_coords(self):
        lm = {"x": 3.14, "y": 2.718}
        recovered = JsonScalarCodec.decode(JsonScalarCodec.encode(lm))
        assert recovered == lm

    def test_roundtrip_zero_coords(self):
        lm = {"x": 0, "y": 0}
        recovered = JsonScalarCodec.decode(JsonScalarCodec.encode(lm))
        assert recovered == lm


class TestJsonScalarCodecBbox:
    def test_roundtrip(self):
        bb = {"x": 5, "y": 10, "w": 100, "h": 200}
        recovered = JsonScalarCodec.decode(JsonScalarCodec.encode(bb))
        assert recovered == bb

    def test_roundtrip_float_dims(self):
        bb = {"x": 0.5, "y": 1.5, "w": 50.0, "h": 75.0}
        recovered = JsonScalarCodec.decode(JsonScalarCodec.encode(bb))
        assert recovered == bb

    def test_encode_produces_bytes(self):
        payload = {"x": 1, "y": 2, "w": 3, "h": 4}
        blob = JsonScalarCodec.encode(payload)
        assert isinstance(blob, bytes)


# ---------------------------------------------------------------------------
# get_codec lookup
# ---------------------------------------------------------------------------

class TestGetCodec:
    def test_rle_codec_lookup(self):
        codec = get_codec("rle")
        assert codec is RLECodec

    def test_json_scalar_codec_lookup(self):
        codec = get_codec("json_scalar")
        assert codec is JsonScalarCodec

    def test_unknown_codec_raises_friendly_error(self):
        with pytest.raises(AnnotationError):
            get_codec("nonexistent_codec")


# ---------------------------------------------------------------------------
# Registry — 4 built-in types
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_4_builtins_present(self):
        types = list_types()
        assert "binary_segmentation" in types
        assert "label_map" in types
        assert "landmark" in types
        assert "bbox" in types

    def test_binary_segmentation_uses_rle(self):
        entry = get_type("binary_segmentation")
        assert entry.codec == "rle"
        assert entry.default_aggregator == "reference"

    def test_label_map_uses_rle(self):
        entry = get_type("label_map")
        assert entry.codec == "rle"
        assert entry.default_aggregator == "reference"

    def test_landmark_uses_json_scalar(self):
        entry = get_type("landmark")
        assert entry.codec == "json_scalar"
        assert entry.default_aggregator == "reference"

    def test_bbox_uses_json_scalar(self):
        entry = get_type("bbox")
        assert entry.codec == "json_scalar"
        assert entry.default_aggregator == "reference"

    def test_unknown_type_raises_friendly_error(self):
        with pytest.raises(AnnotationError):
            get_type("not_a_real_type")

    def test_type_entry_has_validator(self):
        for tname in ("binary_segmentation", "label_map", "landmark", "bbox"):
            entry = get_type(tname)
            assert callable(entry.validator)

    def test_no_aggregator_class_imported(self):
        """default_aggregator is a string NAME only, not a callable/class."""
        for tname in list_types():
            entry = get_type(tname)
            assert isinstance(entry.default_aggregator, str)
            assert not callable(entry.default_aggregator)


# ---------------------------------------------------------------------------
# validate_annotator_id — PHI guard
# ---------------------------------------------------------------------------

class TestValidateAnnotatorId:
    def test_accepts_worker_token(self):
        validate_annotator_id("worker_A1")  # must not raise

    def test_accepts_hawkid(self):
        validate_annotator_id("hawkid123")  # must not raise

    def test_accepts_hyphenated(self):
        validate_annotator_id("annotator-007")  # must not raise

    def test_accepts_all_digits(self):
        validate_annotator_id("12345")  # must not raise

    def test_rejects_human_name_space(self):
        with pytest.raises(AnnotationError) as exc_info:
            validate_annotator_id("John Doe")
        assert "whitespace" in exc_info.value.message.lower() or \
               "opaque" in exc_info.value.message.lower() or \
               "PHI" in exc_info.value.title

    def test_rejects_first_last_name(self):
        with pytest.raises(AnnotationError):
            validate_annotator_id("Jane Smith")

    def test_rejects_empty_string(self):
        with pytest.raises(AnnotationError):
            validate_annotator_id("")

    def test_rejects_non_string(self):
        with pytest.raises(AnnotationError):
            validate_annotator_id(42)

    def test_rejects_dot_separated(self):
        with pytest.raises(AnnotationError):
            validate_annotator_id("john.doe")

    def test_rejects_slash(self):
        with pytest.raises(AnnotationError):
            validate_annotator_id("user/name")


# ---------------------------------------------------------------------------
# validate_mask_shape — shape mismatch
# ---------------------------------------------------------------------------

class TestValidateMaskShape:
    def test_empty_list_ok(self):
        validate_mask_shape([])  # must not raise

    def test_single_mask_ok(self):
        mask = np.zeros((16, 16), dtype=np.uint8)
        validate_mask_shape([mask])

    def test_matching_shapes_ok(self):
        masks = [np.zeros((16, 16), dtype=np.uint8) for _ in range(3)]
        validate_mask_shape(masks)

    def test_mismatch_raises_friendly_error(self):
        m1 = np.zeros((16, 16), dtype=np.uint8)
        m2 = np.zeros((32, 32), dtype=np.uint8)
        with pytest.raises(AnnotationError) as exc_info:
            validate_mask_shape([m1, m2])
        assert "shape" in exc_info.value.title.lower() or \
               "mismatch" in exc_info.value.title.lower()

    def test_non_2d_mask_raises(self):
        m3d = np.zeros((4, 4, 4), dtype=np.uint8)
        with pytest.raises(AnnotationError):
            validate_mask_shape([m3d])

    def test_non_ndarray_raises(self):
        with pytest.raises(AnnotationError):
            validate_mask_shape([[[0, 1], [1, 0]]])


# ---------------------------------------------------------------------------
# Per-type payload validators
# ---------------------------------------------------------------------------

class TestValidateMaskPayload:
    def test_valid_uint8(self):
        validate_mask_payload(np.zeros((4, 4), dtype=np.uint8))

    def test_valid_int32(self):
        validate_mask_payload(np.zeros((4, 4), dtype=np.int32))

    def test_rejects_float(self):
        with pytest.raises(AnnotationError):
            validate_mask_payload(np.zeros((4, 4), dtype=np.float32))

    def test_rejects_3d(self):
        with pytest.raises(AnnotationError):
            validate_mask_payload(np.zeros((4, 4, 3), dtype=np.uint8))

    def test_rejects_non_array(self):
        with pytest.raises(AnnotationError):
            validate_mask_payload([[0, 1], [1, 0]])


class TestValidateLandmarkPayload:
    def test_valid(self):
        validate_landmark_payload({"x": 5, "y": 10})

    def test_valid_float(self):
        validate_landmark_payload({"x": 1.5, "y": 2.5})

    def test_missing_x(self):
        with pytest.raises(AnnotationError):
            validate_landmark_payload({"y": 10})

    def test_missing_y(self):
        with pytest.raises(AnnotationError):
            validate_landmark_payload({"x": 5})

    def test_non_dict(self):
        with pytest.raises(AnnotationError):
            validate_landmark_payload([5, 10])

    def test_string_coord(self):
        with pytest.raises(AnnotationError):
            validate_landmark_payload({"x": "five", "y": 10})


class TestValidateBboxPayload:
    def test_valid(self):
        validate_bbox_payload({"x": 0, "y": 0, "w": 100, "h": 50})

    def test_valid_float(self):
        validate_bbox_payload({"x": 0.5, "y": 1.5, "w": 10.0, "h": 20.0})

    def test_missing_key(self):
        with pytest.raises(AnnotationError):
            validate_bbox_payload({"x": 0, "y": 0, "w": 10})  # missing h

    def test_non_dict(self):
        with pytest.raises(AnnotationError):
            validate_bbox_payload((0, 0, 10, 20))

    def test_string_value(self):
        with pytest.raises(AnnotationError):
            validate_bbox_payload({"x": 0, "y": 0, "w": "wide", "h": 50})


# ---------------------------------------------------------------------------
# AnnotationSet.latest_per_annotator — versioning
# ---------------------------------------------------------------------------

class TestLatestPerAnnotator:
    def _make_ann(self, annotator: str, ann_type: str, version: int) -> Annotation:
        mask = np.zeros((8, 8), dtype=np.uint8)
        return Annotation(
            annotator_id=annotator,
            tool="test_tool",
            annotation_type=ann_type,
            created_at="2026-06-05T00:00:00Z",
            version=version,
            payload=mask,
        )

    def test_single_annotator_single_version(self):
        aset = AnnotationSet(image_ref="scan_001")
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 1))
        latest = aset.latest_per_annotator()
        assert ("worker_A1", "binary_segmentation") in latest
        assert latest[("worker_A1", "binary_segmentation")].version == 1

    def test_picks_highest_version(self):
        aset = AnnotationSet(image_ref="scan_001")
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 1))
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 3))
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 2))
        latest = aset.latest_per_annotator()
        assert latest[("worker_A1", "binary_segmentation")].version == 3

    def test_multiple_annotators_independent(self):
        aset = AnnotationSet(image_ref="scan_002")
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 2))
        aset.add(self._make_ann("worker_B2", "binary_segmentation", 5))
        latest = aset.latest_per_annotator()
        assert latest[("worker_A1", "binary_segmentation")].version == 2
        assert latest[("worker_B2", "binary_segmentation")].version == 5

    def test_different_types_per_user_kept_separate(self):
        aset = AnnotationSet(image_ref="scan_003")
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 1))
        # Add landmark for same annotator
        lm_ann = Annotation(
            annotator_id="worker_A1",
            tool="test_tool",
            annotation_type="landmark",
            created_at="2026-06-05T00:00:00Z",
            version=2,
            payload={"x": 5, "y": 10},
        )
        aset.annotations.append(lm_ann)  # bypass validate_annotator_id (already tested)
        latest = aset.latest_per_annotator()
        assert ("worker_A1", "binary_segmentation") in latest
        assert ("worker_A1", "landmark") in latest

    def test_by_annotator_groups_correctly(self):
        aset = AnnotationSet(image_ref="scan_004")
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 1))
        aset.add(self._make_ann("worker_A1", "binary_segmentation", 2))
        aset.add(self._make_ann("worker_B2", "binary_segmentation", 1))
        grouped = aset.by_annotator()
        assert len(grouped["worker_A1"]) == 2
        assert len(grouped["worker_B2"]) == 1

    def test_add_rejects_phi_annotator_id(self):
        aset = AnnotationSet(image_ref="scan_005")
        ann = self._make_ann("John Doe", "binary_segmentation", 1)
        with pytest.raises(AnnotationError):
            aset.add(ann)


# ---------------------------------------------------------------------------
# AnnotationSet manifest round-trip (to_manifest / from_manifest)
# ---------------------------------------------------------------------------

class TestAnnotationSetManifest:
    def test_manifest_roundtrip_with_blobs(self):
        aset = AnnotationSet(image_ref="scan_manifest_test")
        mask = _sparse_binary_mask(32, 32)
        ann = Annotation(
            annotator_id="worker_X",
            tool="labelbox",
            annotation_type="binary_segmentation",
            created_at="2026-06-05T10:00:00Z",
            version=1,
            payload=mask,
        )
        ann.encode()
        aset.add(ann)

        manifest = aset.to_manifest()
        assert manifest["image_ref"] == "scan_manifest_test"
        assert len(manifest["annotations"]) == 1

        blobs = {0: ann.blob}
        recovered_set = AnnotationSet.from_manifest(manifest, blobs)
        assert recovered_set.image_ref == "scan_manifest_test"
        assert len(recovered_set.annotations) == 1
        np.testing.assert_array_equal(recovered_set.annotations[0].payload, mask)

    def test_manifest_no_blobs(self):
        aset = AnnotationSet(image_ref="scan_no_blob")
        ann = Annotation(
            annotator_id="worker_Y",
            tool="tool_a",
            annotation_type="landmark",
            created_at="2026-06-05T11:00:00Z",
            version=1,
            payload={"x": 3, "y": 7},
        )
        aset.annotations.append(ann)
        manifest = aset.to_manifest()
        recovered_set = AnnotationSet.from_manifest(manifest)
        assert recovered_set.image_ref == "scan_no_blob"
        # no payload decoded (no blobs)
        assert recovered_set.annotations[0].payload is None


# ---------------------------------------------------------------------------
# ConsensusResult — basic smoke
# ---------------------------------------------------------------------------

class TestConsensusResult:
    def test_construction(self):
        cr = ConsensusResult(
            payload=np.zeros((8, 8), dtype=np.uint8),
            per_annotator={"worker_A1": np.zeros((8, 8), dtype=np.uint8)},
            method="majority_vote",
        )
        assert cr.method == "majority_vote"
        assert "worker_A1" in cr.per_annotator


# ---------------------------------------------------------------------------
# Compression ratio sanity (informational, not a hard failure)
# ---------------------------------------------------------------------------

class TestRLECompressionRatio:
    def test_compression_ratio_printed(self, capsys):
        """Non-failing: prints RLE compression ratio for a sample sparse mask."""
        mask = _sparse_binary_mask(128, 128)
        blob = RLECodec.encode(mask)
        ratio = mask.nbytes / len(blob)
        # Just assert it's a meaningful improvement (>1x) for a sparse mask
        assert ratio > 1.0, f"Expected compression ratio > 1x, got {ratio:.2f}x"
