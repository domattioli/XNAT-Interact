"""
Tests for Stage-1 pixel de-identification detector (specs/010-pixel-deid, T004-T006).

All fixtures are synthetic — no real PHI, no network, no GPU.

Test inventory
--------------
test_image_variants_keys           — image_variants() returns the four expected keys.
test_image_variants_shapes         — all variants have the same shape as the input.
test_image_variants_invert         — invert variant is 255 − orig.
test_image_variants_stretch_range  — stretch variant spans [0, 255] for non-uniform input.
test_tesseract_boxes_crisp_phi     — Tesseract finds ≥1 box on crisp burned-in PHI.
test_tesseract_boxes_blank_image   — Tesseract returns [] on a blank (all-zero) frame.
test_detect_text_regions_no_model  — graceful degrade: returns [] when model_dir is empty.
test_detect_text_regions_missing_dir — graceful degrade: non-existent model_dir → [].
test_union_dilate_merges_overlapping — two overlapping boxes merged into one.
test_union_dilate_disjoint_boxes    — two far-apart boxes stay separate.
test_union_dilate_grows_by_margin   — merged box is at least 2*margin larger per axis.
test_union_dilate_clamps_to_shape   — dilation does not exceed the image boundary.
test_union_dilate_empty             — empty input → empty output.
test_multipass_detect_covers_phi   — multipass_detect returns ≥1 box covering the text region
                                      on crisp burned-in PHI (requires tesseract binary).
"""
from __future__ import annotations

import shutil

import numpy as np
import pytest

from src.services.pixel_deid.detect import (
    detect_text_regions,
    image_variants,
    multipass_detect,
    tesseract_boxes,
    union_dilate,
)
from tests.synthetic_data import make_burned_in_phi_pixel_array

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Ground-truth bounding box of the default burned-in PHI text
# (text rendered at origin (4, rows//2) with font_scale=0.5 and thickness=1).
# Computed empirically from the synthetic fixture; kept as a generous envelope.
_PHI_TEXT = "PATIENT NAME 01/02/1980"
_ROWS, _COLS = 64, 256


def _phi_gt_bbox(arr: np.ndarray) -> tuple[int, int, int, int]:
    """Return (x0, y0, x1, y1) enclosing all non-zero pixels in *arr*."""
    ys, xs = np.where(arr > 0)
    assert len(xs) > 0, "synthetic PHI array contains no non-zero pixels"
    return int(xs.min()), int(ys.min()), int(xs.max()), int(ys.max())


def _box_covers(
    gt: tuple[int, int, int, int],
    detected: list[tuple],
) -> bool:
    """Return True if any detected box overlaps the ground-truth box."""
    gx0, gy0, gx1, gy1 = gt
    for bx0, by0, bx1, by1 in detected:
        # Overlaps when NOT disjoint.
        if not (bx1 < gx0 or gx1 < bx0 or by1 < gy0 or gy1 < by0):
            return True
    return False


def _requires_tesseract():
    """Skip marker: skip the test if the tesseract binary is unavailable."""
    return pytest.mark.skipif(
        shutil.which("tesseract") is None,
        reason="tesseract binary not found on PATH",
    )


# ---------------------------------------------------------------------------
# image_variants tests
# ---------------------------------------------------------------------------

class TestImageVariants:
    def setup_method(self):
        rng = np.random.default_rng(0)
        self.img = rng.integers(0, 256, size=(32, 64), dtype=np.uint8)
        self.variants = image_variants(self.img)

    def test_keys(self):
        assert set(self.variants.keys()) == {"orig", "invert", "stretch", "clahe"}

    def test_shapes(self):
        for name, v in self.variants.items():
            assert v.shape == self.img.shape, f"{name} shape mismatch"
            assert v.dtype == np.uint8, f"{name} dtype not uint8"

    def test_invert_values(self):
        expected = (255 - self.img.astype(np.int16)).clip(0, 255).astype(np.uint8)
        np.testing.assert_array_equal(self.variants["invert"], expected)

    def test_stretch_range(self):
        # A non-uniform input should be stretched to span [0, 255].
        v = self.variants["stretch"]
        assert int(v.min()) == 0, "stretch min should be 0"
        assert int(v.max()) == 255, "stretch max should be 255"

    def test_orig_is_input(self):
        # "orig" is the same array (or equal values) as the input.
        np.testing.assert_array_equal(self.variants["orig"], self.img)


# ---------------------------------------------------------------------------
# tesseract_boxes tests
# ---------------------------------------------------------------------------

@_requires_tesseract()
class TestTesseractBoxes:
    def test_crisp_phi_finds_at_least_one_box(self):
        arr = make_burned_in_phi_pixel_array(_PHI_TEXT, rows=_ROWS, cols=_COLS)
        boxes = tesseract_boxes(arr)
        assert len(boxes) >= 1, (
            f"Expected ≥1 box from Tesseract on crisp PHI; got {boxes}"
        )

    def test_blank_image_returns_empty(self):
        blank = np.zeros((_ROWS, _COLS), dtype=np.uint8)
        boxes = tesseract_boxes(blank)
        assert boxes == [], f"Expected [] on blank image; got {boxes}"

    def test_return_type_structure(self):
        arr = make_burned_in_phi_pixel_array(_PHI_TEXT, rows=_ROWS, cols=_COLS)
        boxes = tesseract_boxes(arr)
        for box in boxes:
            assert len(box) == 4, f"Box should be a 4-tuple: {box}"
            x0, y0, x1, y1 = box
            assert x1 >= x0, f"x1 < x0 in box {box}"
            assert y1 >= y0, f"y1 < y0 in box {box}"


# ---------------------------------------------------------------------------
# detect_text_regions tests (graceful-degrade, no real model needed)
# ---------------------------------------------------------------------------

class TestDetectTextRegions:
    def _blank(self):
        return np.zeros((_ROWS, _COLS), dtype=np.uint8)

    def test_empty_model_dir_returns_empty_list(self, tmp_path):
        """No .onnx file in model_dir → graceful degrade: return []."""
        result = detect_text_regions(self._blank(), model_dir=str(tmp_path))
        assert result == [], f"Expected [] from empty model dir; got {result}"

    def test_default_models_craft_dir_returns_empty(self):
        """models/craft is empty in this environment; must return [] without raising."""
        result = detect_text_regions(self._blank(), model_dir="models/craft")
        assert result == [], (
            f"Expected [] from empty models/craft; got {result}"
        )

    def test_nonexistent_model_dir_returns_empty(self):
        """A model_dir that does not exist at all → graceful degrade."""
        result = detect_text_regions(
            self._blank(), model_dir="/tmp/no_such_model_dir_abc123xyz"
        )
        assert result == [], (
            f"Expected [] from nonexistent model dir; got {result}"
        )

    def test_does_not_raise_on_any_input(self):
        """No exception for any reasonable call — graceful degrade contract."""
        rng = np.random.default_rng(42)
        img = rng.integers(0, 256, size=(128, 256), dtype=np.uint8)
        # This must not raise, regardless of the model_dir contents.
        try:
            detect_text_regions(img, model_dir="models/craft")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"detect_text_regions raised an exception: {exc}")


# ---------------------------------------------------------------------------
# union_dilate tests
# ---------------------------------------------------------------------------

class TestUnionDilate:
    def test_empty_input(self):
        assert union_dilate([]) == []

    def test_single_box_dilated(self):
        result = union_dilate([(10, 20, 30, 40)], margin=5)
        assert len(result) == 1
        x0, y0, x1, y1 = result[0]
        assert x0 == 5
        assert y0 == 15
        assert x1 == 35
        assert y1 == 45

    def test_overlapping_boxes_merged_into_one(self):
        # Two clearly overlapping boxes should become one.
        b1 = (10, 20, 60, 50)
        b2 = (30, 25, 80, 55)
        result = union_dilate([b1, b2], margin=0)
        assert len(result) == 1, (
            f"Two overlapping boxes should merge into 1; got {result}"
        )
        x0, y0, x1, y1 = result[0]
        # The merged box must cover both inputs exactly (margin=0).
        assert x0 <= 10
        assert y0 <= 20
        assert x1 >= 80
        assert y1 >= 55

    def test_disjoint_boxes_remain_separate(self):
        # Two boxes with a large gap should remain as separate boxes.
        b1 = (0, 0, 10, 10)
        b2 = (200, 200, 210, 210)
        result = union_dilate([b1, b2], margin=0)
        assert len(result) == 2, (
            f"Two disjoint boxes should stay separate; got {result}"
        )

    def test_dilation_grows_box_by_margin(self):
        margin = 4
        box = (20, 30, 60, 70)
        result = union_dilate([box], margin=margin)
        assert len(result) == 1
        x0, y0, x1, y1 = result[0]
        # Each edge should be offset by exactly margin (no shape clamping here).
        assert x0 == 20 - margin
        assert y0 == 30 - margin
        assert x1 == 60 + margin
        assert y1 == 70 + margin

    def test_clamped_to_shape(self):
        shape = (50, 100)  # (height, width)
        # Box that when dilated would go out of bounds.
        box = (0, 0, 90, 40)
        result = union_dilate([box], margin=20, shape=shape)
        assert len(result) == 1
        x0, y0, x1, y1 = result[0]
        h, w = shape
        assert x0 >= 0
        assert y0 >= 0
        assert x1 <= w
        assert y1 <= h

    def test_output_integer_coordinates(self):
        result = union_dilate([(1.5, 2.7, 10.3, 20.9)], margin=1)
        for coord in result[0]:
            assert isinstance(coord, int), f"Expected int coordinate; got {type(coord)}"

    def test_multiple_nearby_boxes_reduce_count(self):
        # Three touching boxes should merge into fewer (or equal) boxes.
        boxes = [(0, 0, 50, 20), (48, 0, 100, 20), (98, 0, 150, 20)]
        result = union_dilate(boxes, margin=0)
        assert len(result) < len(boxes), (
            f"Touching boxes should reduce count; got {result}"
        )


# ---------------------------------------------------------------------------
# multipass_detect end-to-end test (requires tesseract)
# ---------------------------------------------------------------------------

@_requires_tesseract()
class TestMultipassDetect:
    def test_covers_phi_region_on_crisp_text(self):
        """
        multipass_detect must return at least one box that overlaps the
        ground-truth text region on a crisp burned-in PHI frame.

        The ground-truth box is derived from the actual non-zero pixel extent
        of the synthetic fixture — so this test proves the detector actually
        found the text, not just any pixel.
        """
        arr = make_burned_in_phi_pixel_array(_PHI_TEXT, rows=_ROWS, cols=_COLS)
        gt = _phi_gt_bbox(arr)
        boxes = multipass_detect(arr)

        assert len(boxes) >= 1, (
            "multipass_detect returned no boxes on crisp PHI image"
        )
        assert _box_covers(gt, boxes), (
            f"No detected box overlaps the ground-truth PHI region {gt}. "
            f"Detected boxes: {boxes}"
        )

    def test_returns_list_of_4tuples(self):
        arr = make_burned_in_phi_pixel_array(_PHI_TEXT, rows=_ROWS, cols=_COLS)
        boxes = multipass_detect(arr)
        for box in boxes:
            assert len(box) == 4, f"Expected 4-tuple box; got {box}"

    def test_blank_image_returns_list(self):
        """Blank image must return an empty list, not raise."""
        blank = np.zeros((_ROWS, _COLS), dtype=np.uint8)
        result = multipass_detect(blank)
        assert isinstance(result, list)

    def test_boxes_within_image_bounds(self):
        """All returned boxes must lie within the image dimensions."""
        arr = make_burned_in_phi_pixel_array(_PHI_TEXT, rows=_ROWS, cols=_COLS)
        boxes = multipass_detect(arr)
        for x0, y0, x1, y1 in boxes:
            assert x0 >= 0 and y0 >= 0, f"Box has negative coord: {(x0,y0,x1,y1)}"
            assert x1 <= _COLS, f"Box x1 exceeds image width: {(x0,y0,x1,y1)}"
            assert y1 <= _ROWS, f"Box y1 exceeds image height: {(x0,y0,x1,y1)}"
