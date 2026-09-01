"""
Tests for pixel-review helpers in src.services.deidentify.

Covers:
  - needs_pixel_review always returns True (conservative placeholder).
  - apply_redaction zeroes exactly the specified box region and leaves the
    rest of the array unchanged.
  - apply_redaction does NOT modify the caller's original array (pure).

All tests are offline, no PHI, no network.
"""
from __future__ import annotations

import numpy as np

from src.services.deidentify import apply_redaction, needs_pixel_review
from tests.synthetic_data import make_burned_in_phi_pixel_array


# --------------------------------------------------------------------------- #
# needs_pixel_review
# --------------------------------------------------------------------------- #

def test_needs_pixel_review_returns_true_for_uint8_array():
    arr = np.zeros((16, 16), dtype=np.uint8)
    assert needs_pixel_review(arr) is True


def test_needs_pixel_review_returns_true_for_uint16_array():
    arr = np.ones((32, 64), dtype=np.uint16) * 4095
    assert needs_pixel_review(arr) is True


def test_needs_pixel_review_returns_true_for_rgb_array():
    arr = np.zeros((64, 64, 3), dtype=np.uint8)
    assert needs_pixel_review(arr) is True


def test_needs_pixel_review_returns_true_for_burned_in_phi_array():
    arr = make_burned_in_phi_pixel_array()
    assert needs_pixel_review(arr) is True


# --------------------------------------------------------------------------- #
# apply_redaction — basic contracts
# --------------------------------------------------------------------------- #

def test_apply_redaction_zeroes_specified_box():
    """Pixels inside the redaction box must all be zero after redaction."""
    arr = make_burned_in_phi_pixel_array(text="DOE^JOHN 01/01/1970")

    # Confirm the rendered text produces at least one non-zero pixel in the
    # region we're about to redact (otherwise the test proves nothing).
    x, y, w, h = 0, 0, arr.shape[1], arr.shape[0]  # full-image box
    assert arr[y : y + h, x : x + w].max() > 0, (
        "Synthetic burned-in array should contain non-zero pixels before redaction"
    )

    result = apply_redaction(arr, [(x, y, w, h)])
    assert result[y : y + h, x : x + w].max() == 0


def test_apply_redaction_leaves_outside_region_unchanged():
    """Pixels *outside* all redaction boxes must be bit-for-bit identical."""
    arr = make_burned_in_phi_pixel_array()
    # Redact only the top quarter row
    rows, cols = arr.shape
    box = (0, 0, cols, rows // 4)

    result = apply_redaction(arr, [box])

    # Bottom three-quarters should be unchanged
    np.testing.assert_array_equal(
        result[rows // 4 :, :],
        arr[rows // 4 :, :],
        err_msg="Pixels outside the redaction box were modified",
    )


def test_apply_redaction_is_pure_does_not_mutate_input():
    """apply_redaction must not modify the caller's array."""
    arr = make_burned_in_phi_pixel_array()
    original = arr.copy()
    _ = apply_redaction(arr, [(0, 0, arr.shape[1], arr.shape[0])])
    np.testing.assert_array_equal(arr, original)


def test_apply_redaction_partial_box_zeroes_only_that_region():
    """Partial redaction box: only that sub-region zeroed."""
    rows, cols = 32, 64
    arr = np.full((rows, cols), fill_value=128, dtype=np.uint8)

    # Redact bottom-right quadrant
    bx, by, bw, bh = cols // 2, rows // 2, cols // 2, rows // 2
    result = apply_redaction(arr, [(bx, by, bw, bh)])

    # Redacted region: all zero
    assert result[by : by + bh, bx : bx + bw].max() == 0
    # Outside: still 128
    assert result[: rows // 2, : cols // 2].min() == 128


def test_apply_redaction_multiple_boxes():
    """Multiple non-overlapping boxes are all zeroed."""
    arr = np.ones((64, 128), dtype=np.uint16) * 1000

    boxes = [(0, 0, 16, 16), (50, 20, 20, 10), (100, 40, 28, 24)]
    result = apply_redaction(arr, boxes)

    for x, y, w, h in boxes:
        assert result[y : y + h, x : x + w].max() == 0, (
            f"Box ({x},{y},{w},{h}) was not fully zeroed"
        )


def test_apply_redaction_with_uint16_array():
    """Redaction works on 16-bit arrays (typical DICOM pixel data)."""
    arr = make_burned_in_phi_pixel_array(dtype=np.uint16)
    # Render text into a uint8 copy to locate a non-zero region
    arr_u8 = make_burned_in_phi_pixel_array()
    rows, cols = arr.shape

    # Scale uint8 values so uint16 array has non-zero content in same region
    arr_u16 = arr_u8.astype(np.uint16) * 16  # scale to 16-bit range
    box = (0, 0, cols, rows)
    result = apply_redaction(arr_u16, [box])
    assert result.max() == 0


def test_apply_redaction_text_region_non_zero_before_redaction():
    """
    Sanity: the burned-in text region must actually be non-zero *before*
    redaction (validates the synthetic generator is working correctly).
    """
    arr = make_burned_in_phi_pixel_array(text="PATIENT NAME 01/02/1980")
    assert arr.max() > 0, (
        "Synthetic burned-in array has no non-zero pixels — cv2.putText may have failed"
    )

    result = apply_redaction(arr, [(0, 0, arr.shape[1], arr.shape[0])])
    assert result.max() == 0, "Full-image redaction should zero all pixels"
