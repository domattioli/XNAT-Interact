"""
tests/regression_014/test_s1_gray_img_shape.py -- #33 S1 investigation (spec 014).

S1 (audit-flagged as still-open, then investigated): concern was that
`ds.Rows, ds.Columns = self.image.gray_img.shape` (src/xnat_scan_data.py)
would raise a ValueError on a 3-channel (H, W, 3) image, since a 2-tuple
unpack of a 3-tuple shape fails.

Investigation (this test): ImageHash.__init__ (src/utilities.py:1335-1342)
unconditionally calls _convert_to_grayscale() before any caller can read
gray_img. That method does `np.mean(raw_img, axis=2)` whenever
raw_img.ndim == 3 -- axis=2 is always the channel axis for an (H, W, C)
array regardless of C (3-channel BGR, 4-channel BGRA, etc.), so gray_img
is *always* a 2-D (H, W) array by construction, never 3-D.

Disposition: not-a-bug (see ledger.md) -- this test pins the invariant so
a future change to the grayscale pipeline can't silently reintroduce the
concern the audit raised.

Offline only -- pure numpy, no server, no PHI.
"""
from __future__ import annotations

import numpy as np
import pytest

from src.utilities import ImageHash


@pytest.mark.parametrize("channels", [3, 4])
def test_gray_img_is_always_2d_regardless_of_input_channel_count(channels: int):
    img = np.random.randint(0, 256, size=(10, 12, channels), dtype=np.uint8)
    ih = ImageHash(img=img)

    assert ih.gray_img.ndim == 2, (
        f"gray_img should always be 2D, got shape {ih.gray_img.shape} "
        f"for a {channels}-channel input"
    )
    # The exact unpack used in src/xnat_scan_data.py -- must not raise.
    rows, cols = ih.gray_img.shape
    assert (rows, cols) == (10, 12)


def test_gray_img_2d_input_passes_through_unchanged():
    img = np.random.randint(0, 256, size=(8, 9), dtype=np.uint8)
    ih = ImageHash(img=img)
    assert ih.gray_img.ndim == 2
    rows, cols = ih.gray_img.shape
    assert (rows, cols) == (8, 9)
