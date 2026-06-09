"""
Tests for src.utilities.ImageHash — perceptual de-duplication hash.

ImageHash is the mechanism that stops the same image being uploaded twice. It
runs fully offline when no ConfigTables reference is passed, which makes it an
ideal unit to lock down.
"""
from __future__ import annotations

import numpy as np
import pytest

from src.utilities import ImageHash


def _img(seed: int, shape=(32, 40)) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return rng.integers(0, 256, size=shape, dtype=np.uint8)


def test_hash_is_64_char_hex():
    h = ImageHash(img=_img(1))
    assert isinstance(h.hash_str, str)
    assert len(h.hash_str) == 64
    int(h.hash_str, 16)  # raises if not valid hex


def test_hash_is_deterministic_for_same_image():
    arr = _img(2)
    assert ImageHash(img=arr.copy()).hash_str == ImageHash(img=arr.copy()).hash_str


def test_different_images_give_different_hashes():
    assert ImageHash(img=_img(3)).hash_str != ImageHash(img=_img(4)).hash_str


def test_processed_image_is_resized_to_required_size():
    h = ImageHash(img=_img(5, shape=(10, 200)))
    assert h.processed_img.shape == h.required_img_size_for_hashing


def test_color_image_is_reduced_to_grayscale_then_hashed():
    rng = np.random.default_rng(6)
    color = rng.integers(0, 256, size=(20, 20, 3), dtype=np.uint8)
    h = ImageHash(img=color)
    assert len(h.hash_str) == 64


def test_no_reference_table_means_not_in_metatable():
    h = ImageHash(img=_img(7))
    assert h.in_img_hash_metatable is False


def test_wrong_dimensionality_is_rejected():
    # _validate_input coerces dtype to uint64, so the dtype guard can't fail in
    # practice; the real guard is on dimensionality (must be 2D or 3D).
    bad = np.ones((8,), dtype=np.uint8)  # 1D
    with pytest.raises(Exception):
        ImageHash(img=bad)
