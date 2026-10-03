"""
Offline checks of the three live-XNAT synthetic cases (spec 014, FR-003).

These run without a server. They prove the fixture composition is fixed and
deterministic, so the live suite's repeat-run comparison (SC-008) is meaningful.
"""
from __future__ import annotations

import hashlib

import numpy as np
import pydicom
import pytest

from tests.synthetic_data import (
    make_hip_2024_case,
    make_knee_2025_case,
    make_radiofluoro_2026_case,
)

BUILDERS = {
    "KNEE_2025": (make_knee_2025_case, 65),
    "HIP_2024": (make_hip_2024_case, 20),
    "RADIOFLUORO_2026": (make_radiofluoro_2026_case, 7),
}


def _sha(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.parametrize("name", sorted(BUILDERS))
def test_case_file_count_and_determinism(name, tmp_path):
    build, expected = BUILDERS[name]
    first = build(tmp_path / "a")
    second = build(tmp_path / "b")
    assert len(first["source_files"]) == expected
    assert [p.name for p in first["source_files"]] == [p.name for p in second["source_files"]]
    assert [_sha(p) for p in first["source_files"]] == [_sha(p) for p in second["source_files"]]


def test_radiofluoro_pairs(tmp_path):
    case = make_radiofluoro_2026_case(tmp_path)
    rf = case["rf_dir"]
    dup_a, dup_b = (pydicom.dcmread(rf / n) for n in case["duplicate_pair"])
    coll_a, coll_b = (pydicom.dcmread(rf / n) for n in case["collision_pair"])
    assert dup_a.SOPInstanceUID == dup_b.SOPInstanceUID
    assert dup_a.PixelData == dup_b.PixelData
    assert coll_a.SOPInstanceUID == coll_b.SOPInstanceUID
    assert hashlib.sha256(coll_a.PixelData).hexdigest() != hashlib.sha256(coll_b.PixelData).hexdigest()
    with pytest.raises(Exception):
        pydicom.dcmread(rf / case["corrupted_file"])


def test_hip_multiframe_and_absent_tags(tmp_path):
    case = make_hip_2024_case(tmp_path)
    for name, n_frames in case["expected_frames"].items():
        ds = pydicom.dcmread(case["rf_dir"] / name)
        assert int(ds.NumberOfFrames) == n_frames
        assert len(ds.PixelData) == n_frames * ds.Rows * ds.Columns * ds.BitsAllocated // 8
    for name, tags in case["expected_absent_tags"].items():
        ds = pydicom.dcmread(case["rf_dir"] / name)
        for tag in tags:
            assert tag not in ds
    assert len(case["expected_absent_tags"]) == 7  # 6 single-frame + the US instance


def test_knee_phi_boxes_cover_bright_text(tmp_path):
    from tests.synthetic_data import BRIGHT_TEXT_THRESHOLD

    case = make_knee_2025_case(tmp_path)
    assert len(case["phi_boxes"]) == 42
    for name, (x0, y0, x1, y1) in case["phi_boxes"].items():
        arr = pydicom.dcmread(case["rf_dir"] / name).pixel_array
        assert np.count_nonzero(arr[y0:y1, x0:x1] > BRIGHT_TEXT_THRESHOLD) > 0
        outside = arr.copy()
        outside[y0:y1, x0:x1] = 0
        assert np.count_nonzero(outside > BRIGHT_TEXT_THRESHOLD) == 0
