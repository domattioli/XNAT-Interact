"""
tests/test_device_id_private_group.py — B4: device_id_for ignores private group 0x0019.

The removed branch tested tag (0000,0019) instead of group 0x0019 and keyed on
id(), which is not stable. Group 0x0019 must not decide device identity: GE
files and our own de-id stash tags both use it. Offline, synthetic tags only.
"""
from __future__ import annotations

from pydicom.dataset import Dataset

from src.services.pixel_deid.profiles import device_id_for


def _ds() -> Dataset:
    ds = Dataset()
    ds.Manufacturer = "ZZSYNTH Medical"
    ds.ManufacturerModelName = "Twin C-Arm"
    ds.Modality = "RF"
    return ds


def test_private_group_0019_does_not_change_device_id():
    plain = _ds()
    tagged = _ds()
    tagged.add_new((0x0019, 0x1001), "LT", "CaseDateHash: synthetic")
    tagged.add_new((0x0019, 0x1003), "LT", "Old_StudyInstanceUID: 1.2.3")
    assert device_id_for(plain) == device_id_for(tagged) == "zzsynth_medical_twin_c-arm"


def test_element_0000_0019_does_not_hijack_device_id():
    ds = _ds()
    ds.add_new((0x0000, 0x0019), "UL", 1)  # what the old "0x0019 in dataset" actually tested
    assert device_id_for(ds) == "zzsynth_medical_twin_c-arm"


def test_device_id_stable_across_copies():
    assert len({device_id_for(_ds()) for _ in range(5)}) == 1
