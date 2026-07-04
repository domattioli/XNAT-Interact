from __future__ import annotations

import pytest

from src.xnat_scan_data import ScanFile

gen = ScanFile.generate_source_image_file_name


@pytest.mark.parametrize(
    "inst, expected",
    [
        ("1", "0001-U"),
        ("5", "0005-U"),
        ("42", "0042-U"),
        ("999", "0999-U"),
        # Regression (#33 M1): instance numbers >= 1000 previously crashed with
        # AssertionError despite the 4-digit zero-pad. Now supported up to 9999,
        # filenames stay fixed-width so lexical sort matches numeric order.
        ("1000", "1000-U"),
        ("9999", "9999-U"),
    ],
)
def test_filename_zero_padded_four_digits(inst, expected):
    assert gen(inst, "U") == expected


def test_filenames_sort_in_numeric_order():
    names = [gen(str(n), "U") for n in (1, 9, 10, 999, 1000, 9999)]
    assert names == sorted(names)


def test_instance_over_9999_still_rejected():
    # Documented hard cap: >= 10000 instances is out of scope and must fail loudly.
    with pytest.raises(AssertionError):
        gen("10000", "U")
