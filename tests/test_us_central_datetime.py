"""
Tests for src.utilities.USCentralDateTime — date/time normalization.

This class is pure logic (no server) and is used all over the upload path, so
it is a high-value, low-cost thing to pin down with tests.
"""
from __future__ import annotations

import pytest

from src.utilities import USCentralDateTime


def test_parses_explicit_datetime():
    dt = USCentralDateTime("2022-01-01 11:00:00")
    assert dt.date == "20220101"
    assert dt.time.startswith("11")


def test_default_is_epoch_placeholder():
    # No argument -> the code's documented placeholder of 1900-01-01.
    dt = USCentralDateTime()
    assert dt.date == "19000101"


def test_date_and_time_formats_are_fixed_width():
    dt = USCentralDateTime("2023-07-04 09:30:00")
    assert len(dt.date) == 8          # YYYYMMDD
    assert dt.time.count(":") == 0    # HHMMSS.mmm, no separators
    assert dt.date == "20230704"


def test_pst_input_is_converted_to_us_central():
    # 11:00 PST == 13:00 US-Central (standard time, 2-hour offset).
    dt = USCentralDateTime("2022-01-01 11:00:00 PST")
    assert dt.date == "20220101"
    assert dt.time.startswith("13")


def test_str_is_stable_and_labeled_central():
    dt = USCentralDateTime("2022-01-01 11:00:00")
    assert "US-CST" in str(dt)


@pytest.mark.known_issue
def test_unparseable_string_currently_raises():
    """
    Current behavior: a totally junk string raises rather than returning a
    safe default or a user-friendly message. The improvement plan replaces
    this with graceful handling + re-prompt.
    """
    with pytest.raises(Exception):
        USCentralDateTime("nonsense time o'clock zzz")
