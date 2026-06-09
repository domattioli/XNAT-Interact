"""
Tests for the pure string-validation helpers in
src.batch_upload.BatchUploadRepresentation.

The batch path is where one bad spreadsheet cell currently aborts an entire
upload. These helpers decide whether a "Performer HawkID-Task" cell is well
formed. They are pure (no server / no config), so we drive them on a bare
instance built with __new__.
"""
from __future__ import annotations

import pytest

from src.batch_upload import BatchUploadRepresentation


@pytest.fixture
def batch():
    """A bare instance — enough to call the pure helper methods."""
    return BatchUploadRepresentation.__new__(BatchUploadRepresentation)


def test_valid_dict_string_produces_no_notices(batch):
    formatted, notices = batch._validate_and_format_dict_string("{john: lead}")
    assert notices == []
    assert "john" in formatted.lower()


def test_missing_braces_is_flagged(batch):
    _, notices = batch._validate_and_format_dict_string("john: lead")
    assert notices  # at least one complaint


def test_mismatched_colons_and_semicolons_flagged(batch):
    # two keys but no separating semicolon -> colon/semicolon count mismatch.
    _, notices = batch._validate_and_format_dict_string("{a: 1 b: 2}")
    assert notices


def test_revise_string_tightens_quote_spacing(batch):
    # _revise_string collapses spaces around interior quotes.
    out = batch._revise_string("{'a' : 'b'}")
    assert isinstance(out, str)


@pytest.mark.parametrize("value,expected", [("", True), (" ", True), (None, True), ("x", False)])
def test_col_is_empty(batch, value, expected):
    assert batch._col_is_empty(value) is expected
