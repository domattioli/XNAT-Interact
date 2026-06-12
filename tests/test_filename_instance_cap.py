"""
Tests for src.xnat_scan_data.ScanFile.generate_source_image_file_name.

Validates that instance numbers (0-9999) are correctly zero-padded to 4 digits,
and rejects values outside this range or non-digit input.

Fixes issue #33-M1: instance numbers >= 1000 are now supported.
"""
from __future__ import annotations

import pytest

from src.xnat_scan_data import ScanFile


@pytest.fixture
def scan_stub():
    """Create a minimal ScanFile stub to access the static method."""
    stub = ScanFile.__new__(ScanFile)
    stub._image = 0  # keep ScanFile.__del__ quiet during GC
    return stub


class TestInstanceNumberZeroPadding:
    """Instance numbers should be zero-padded to 4 digits."""

    def test_single_digit_instance_zero_padded(self, scan_stub):
        """Instance 1 → '0001-UID'."""
        result = ScanFile.generate_source_image_file_name("1", "test_uid")
        assert result == "0001-test_uid"

    def test_two_digit_instance_zero_padded(self, scan_stub):
        """Instance 42 → '0042-UID'."""
        result = ScanFile.generate_source_image_file_name("42", "test_uid")
        assert result == "0042-test_uid"

    def test_three_digit_instance_zero_padded(self, scan_stub):
        """Instance 999 → '0999-UID'."""
        result = ScanFile.generate_source_image_file_name("999", "test_uid")
        assert result == "0999-test_uid"

    def test_four_digit_instance_no_padding_needed(self, scan_stub):
        """Instance 1000 → '1000-UID' (no padding, already 4 digits)."""
        result = ScanFile.generate_source_image_file_name("1000", "test_uid")
        assert result == "1000-test_uid"

    def test_four_digit_instance_9999(self, scan_stub):
        """Instance 9999 → '9999-UID' (max valid value)."""
        result = ScanFile.generate_source_image_file_name("9999", "test_uid")
        assert result == "9999-test_uid"


class TestInstanceNumberBoundaryAndFormat:
    """Test boundary cases and format validation."""

    def test_zero_instance_zero_padded(self, scan_stub):
        """Instance 0 → '0000-UID'."""
        result = ScanFile.generate_source_image_file_name("0", "test_uid")
        assert result == "0000-test_uid"

    def test_returns_correct_format_with_hyphen(self, scan_stub):
        """Result format is 'DDDD-UID', separated by hyphen."""
        result = ScanFile.generate_source_image_file_name("123", "my_patient_id")
        assert result == "0123-my_patient_id"
        assert "-" in result
        parts = result.split("-")
        assert len(parts) == 2
        assert len(parts[0]) == 4


class TestInstanceNumberRejection:
    """Instance numbers outside 0-9999 or non-digit should be rejected."""

    def test_five_digit_instance_rejected(self, scan_stub):
        """Instance 10000 (5 digits) → AssertionError."""
        with pytest.raises(AssertionError, match="at most 4 digits"):
            ScanFile.generate_source_image_file_name("10000", "test_uid")

    def test_leading_zero_five_digit_rejected(self, scan_stub):
        """Instance 01000 (5 chars, even with leading 0) → AssertionError."""
        with pytest.raises(AssertionError, match="at most 4 digits"):
            ScanFile.generate_source_image_file_name("01000", "test_uid")

    def test_non_digit_letters_rejected(self, scan_stub):
        """Instance 'abc' → AssertionError (not digits-only)."""
        with pytest.raises(AssertionError, match="digits-only"):
            ScanFile.generate_source_image_file_name("abc", "test_uid")

    def test_non_digit_mixed_rejected(self, scan_stub):
        """Instance '12a3' → AssertionError (not digits-only)."""
        with pytest.raises(AssertionError, match="digits-only"):
            ScanFile.generate_source_image_file_name("12a3", "test_uid")

    def test_non_digit_special_chars_rejected(self, scan_stub):
        """Instance '100!' → AssertionError (not digits-only)."""
        with pytest.raises(AssertionError, match="digits-only"):
            ScanFile.generate_source_image_file_name("100!", "test_uid")

    def test_empty_string_rejected(self, scan_stub):
        """Instance '' → AssertionError (empty, not digits-only)."""
        with pytest.raises(AssertionError, match="digits-only"):
            ScanFile.generate_source_image_file_name("", "test_uid")

    def test_whitespace_rejected(self, scan_stub):
        """Instance ' 123 ' → AssertionError (contains whitespace)."""
        with pytest.raises(AssertionError, match="digits-only"):
            ScanFile.generate_source_image_file_name(" 123 ", "test_uid")


class TestLexicographicSafety:
    """Ensure 4-digit zero-padding maintains lexicographic sort order."""

    def test_padded_instances_sort_lexicographically(self, scan_stub):
        """Padded instance names sort lexicographically in numeric order."""
        names = [
            ScanFile.generate_source_image_file_name(str(i), "uid")
            for i in [1, 10, 100, 1000, 9999]
        ]
        assert names == sorted(names), "Instance names should be lexicographically sorted"

    def test_boundary_instances_sort_correctly(self, scan_stub):
        """Instances 999, 1000, 1001 sort in correct order."""
        names = [
            ScanFile.generate_source_image_file_name("999", "uid"),
            ScanFile.generate_source_image_file_name("1000", "uid"),
            ScanFile.generate_source_image_file_name("1001", "uid"),
        ]
        assert names == ["0999-uid", "1000-uid", "1001-uid"]
        assert names == sorted(names)


class TestPatientUIDPassthrough:
    """Patient UID should be returned as-is."""

    def test_patient_uid_unchanged(self, scan_stub):
        """Patient UID is returned exactly as passed."""
        patient_uid = "some_complex_uid_123"
        result = ScanFile.generate_source_image_file_name("5", patient_uid)
        assert result.endswith(f"-{patient_uid}")

    def test_patient_uid_with_underscores(self, scan_stub):
        """Patient UID with underscores is preserved."""
        patient_uid = "1_2_840_10008_1_2_3"
        result = ScanFile.generate_source_image_file_name("100", patient_uid)
        assert result == f"0100-{patient_uid}"

    def test_patient_uid_empty_string(self, scan_stub):
        """Patient UID can be empty string (though unusual)."""
        result = ScanFile.generate_source_image_file_name("42", "")
        assert result == "0042-"
