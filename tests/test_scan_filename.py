import pytest
from src.xnat_scan_data import ScanFile


class TestGenerateSourceImageFileName:
    """Regression tests for ScanFile.generate_source_image_file_name."""

    def test_single_digit_instance(self):
        """Single digit should be zero-padded to 4 digits."""
        result = ScanFile.generate_source_image_file_name("1", "UID")
        assert result == "0001-UID"

    def test_three_digit_instance(self):
        """Three digits at the 999 boundary (previously max)."""
        result = ScanFile.generate_source_image_file_name("999", "UID")
        assert result == "0999-UID"

    def test_four_digit_instance_at_1000(self):
        """Four digits at 1000 (previously crashed with AssertionError)."""
        result = ScanFile.generate_source_image_file_name("1000", "UID")
        assert result == "1000-UID"

    def test_four_digit_instance_at_9999(self):
        """Four digits at 9999."""
        result = ScanFile.generate_source_image_file_name("9999", "UID")
        assert result == "9999-UID"

    def test_five_digit_instance(self):
        """Five digits (natural width preserved, no truncation)."""
        result = ScanFile.generate_source_image_file_name("12345", "UID")
        assert result == "12345-UID"

    def test_non_digit_input_raises_assertion_error(self):
        """Non-digit input (e.g. alphanumeric) should raise AssertionError."""
        with pytest.raises(AssertionError):
            ScanFile.generate_source_image_file_name("12a", "UID")

    def test_empty_string_raises_assertion_error(self):
        """Empty string should raise AssertionError."""
        with pytest.raises(AssertionError):
            ScanFile.generate_source_image_file_name("", "UID")

    def test_whitespace_raises_assertion_error(self):
        """Whitespace should raise AssertionError."""
        with pytest.raises(AssertionError):
            ScanFile.generate_source_image_file_name(" ", "UID")
