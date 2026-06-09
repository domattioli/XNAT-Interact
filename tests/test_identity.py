"""
Tests for src/services/identity.py — Stage 1 (T002).

All tests run offline: no XNAT server, no VPN, no PHI.
Uses synthetic DICOM datasets from tests/synthetic_data.py.
"""
from __future__ import annotations

import pytest

from tests.synthetic_data import make_phi_dicom_dataset
from src.services.identity import (
    case_date_hash,
    image_content_hash,
    load_identity_salt,
    surgeon_pseudonym,
    uids_corroborate,
)
from src.services.xnat_gateway import GatewayError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_SALT_A = bytes.fromhex("deadbeef" * 8)  # 32 bytes, arbitrary test value
_SALT_B = bytes.fromhex("cafebabe" * 8)  # different salt for contrast tests


# ---------------------------------------------------------------------------
# T002-a: image_content_hash
# ---------------------------------------------------------------------------

class TestImageContentHash:
    def test_same_pixels_same_hash(self):
        """Identical pixel data → identical hash (deterministic)."""
        ds1 = make_phi_dicom_dataset(seed=42)
        ds2 = make_phi_dicom_dataset(seed=42)
        assert image_content_hash(ds1) == image_content_hash(ds2)

    def test_different_seeds_different_hash(self):
        """Different seeds → different pixel data → different hash."""
        ds1 = make_phi_dicom_dataset(seed=0)
        ds2 = make_phi_dicom_dataset(seed=1)
        assert image_content_hash(ds1) != image_content_hash(ds2)

    def test_one_pixel_difference_different_hash(self):
        """Flipping one pixel byte → different hash."""
        ds = make_phi_dicom_dataset(seed=7)
        original_hash = image_content_hash(ds)

        # Flip the last byte of PixelData
        pixels = bytearray(ds.PixelData)
        pixels[-1] ^= 0xFF
        ds.PixelData = bytes(pixels)

        assert image_content_hash(ds) != original_hash

    def test_returns_hex_string(self):
        """Result is a 64-char lowercase hex string (sha256)."""
        ds = make_phi_dicom_dataset(seed=0)
        result = image_content_hash(ds)
        assert isinstance(result, str)
        assert len(result) == 64
        assert result == result.lower()
        int(result, 16)  # raises ValueError if not hex

    def test_hash_before_processing_is_stable(self):
        """Hash uses raw bytes — not affected by ds.pixel_array or decoding."""
        ds = make_phi_dicom_dataset(seed=3)
        h1 = image_content_hash(ds)
        # Access pixel_array (triggers pydicom decode) then hash again — must match.
        _ = ds.pixel_array
        h2 = image_content_hash(ds)
        assert h1 == h2


# ---------------------------------------------------------------------------
# T002-b: surgeon_pseudonym
# ---------------------------------------------------------------------------

class TestSurgeonPseudonym:
    def test_same_inputs_same_pseudonym(self):
        """Same hawkid + same salt → same pseudonym."""
        assert surgeon_pseudonym("jdoe", _SALT_A) == surgeon_pseudonym("jdoe", _SALT_A)

    def test_different_salt_different_pseudonym(self):
        """Same hawkid + different salt → different pseudonym."""
        assert surgeon_pseudonym("jdoe", _SALT_A) != surgeon_pseudonym("jdoe", _SALT_B)

    def test_different_hawkid_different_pseudonym(self):
        """Different hawkids → different pseudonyms."""
        assert surgeon_pseudonym("jdoe", _SALT_A) != surgeon_pseudonym("jsmith", _SALT_A)

    def test_output_does_not_contain_hawkid(self):
        """Irreversibility sanity: raw HawkID must not appear in pseudonym."""
        hawkid = "secrethawk"
        result = surgeon_pseudonym(hawkid, _SALT_A)
        assert hawkid not in result
        assert hawkid.upper() not in result.upper()

    def test_pseudonym_format(self):
        """Pseudonym starts with 'surgeon_' prefix and is a fixed-length string."""
        result = surgeon_pseudonym("anyone", _SALT_A)
        assert result.startswith("surgeon_")
        suffix = result[len("surgeon_"):]
        # First 12 hex chars of SHA-256 digest
        assert len(suffix) == 12
        int(suffix, 16)  # must be valid hex


# ---------------------------------------------------------------------------
# T002-c: case_date_hash
# ---------------------------------------------------------------------------

class TestCaseDateHash:
    def test_deterministic(self):
        """Same inputs → same hash."""
        h1 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        h2 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        assert h1 == h2

    def test_different_date_different_hash(self):
        h1 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        h2 = case_date_hash("20240116", "142035", "GE OEC 9900", _SALT_A)
        assert h1 != h2

    def test_different_time_different_hash(self):
        h1 = case_date_hash("20240115", "120000", "GE OEC 9900", _SALT_A)
        h2 = case_date_hash("20240115", "130000", "GE OEC 9900", _SALT_A)
        assert h1 != h2

    def test_different_device_different_hash(self):
        h1 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        h2 = case_date_hash("20240115", "142035", "Siemens ARCADIS", _SALT_A)
        assert h1 != h2

    def test_different_salt_different_hash(self):
        h1 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        h2 = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_B)
        assert h1 != h2

    def test_returns_hex_string(self):
        result = case_date_hash("20240115", "142035", "GE OEC 9900", _SALT_A)
        assert len(result) == 64
        int(result, 16)


# ---------------------------------------------------------------------------
# T002-d: uids_corroborate
# ---------------------------------------------------------------------------

class TestUidsCorroborate:
    def test_matching_study_uid(self):
        a = {"StudyInstanceUID": "1.2.3", "SOPInstanceUID": "9.9.9"}
        b = {"StudyInstanceUID": "1.2.3", "SOPInstanceUID": "8.8.8"}
        assert uids_corroborate(a, b) is True

    def test_matching_sop_uid(self):
        a = {"SOPInstanceUID": "1.1.1"}
        b = {"SOPInstanceUID": "1.1.1"}
        assert uids_corroborate(a, b) is True

    def test_no_match(self):
        a = {"StudyInstanceUID": "1.2.3"}
        b = {"StudyInstanceUID": "4.5.6"}
        assert uids_corroborate(a, b) is False

    def test_empty_dicts(self):
        assert uids_corroborate({}, {}) is False

    def test_one_empty_dict(self):
        a = {"StudyInstanceUID": "1.2.3"}
        assert uids_corroborate(a, {}) is False

    def test_empty_string_values_do_not_corroborate(self):
        a = {"StudyInstanceUID": ""}
        b = {"StudyInstanceUID": ""}
        assert uids_corroborate(a, b) is False


# ---------------------------------------------------------------------------
# T002-e: load_identity_salt
# ---------------------------------------------------------------------------

class TestLoadIdentitySalt:
    def test_env_var_present_returns_bytes(self, monkeypatch):
        """Valid hex env var → decoded bytes."""
        hex_salt = "deadbeef" * 8  # 64 hex chars = 32 bytes
        monkeypatch.setenv("XNAT_IDENTITY_SALT", hex_salt)
        result = load_identity_salt()
        assert isinstance(result, bytes)
        assert result == bytes.fromhex(hex_salt)

    def test_env_var_absent_raises_gateway_error(self, monkeypatch):
        """Missing env var → GatewayError with a FriendlyError inside."""
        monkeypatch.delenv("XNAT_IDENTITY_SALT", raising=False)
        with pytest.raises(GatewayError) as exc_info:
            load_identity_salt()
        fe = exc_info.value.friendly
        assert "XNAT_IDENTITY_SALT" in fe.message or "XNAT_IDENTITY_SALT" in fe.title or any(
            "XNAT_IDENTITY_SALT" in r for r in fe.recourse
        )

    def test_error_message_does_not_contain_salt(self, monkeypatch):
        """Even a malformed salt value must not leak into the error message."""
        secret = "cafebabe" * 8
        monkeypatch.setenv("XNAT_IDENTITY_SALT", "not-valid-hex-!!")
        with pytest.raises(GatewayError) as exc_info:
            load_identity_salt()
        fe = exc_info.value.friendly
        # The raw bad value must not appear verbatim (it could contain PHI if mis-set).
        assert "not-valid-hex-!!" not in fe.message
        _ = secret  # salt itself is never touched in this branch

    def test_file_path_env_var(self, monkeypatch, tmp_path):
        """Env var pointing to a file → reads hex salt from file first line."""
        hex_salt = "aabbccdd" * 8
        salt_file = tmp_path / "salt.hex"
        salt_file.write_text(hex_salt + "\n")
        monkeypatch.setenv("XNAT_IDENTITY_SALT", str(salt_file))
        result = load_identity_salt()
        assert result == bytes.fromhex(hex_salt)

    def test_unreadable_file_raises_gateway_error(self, monkeypatch, tmp_path):
        """Env var pointing to a nonexistent file → GatewayError."""
        monkeypatch.setenv("XNAT_IDENTITY_SALT", str(tmp_path / "nonexistent.hex"))
        with pytest.raises(GatewayError):
            load_identity_salt()
