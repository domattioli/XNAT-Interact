"""
T003 — AEAD crosswalk encryption tests (011 US1).

All tests run offline; cryptography dep skipped when absent (F3).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("cryptography", reason="cryptography not installed — skipping crosswalk_crypto tests")

# Guard against broken system cryptography (pyo3 Rust panic cannot be caught;
# probe in a subprocess to detect before importing here).
import subprocess as _subprocess, sys as _sys  # noqa: E401
_probe = _subprocess.run(
    [_sys.executable, "-c",
     "from cryptography.hazmat.primitives.kdf.scrypt import Scrypt; "
     "from cryptography.hazmat.primitives.ciphers.aead import AESGCM"],
    capture_output=True,
)
if _probe.returncode != 0:
    pytest.skip(
        "cryptography bindings broken in this environment (pyo3/cffi init failure)",
        allow_module_level=True,
    )

from src.services.crosswalk_crypto import (
    derive_key,
    is_legacy_plaintext,
    open_envelope,
    seal,
)

PASSPHRASE = "test-librarian-passphrase"
KDF_SALT = b"\x01" * 16
FAKE_HAWKID = "testuser123"


def _make_key() -> bytes:
    return derive_key(PASSPHRASE, KDF_SALT)


def test_seal_open_roundtrip():
    """Seal and open produce byte-identical dicts."""
    key = _make_key()
    data = {FAKE_HAWKID: "pseudonym-abc", "another": "pseudonym-xyz"}
    blob = seal(data, key)
    recovered = open_envelope(blob, key)
    assert recovered == data


def test_plaintext_grep_fails():
    """Sealed blob does NOT contain the plaintext HawkID."""
    key = _make_key()
    data = {FAKE_HAWKID: "pseudonym-abc"}
    blob = seal(data, key)
    assert FAKE_HAWKID.encode() not in blob
    # Also verify the string doesn't appear as utf-8 anywhere
    assert FAKE_HAWKID not in blob.decode("latin-1")


def test_wrong_passphrase_raises():
    """Decryption with a wrong passphrase raises (fail-closed)."""
    key = _make_key()
    data = {"k": "v"}
    blob = seal(data, key)
    wrong_key = derive_key("wrong-passphrase", KDF_SALT)
    with pytest.raises(Exception):
        open_envelope(blob, wrong_key)


def test_legacy_plaintext_detected_and_resealed(tmp_path: Path):
    """is_legacy_plaintext detects plain JSON; re-seal produces valid envelope."""
    legacy_data = {FAKE_HAWKID: "pseudonym-abc"}
    legacy_bytes = json.dumps(legacy_data).encode("utf-8")

    assert is_legacy_plaintext(legacy_bytes), "should detect legacy JSON blob"

    # Re-seal
    key = _make_key()
    blob = seal(legacy_data, key)
    assert not is_legacy_plaintext(blob), "sealed envelope should NOT be detected as legacy"
    recovered = open_envelope(blob, key)
    assert recovered == legacy_data


def test_sealed_blob_not_detected_as_legacy():
    """is_legacy_plaintext returns False for a valid sealed envelope."""
    key = _make_key()
    blob = seal({"a": "b"}, key)
    assert not is_legacy_plaintext(blob)
