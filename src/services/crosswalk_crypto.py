"""
011 US1 — AEAD envelope for CrosswalkStore at-rest encryption.

All cryptography imports are lazy (inside each function) so this module
can be imported even on a bare env; callers should use
``pytest.importorskip("cryptography")`` or handle ImportError.

Envelope layout (bytes):
    MAGIC(8) || kdf_salt(16) || nonce(12) || ciphertext

KDF: scrypt(n=2**14, r=8, p=1, length=32) — pinned params.
AEAD: AES-256-GCM (96-bit nonce, 128-bit tag).
"""
from __future__ import annotations

import json
import os

# --- constants ---------------------------------------------------------------

_MAGIC = b"XWLK\x00\x01\x00\x00"  # 8 bytes
_MAGIC_LEN = 8
_SALT_LEN = 16
_NONCE_LEN = 12
_KEY_LEN = 32

# scrypt pinned params
_N = 2 ** 14
_R = 8
_P = 1


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def derive_key(passphrase: str, kdf_salt: bytes) -> bytes:
    """
    Derive a 256-bit key from *passphrase* using scrypt.

    Parameters never logged or returned by reference — caller must zeroize
    if needed.
    """
    from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
    from cryptography.hazmat.backends import default_backend

    kdf = Scrypt(salt=kdf_salt, length=_KEY_LEN, n=_N, r=_R, p=_P,
                 backend=default_backend())
    return kdf.derive(passphrase.encode("utf-8"))


def seal(data: dict, key: bytes) -> bytes:
    """
    JSON-encode *data* then AEAD-encrypt with *key*.

    Returns the binary envelope (MAGIC || kdf_salt || nonce || ciphertext).
    kdf_salt is stored so ``open_envelope`` has access to it; the caller-
    supplied *key* is already derived — kdf_salt in the envelope is
    informational (needed when re-deriving from passphrase externally).
    We generate a fresh random kdf_salt per call so the envelope is
    self-contained; pass the same kdf_salt to ``derive_key`` for key
    consistency across calls.

    NOTE: the kdf_salt embedded here is a NEW random value (used for
    envelope self-description).  When integrating with CrosswalkStore use
    the same kdf_salt that produced *key* so ``open_envelope`` can signal
    the correct salt back to callers.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    kdf_salt = os.urandom(_SALT_LEN)
    nonce = os.urandom(_NONCE_LEN)
    plaintext = json.dumps(data, separators=(",", ":")).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    return _MAGIC + kdf_salt + nonce + ciphertext


def seal_with_salt(data: dict, key: bytes, kdf_salt: bytes) -> bytes:
    """
    Like ``seal`` but embeds the caller-supplied *kdf_salt* in the envelope.
    Use this when you want round-trip key derivation from passphrase.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    if len(kdf_salt) != _SALT_LEN:
        raise ValueError(f"kdf_salt must be {_SALT_LEN} bytes, got {len(kdf_salt)}")
    nonce = os.urandom(_NONCE_LEN)
    plaintext = json.dumps(data, separators=(",", ":")).encode("utf-8")
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, None)
    return _MAGIC + kdf_salt + nonce + ciphertext


def open_envelope(blob: bytes, key: bytes) -> dict:
    """
    Decrypt and deserialize an AEAD envelope produced by ``seal`` / ``seal_with_salt``.

    Raises ``InvalidTag`` (from cryptography) on any authentication failure
    (wrong key, tampered ciphertext) — fail-closed.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    _check_magic(blob)
    offset = _MAGIC_LEN + _SALT_LEN  # skip magic + kdf_salt
    nonce = blob[offset: offset + _NONCE_LEN]
    ciphertext = blob[offset + _NONCE_LEN:]
    plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
    return json.loads(plaintext.decode("utf-8"))


def extract_kdf_salt(blob: bytes) -> bytes:
    """Return the kdf_salt embedded in a sealed envelope."""
    _check_magic(blob)
    return blob[_MAGIC_LEN: _MAGIC_LEN + _SALT_LEN]


def is_legacy_plaintext(blob: bytes) -> bool:
    """
    Return True if *blob* does NOT start with the XWLK magic header.

    A legacy crosswalk file is plain UTF-8 JSON.  Any file starting with
    the magic bytes is treated as a sealed envelope.
    """
    return not blob.startswith(_MAGIC)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _check_magic(blob: bytes) -> None:
    if not blob[:_MAGIC_LEN] == _MAGIC:
        raise ValueError(
            "Envelope magic mismatch — not a sealed crosswalk blob "
            "(use is_legacy_plaintext() to detect legacy files)."
        )
