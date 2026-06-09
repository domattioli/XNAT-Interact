"""
Identity primitives for XNAT-Interact (Feature 009, Stage 1).

Pure module — no side effects, no I/O, no external dependencies beyond stdlib.
Wire-in to the ingest pipeline is deferred to Stages 4/5.

Public API
----------
image_content_hash(ds) -> str
    sha256 hex of raw PixelData bytes, taken before any processing.

surgeon_pseudonym(hawkid, salt) -> str
    HMAC-SHA256(salt, hawkid) hex; stable and irreversible without salt.

case_date_hash(date, time, device, salt) -> str
    HMAC-SHA256(salt, "{date}|{time}|{device}") hex; hides the HIPAA date.

uids_corroborate(orig_a, orig_b) -> bool
    Compares original StudyInstanceUID/SOPInstanceUID dicts as corroborating
    evidence (not authoritative).

load_identity_salt() -> bytes
    Reads the librarian-only salt from env var XNAT_IDENTITY_SALT.
    Raises GatewayError (wrapping a FriendlyError) if absent.

SECURITY INVARIANTS
-------------------
- Salt is NEVER logged, included in error messages, or written anywhere.
- FriendlyError is raised (via GatewayError) — not returned — when the salt
  is absent, so callers cannot silently ignore the failure.
"""
from __future__ import annotations

import hashlib
import hmac
import os
from typing import Dict

from src.services.errors import FriendlyError
from src.services.xnat_gateway import GatewayError


# ---------------------------------------------------------------------------
# Image identity
# ---------------------------------------------------------------------------

def image_content_hash(ds) -> str:
    """
    Return the sha256 hex digest of the raw PixelData bytes in *ds*.

    This is the **authoritative image identity** (DATA_MODEL §3.4, §5.1).
    Must be called on the source Dataset *before* any tag rewrite or pixel
    normalization; the hash survives de-identification and re-export precisely
    because it is anchored to raw acquisition pixels.

    Parameters
    ----------
    ds:
        A pydicom Dataset with a ``PixelData`` attribute (bytes or bytearray).

    Returns
    -------
    str
        64-character lowercase hex digest.

    Raises
    ------
    AttributeError
        If *ds* has no ``PixelData`` attribute.
    """
    pixel_bytes: bytes = bytes(ds.PixelData)
    return hashlib.sha256(pixel_bytes).hexdigest()


# ---------------------------------------------------------------------------
# Surgeon pseudonym
# ---------------------------------------------------------------------------

def surgeon_pseudonym(hawkid: str, salt: bytes) -> str:
    """
    Return the keyed-HMAC pseudonym for a surgeon's HawkID.

    ``HMAC-SHA256(salt, hawkid.encode('utf-8'))`` — stable for identical
    inputs, irreversible without the salt (DATA_MODEL §3.2, FR-011).

    Parameters
    ----------
    hawkid:
        The surgeon's institutional HawkID (plaintext — never stored).
    salt:
        Librarian-held secret bytes.  Obtain via :func:`load_identity_salt`.

    Returns
    -------
    str
        ``"surgeon_"`` prefix + first 12 hex characters of the HMAC digest.
        Short enough to be a readable label; long enough (48-bit) to be
        collision-resistant within the corpus size.

        The full digest is deterministic — the prefix+12 form is chosen for
        readability.  Internal callers that need the full digest can call
        ``hmac.new(salt, hawkid.encode(), 'sha256').hexdigest()`` directly.
    """
    digest = hmac.new(salt, hawkid.encode("utf-8"), "sha256").hexdigest()
    return f"surgeon_{digest[:12]}"


# ---------------------------------------------------------------------------
# Case date hash
# ---------------------------------------------------------------------------

def case_date_hash(date: str, time: str, device: str, salt: bytes) -> str:
    """
    Return the HMAC-SHA256 dedup fingerprint for a case's date/time/device.

    ``HMAC-SHA256(salt, "{date}|{time}|{device}".encode('utf-8'))``

    The exact surgery date is a HIPAA identifier and MUST NOT be stored in
    readable form on uploaded objects or in the registry.  This hash is the
    only persistent record of the date (DATA_MODEL §3.3, FR-003).

    Parameters
    ----------
    date:
        StudyDate string (e.g. ``"20240115"``).
    time:
        StudyTime string (e.g. ``"142035"``).
    device:
        Manufacturer or ManufacturerModelName string.
    salt:
        Librarian-held secret bytes.  Obtain via :func:`load_identity_salt`.

    Returns
    -------
    str
        64-character lowercase hex digest.
    """
    payload = f"{date}|{time}|{device}".encode("utf-8")
    return hmac.new(salt, payload, "sha256").hexdigest()


# ---------------------------------------------------------------------------
# UID corroboration helper
# ---------------------------------------------------------------------------

def uids_corroborate(orig_a: Dict[str, str], orig_b: Dict[str, str]) -> bool:
    """
    Return True if the original UIDs in *orig_a* and *orig_b* agree on at
    least one of the checked keys.

    UIDs are **corroborating evidence only** — they are not authoritative
    (DATA_MODEL §3.3, §3.4, Q9).  Agreement raises confidence that two
    content-hash matches refer to the same acquisition; disagreement does NOT
    override a content-hash match.

    Checked keys (either dict may omit any key):
        ``StudyInstanceUID``, ``SOPInstanceUID``

    Parameters
    ----------
    orig_a, orig_b:
        Dicts of original (pre-rewrite) UID strings, e.g.
        ``{"StudyInstanceUID": "1.2.840…", "SOPInstanceUID": "…"}``.

    Returns
    -------
    bool
        True if at least one key is present in both dicts and the values are
        equal (and non-empty).  False if no checked key overlaps or all
        overlapping values differ.
    """
    _KEYS = ("StudyInstanceUID", "SOPInstanceUID")
    for key in _KEYS:
        val_a = orig_a.get(key, "")
        val_b = orig_b.get(key, "")
        if val_a and val_b and val_a == val_b:
            return True
    return False


# ---------------------------------------------------------------------------
# Salt provisioning
# ---------------------------------------------------------------------------

_ENV_VAR = "XNAT_IDENTITY_SALT"


def load_identity_salt() -> bytes:
    """
    Load the librarian-held identity salt from the environment.

    Reads ``XNAT_IDENTITY_SALT`` from the process environment.  The value
    may be either:
      - A raw hex string (decoded to bytes), or
      - A filesystem path to a restricted file whose first line is the hex
        salt — set ``XNAT_IDENTITY_SALT=/path/to/salt.hex`` to use this form.

    The salt is NEVER logged, echoed, or included in any error message.

    Returns
    -------
    bytes
        The decoded salt bytes.

    Raises
    ------
    GatewayError
        Wrapping a :class:`~src.services.errors.FriendlyError` when the env
        var is absent or the salt cannot be read.  Callers must not silently
        ignore this failure — dedup and pseudonymisation require the salt.
    """
    raw = os.environ.get(_ENV_VAR)
    if not raw:
        raise GatewayError(
            FriendlyError(
                title="Identity salt not configured",
                message=(
                    "The XNAT_IDENTITY_SALT environment variable is not set. "
                    "The salt is required for surgeon pseudonymisation and case "
                    "date hashing. Contact the Data Librarian to obtain and "
                    "configure the salt before running the ingest pipeline."
                ),
                recourse=[
                    f"Set the {_ENV_VAR} environment variable to the hex salt "
                    "value provided by the Data Librarian.",
                    "Alternatively, set it to the path of a restricted file "
                    "containing the hex salt on its first line.",
                    "Never hard-code the salt in source or commit it to git.",
                ],
            )
        )

    # If the value looks like a filesystem path, read from the file.
    # Heuristic: contains a path separator or starts with '/'.
    # This avoids interpreting a 64-char hex string as a path.
    if os.sep in raw or raw.startswith("/"):
        try:
            raw = open(raw).readline().strip()  # noqa: WPS515 — intentional one-liner
        except OSError as exc:
            raise GatewayError(
                FriendlyError(
                    title="Identity salt file unreadable",
                    message=(
                        f"The {_ENV_VAR} environment variable points to a file "
                        "that could not be read. Ensure the file exists, is "
                        "accessible to this process, and contains the hex salt "
                        "on its first line."
                    ),
                    recourse=[
                        "Check that the file path is correct and the file is readable.",
                        "Ensure file permissions allow the ingest process to read it.",
                        f"Alternatively, set {_ENV_VAR} directly to the hex salt string.",
                    ],
                )
            ) from exc

    # Decode hex → bytes (validates format as a side-effect).
    try:
        return bytes.fromhex(raw)
    except ValueError as exc:
        raise GatewayError(
            FriendlyError(
                title="Identity salt format invalid",
                message=(
                    f"The value in {_ENV_VAR} is not a valid hex string. "
                    "The salt must be provided as a hexadecimal string "
                    "(e.g. the output of ``openssl rand -hex 32``)."
                ),
                recourse=[
                    "Confirm the salt value with the Data Librarian.",
                    "Ensure no extra whitespace, quotes, or line endings are included.",
                    f"Re-set {_ENV_VAR} to the correct hex string.",
                ],
            )
        ) from exc
