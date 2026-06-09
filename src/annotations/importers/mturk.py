"""
annotations.importers.mturk — Amazon Mechanical Turk batch-row importer.

Mirrors the intent of the dormant MTurkSemanticSegmentation stub in
src/xnat_scan_data.py (lines ~307-365):
  - annotator_id = WorkerId  (opaque MTurk token — not a human name)
  - decode base64 pngImageData field → grayscale mask via cv2
  - created_at from SubmitTime

PHI guard:
  - WorkerId is an opaque platform-assigned token (safe to store).
  - Human names MUST NOT appear in annotator_id — validate_annotator_id enforces this.
  - Never store HITId, AssignmentId, or other MTurk metadata as annotator_id.
"""
from __future__ import annotations

import base64
from typing import Any, Mapping, Optional

import numpy as np

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation
from src.annotations.validate import validate_annotator_id


def _raise(fe: FriendlyError) -> None:
    raise AnnotationError(fe)


# MTurk column names this importer understands
_WORKER_ID_KEY = "WorkerId"
_SUBMIT_TIME_KEY = "SubmitTime"

# Possible pngImageData column suffixes (real batches use e.g.
# "Answer.annotatedResult.pngImageData" or "Answer.pngImageData").
# We search for any key whose name ends with ".pngImageData" or equals
# "pngImageData", and also accept "Answer.*" patterns.
_PNG_SUFFIXES = (".pngImageData",)
_PNG_EXACT = ("pngImageData",)


def _find_png_field(row: Mapping[str, Any]) -> Optional[str]:
    """
    Return the first key in ``row`` that looks like a pngImageData column.
    Search order: exact 'pngImageData', then any key ending with '.pngImageData'.
    Returns None if not found.
    """
    for key in _PNG_EXACT:
        if key in row:
            return key
    for key in row:
        if isinstance(key, str) and key.endswith(_PNG_SUFFIXES[0]):
            return key
    # Also accept 'Answer.*' pattern broadly for flexible MTurk formats
    for key in row:
        if isinstance(key, str) and "pngImageData" in key:
            return key
    return None


def _decode_base64_mask(b64_str: str) -> np.ndarray:
    """
    Decode a base64-encoded PNG string to a 2-D grayscale numpy uint8 array.

    Mirrors commented logic in MTurkSemanticSegmentation._extract_pngImageData:
        cv2.imdecode(np.frombuffer(base64.b64decode(b64_str), np.uint8), IMREAD_GRAYSCALE)

    Raises AnnotationError on decode failure.
    """
    import cv2

    try:
        raw_bytes = base64.b64decode(b64_str)
    except Exception as exc:
        _raise(FriendlyError(
            title="MTurk mask decode failed — bad base64",
            message="Could not base64-decode the pngImageData field.",
            recourse=[
                "Ensure the pngImageData column contains a valid base64-encoded PNG.",
                "Check that the MTurk batch file has not been corrupted.",
            ],
            _original_exc=exc,
        ))

    buf = np.frombuffer(raw_bytes, dtype=np.uint8)
    try:
        mask = cv2.imdecode(buf, cv2.IMREAD_GRAYSCALE)
    except Exception as exc:
        _raise(FriendlyError(
            title="MTurk mask decode failed — cv2.imdecode error",
            message="cv2.imdecode failed to decode the PNG mask.",
            recourse=[
                "Ensure the pngImageData is a valid PNG image.",
                "Verify the base64 string is not truncated.",
            ],
            _original_exc=exc,
        ))

    if mask is None:
        _raise(FriendlyError(
            title="MTurk mask decode failed — null image",
            message=(
                "cv2.imdecode returned None. The pngImageData field may not be "
                "a valid PNG or may be empty."
            ),
            recourse=[
                "Check that pngImageData contains a complete, valid PNG file.",
                "Try decoding the base64 string manually and opening the image.",
            ],
        ))

    return mask  # shape (rows, cols), dtype uint8


def from_mturk_row(
    row: Mapping[str, Any],
    *,
    image_ref: Optional[str] = None,
) -> Annotation:
    """
    Build a canonical Annotation from one row of an MTurk semantic-segmentation
    batch results file.

    Parameters
    ----------
    row       : dict or pandas Series with MTurk result columns:
                  WorkerId        — opaque worker token (→ annotator_id)
                  SubmitTime      — ISO-8601 or MTurk-format timestamp (→ created_at)
                  *.pngImageData  — base64-encoded PNG mask (any column name
                                   ending in '.pngImageData' or 'pngImageData')
    image_ref : Optional opaque image reference (not used in Annotation itself,
                but can be passed by the caller to attach to an AnnotationSet).
                Not stored in the returned Annotation (image_ref lives in
                AnnotationSet, not Annotation).

    Returns
    -------
    Annotation(annotation_type='binary_segmentation', tool='mturk', ...)

    Raises
    ------
    AnnotationError — missing required fields, bad base64, invalid mask, bad WorkerId.

    PHI notes
    ---------
    - WorkerId is an opaque MTurk-assigned alphanumeric token.  It is safe to
      store as annotator_id.  It does NOT contain a human name.
    - SubmitTime is a submission timestamp — not PHI.
    - HITId / AssignmentId are NOT stored here (not needed for the Annotation model).
    """
    # --- 1. Extract WorkerId → annotator_id ---
    worker_id = row.get(_WORKER_ID_KEY) if hasattr(row, "get") else _series_get(row, _WORKER_ID_KEY)
    if not worker_id:
        _raise(FriendlyError(
            title="MTurk row missing WorkerId",
            message="The MTurk batch row does not contain a 'WorkerId' column.",
            recourse=[
                "Ensure the batch results file includes the 'WorkerId' column.",
                "Check that you are passing a full MTurk results row (not a stripped subset).",
            ],
        ))

    # Validate annotator_id — PHI guard (WorkerId is opaque, but we enforce anyway)
    try:
        validate_annotator_id(str(worker_id))
    except AnnotationError:
        raise

    annotator_id = str(worker_id)

    # --- 2. Extract SubmitTime → created_at ---
    submit_time = row.get(_SUBMIT_TIME_KEY) if hasattr(row, "get") else _series_get(row, _SUBMIT_TIME_KEY)
    if submit_time:
        created_at = str(submit_time)
    else:
        from datetime import datetime, timezone
        created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # --- 3. Find and decode pngImageData ---
    png_key = _find_png_field(row)
    if png_key is None:
        _raise(FriendlyError(
            title="MTurk row missing pngImageData",
            message=(
                "No pngImageData field found in the MTurk row. "
                "Expected a column named 'pngImageData' or ending with '.pngImageData'."
            ),
            recourse=[
                "Verify the MTurk annotation task wrote a pngImageData answer field.",
                "Check the column names in your batch results CSV.",
            ],
        ))

    b64_value = row[png_key] if hasattr(row, "__getitem__") else getattr(row, png_key)
    if not b64_value or (isinstance(b64_value, float) and np.isnan(b64_value)):
        _raise(FriendlyError(
            title="MTurk pngImageData is empty",
            message=f"The '{png_key}' column is empty or NaN for this row.",
            recourse=[
                "This worker may have submitted without completing the segmentation.",
                "Filter out rows with empty pngImageData before importing.",
            ],
        ))

    mask = _decode_base64_mask(str(b64_value))

    return Annotation(
        annotator_id=annotator_id,
        tool="mturk",
        annotation_type="binary_segmentation",
        created_at=created_at,
        version=1,
        payload=mask,
    )


def _series_get(obj: Any, key: str, default: Any = None) -> Any:
    """pandas-Series-compatible .get() fallback."""
    try:
        val = obj[key]
        return default if (isinstance(val, float) and np.isnan(val)) else val
    except (KeyError, IndexError, TypeError):
        return default
