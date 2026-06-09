"""
Quarantine store for burned-in PHI pixel de-identification (Feature 010, Stage 4).

A quarantined case is held locally for operator review.  It is NEVER
auto-uploaded or auto-over-masked.  A human must inspect the evidence,
apply corrective redaction, and explicitly release the case.

Public API
----------
QuarantineStore(root)
    Manage a local directory of quarantined cases.

    quarantine_case(case_id, assessment, *, frames=None, dataset=None) -> Path
        Write evidence sidecar + optional pixel data; return the case dir.

    list_quarantined() -> list[str]
        Return case IDs currently in the store.

    is_quarantined(case_id) -> bool
        Return True if case_id is in the store.

    release(case_id) -> None
        Remove a case from the store (operator action stub).
        NOTE: a real release workflow must re-run de-id on the pixel data
        before upload.  This method only removes the quarantine record.

Evidence sidecar (evidence.json)
---------------------------------
Keys: verdict, reason, regions, tiers_fired, phi_categories, frame_count,
      timestamp.

PHI SAFETY: raw OCR'd text is NEVER written.  Only region coordinates
(box bounds) and category names are persisted.
"""
from __future__ import annotations

import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import numpy as np

logger = logging.getLogger(__name__)


class QuarantineStore:
    """
    Local quarantine store — holds blocked cases + evidence sidecars.

    Parameters
    ----------
    root:
        Root directory for the quarantine store.  Created if absent.
    """

    def __init__(self, root: "str | Path") -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def quarantine_case(
        self,
        case_id: str,
        assessment: Any,
        *,
        frames: Optional[List[np.ndarray]] = None,
        dataset: Any = None,
    ) -> Path:
        """
        Write a quarantined case to ``root/<case_id>/``.

        Creates:
        - ``evidence.json`` — audit metadata (no raw PHI text).
        - ``frames.npy``    — pixel data (optional, only when *frames* is given).
          Pixel data is local-only quarantine evidence; it is never uploaded.

        Parameters
        ----------
        case_id:
            Unique identifier for this case (e.g. intake-form UID).
        assessment:
            A ``CaseAssessment`` instance (from verdict.assess_case).
        frames:
            Optional list of numpy frame arrays to persist for operator review.
        dataset:
            pydicom Dataset; unused in this implementation (reserved for future
            metadata extraction such as device model).

        Returns
        -------
        Path
            The case directory ``root/<case_id>/``.
        """
        case_dir = self._root / _safe_id(case_id)
        case_dir.mkdir(parents=True, exist_ok=True)

        evidence = _build_evidence(assessment)
        evidence_path = case_dir / "evidence.json"
        evidence_path.write_text(
            json.dumps(evidence, indent=2, default=_json_default),
            encoding="utf-8",
        )

        # Persist pixel data locally for operator review (never uploaded).
        if frames is not None and len(frames) > 0:
            try:
                frames_path = case_dir / "frames.npy"
                # Stack to a single array if uniform shape; otherwise save each
                # frame individually.
                try:
                    stacked = np.stack(frames, axis=0)
                    np.save(str(frames_path), stacked)
                except ValueError:
                    # Non-uniform frame shapes — save as object array.
                    arr = np.empty(len(frames), dtype=object)
                    for i, f in enumerate(frames):
                        arr[i] = f
                    np.save(str(frames_path), arr, allow_pickle=True)
            except Exception as exc:
                logger.warning(
                    "QuarantineStore: could not persist frames for %s: %s",
                    case_id,
                    exc,
                )

        logger.info(
            "QuarantineStore: case '%s' quarantined at %s", case_id, case_dir
        )
        return case_dir

    def list_quarantined(self) -> List[str]:
        """
        Return a list of case IDs currently in the store.

        Only directories containing an ``evidence.json`` are counted as
        quarantined cases.

        Returns
        -------
        list[str]
        """
        result: List[str] = []
        for child in sorted(self._root.iterdir()):
            if child.is_dir() and (child / "evidence.json").exists():
                result.append(child.name)
        return result

    def is_quarantined(self, case_id: str) -> bool:
        """
        Return True if *case_id* is currently in the quarantine store.

        Parameters
        ----------
        case_id:
            Case identifier (same string passed to quarantine_case).

        Returns
        -------
        bool
        """
        case_dir = self._root / _safe_id(case_id)
        return case_dir.is_dir() and (case_dir / "evidence.json").exists()

    def release(self, case_id: str) -> None:
        """
        Release a case from quarantine (operator action stub).

        Removes ``root/<case_id>/`` from the store.

        IMPORTANT: releasing from quarantine does NOT automatically upload the
        case.  A real release workflow must:
          1. Re-run pixel de-id on the frames (or manually apply corrective
             redaction).
          2. Confirm the revised verdict is CLEAN or REDACTED.
          3. Then proceed with the normal upload gate.

        Parameters
        ----------
        case_id:
            Case identifier to release.

        Raises
        ------
        KeyError
            If *case_id* is not in the store.
        """
        case_dir = self._root / _safe_id(case_id)
        if not case_dir.is_dir():
            raise KeyError(
                f"QuarantineStore: case '{case_id}' is not in the store at {self._root}"
            )
        shutil.rmtree(case_dir)
        logger.info("QuarantineStore: case '%s' released from quarantine.", case_id)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _safe_id(case_id: str) -> str:
    """
    Sanitise *case_id* for use as a directory name.

    Replaces path-separator characters with underscores.
    """
    return case_id.replace("/", "_").replace("\\", "_").replace("..", "__")


def _build_evidence(assessment: Any) -> dict:
    """
    Build the evidence dict from a CaseAssessment.

    Keys written:
        verdict        — string value of the Verdict enum.
        reason         — human-readable routing reason.
        regions        — list of (x0, y0, x1, y1) box coordinates.
        tiers_fired    — list of tier names.
        phi_categories — list of category NAME strings (not raw text).
        frame_count    — number of masked frames.
        timestamp      — ISO-8601 UTC timestamp.

    PHI SAFETY: raw OCR text is NEVER included.
    """
    # Normalise verdict — accept Verdict enum or plain string.
    verdict_val = getattr(assessment.verdict, "value", str(assessment.verdict))

    # Normalise regions — list of tuples/lists; store as lists for JSON.
    regions = [list(r) for r in (assessment.regions or [])]

    # phi_categories: category name strings only (already no raw text per
    # verdict.classify_text contract).
    phi_categories = list(assessment.phi_categories or [])

    return {
        "verdict": verdict_val,
        "reason": str(assessment.reason),
        "regions": regions,
        "tiers_fired": list(assessment.tiers_fired or []),
        "phi_categories": phi_categories,
        "frame_count": len(assessment.masked_frames or []),
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }


def _json_default(obj: Any) -> Any:
    """Fallback JSON serialiser for numpy scalars / tuples."""
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")
