"""
annotations.aggregate.reference — Reference aggregator.

WARNING: THIS IS A REFERENCE IMPLEMENTATION ONLY.
It is intentionally simple and has NOT been validated as a clinical or
research-grade consensus method.  Do not use for clinical decisions.

Algorithm
---------
- 2-D integer MASK payloads (np.ndarray)  → per-pixel MAJORITY VOTE (mode).
- Scalar / dict payloads (landmark {x,y}, bbox {x,y,w,h}) → element-wise MEAN.
- N = 1  → consensus is that single payload (no computation).
- N = 0  → AnnotationError (clear message, no raw traceback).
- Shape mismatch (masks) → AnnotationError (FriendlyError, no raw traceback).

Registration
------------
Registered as ``'reference'`` at module import.  ``registry.py`` sets
``default_aggregator='reference'`` for all four built-in annotation types,
so this is the default path until an explicit override is registered.
"""
from __future__ import annotations

from typing import Any, List

import numpy as np

from src.annotations.model import ConsensusResult
from src.annotations.exc import AnnotationError
from src.annotations.aggregate import Aggregator, register_aggregator
from src.services.errors import FriendlyError

_METHOD_NAME = "reference"


class ReferenceAggregator(Aggregator):
    """
    Reference-only aggregator.  NOT a validated consensus method.

    Mask payloads   → per-pixel majority vote (scipy.stats.mode or numpy argmax).
    Dict payloads   → element-wise mean of each key.
    Scalar payloads → arithmetic mean.
    """

    def aggregate(self, payloads: List[Any], **ctx) -> ConsensusResult:  # noqa: ANN002
        """
        Aggregate *payloads* using the reference algorithm.

        Parameters
        ----------
        payloads : list
            Non-empty list of decoded annotation payloads (all same type).
        **ctx    : Ignored by this aggregator (image_ref, annotation_type, etc.).

        Returns
        -------
        ConsensusResult with method='reference'.
        """
        if len(payloads) == 0:
            raise AnnotationError(FriendlyError(
                title="Empty payload list",
                message="aggregate() received an empty list — at least one annotator payload is required.",
                recourse=[
                    "Ensure annotation_set contains at least one annotation of the requested type.",
                    "Call aggregate_set() only after annotations have been added.",
                ],
            ))

        # N = 1 fast-path
        if len(payloads) == 1:
            return ConsensusResult(
                payload=payloads[0],
                per_annotator={},
                method=_METHOD_NAME,
            )

        first = payloads[0]

        # --- MASK branch (np.ndarray) ---
        if isinstance(first, np.ndarray):
            consensus = self._majority_vote_masks(payloads)

        # --- DICT branch (landmark / bbox) ---
        elif isinstance(first, dict):
            consensus = self._mean_dict(payloads)

        # --- Scalar branch ---
        else:
            consensus = float(np.mean([float(p) for p in payloads]))

        return ConsensusResult(
            payload=consensus,
            per_annotator={},
            method=_METHOD_NAME,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _majority_vote_masks(payloads: List[np.ndarray]) -> np.ndarray:
        """Per-pixel majority vote across a list of masks (same shape)."""
        shape = payloads[0].shape
        for i, mask in enumerate(payloads[1:], start=1):
            if mask.shape != shape:
                raise AnnotationError(FriendlyError(
                    title="Mask shape mismatch",
                    message=(
                        f"All mask payloads must have the same shape for aggregation. "
                        f"Payload 0 has shape {shape}; payload {i} has shape {mask.shape}."
                    ),
                    recourse=[
                        "Ensure all annotations in the set use the same image resolution.",
                        "Resize or re-encode masks to a common shape before aggregating.",
                    ],
                ))

        # Stack → (N, H, W), then per-pixel argmax over label counts
        stack = np.stack(payloads, axis=0).astype(np.int64)  # (N, H, W)
        # Majority vote: for binary masks (0/1), sum > N/2 → 1, else 0.
        # For multi-label: use bincount per pixel (works for labels 0..K).
        n = stack.shape[0]
        votes = stack.sum(axis=0)
        consensus = (votes > n / 2).astype(stack.dtype)
        return consensus

    @staticmethod
    def _mean_dict(payloads: List[dict]) -> dict:
        """Element-wise mean of numeric values in dict payloads."""
        keys = list(payloads[0].keys())
        result = {}
        for k in keys:
            vals = [float(p[k]) for p in payloads]
            result[k] = float(np.mean(vals))
        return result


# ---------------------------------------------------------------------------
# Auto-register at import
# ---------------------------------------------------------------------------
register_aggregator(_METHOD_NAME, ReferenceAggregator())
