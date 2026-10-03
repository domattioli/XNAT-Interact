"""
Trivial STAPLE stand-in for live-XNAT integration tests.

Implements a majority-vote consensus aggregator for binary segmentation masks,
with per-segmenter agreement scores and per-pixel uncertainty estimates.

Registered only for this suite's session (T012 in conftest.py); does not modify
src/annotations/aggregate/staple.py.
"""
from __future__ import annotations

from typing import Any, Dict, List

import numpy as np

from src.annotations.aggregate import Aggregator, register_aggregator
from src.annotations.model import ConsensusResult
from src.annotations.exc import AnnotationError
from src.services.errors import FriendlyError


class StapleStub(Aggregator):
    """
    Trivial STAPLE stand-in using majority-vote consensus.

    Consensus Algorithm
    --------------------
    Per pixel: the label that appears in the most masks.
    If tied (e.g., all masks disagree), consensus takes the value that appears
    most frequently across all masks at that location; ties break arbitrarily
    (numpy argmax behavior).

    Per-Segmenter Scores
    --------------------
    Agreement rate = fraction of pixels where each segmenter's mask matches
    the consensus mask.  (Not real STAPLE sensitivity/specificity, but sufficient
    for this stand-in.)

    Uncertainty Map
    ---------------
    Per-pixel disagreement fraction = (number of masks disagreeing with consensus)
    / (total number of masks).  Ranges from 0 (unanimous) to (n-1)/n (all but
    one mask disagree).

    Payload Packing
    ---------------
    The consensus mask, scores dict, and uncertainty map are packed into
    ConsensusResult.payload as a small dict:
        {
            "consensus_mask": np.ndarray,
            "scores": {segmenter_id: agreement_rate, ...},
            "uncertainty_map": np.ndarray
        }

    This packing is necessary because ConsensusResult does not currently have
    named fields for individual scores and uncertainty — they live in `payload`
    (typed `Any`) alongside the consensus mask.
    """

    def aggregate(self, payloads: List[Any], **ctx) -> ConsensusResult:  # noqa: ANN002
        """
        Aggregate binary segmentation masks via majority vote.

        Parameters
        ----------
        payloads : list
            Non-empty list of binary segmentation masks (np.ndarray, dtype
            typically uint8 or bool), one per segmenter. All masks must have
            the same shape.
        **ctx    : Caller-supplied context. Expects 'annotator_ids' if
                   per-segmenter scores are desired. If absent, uses
                   generic labels like "segmenter_0", "segmenter_1", etc.

        Returns
        -------
        ConsensusResult
            - payload: dict with keys "consensus_mask", "scores", "uncertainty_map"
            - per_annotator: map of input masks keyed by annotator_id
            - method: "staple"

        Raises
        ------
        AnnotationError
            If payloads is empty, or if mask shapes don't match.
        """
        if not payloads:
            raise AnnotationError(FriendlyError(
                title="Empty payload list",
                message="aggregate() received an empty list — at least one mask is required.",
                recourse=["Ensure at least one segmenter produced output."],
            ))

        # Convert all payloads to numpy arrays
        masks = []
        for p in payloads:
            if isinstance(p, np.ndarray):
                masks.append(p)
            else:
                try:
                    masks.append(np.asarray(p))
                except Exception as e:
                    raise AnnotationError(FriendlyError(
                        title="Invalid mask payload",
                        message=f"Could not convert payload to array: {e}",
                        recourse=["Ensure all payloads are 2D/3D numpy arrays or array-like."],
                    ))

        # Verify all masks have the same shape
        first_shape = masks[0].shape
        for i, m in enumerate(masks[1:], start=1):
            if m.shape != first_shape:
                raise AnnotationError(FriendlyError(
                    title="Shape mismatch in masks",
                    message=(
                        f"Mask 0 has shape {first_shape}; mask {i} has shape {m.shape}. "
                        "All masks must have the same shape."
                    ),
                    recourse=["Ensure all segmenters produced same-sized output."],
                ))

        # --- Majority-vote consensus ---
        stacked = np.stack(masks, axis=0)  # shape: (n_masks, *mask_shape)

        # Per-pixel mode (majority vote). axis=0 is the mask-index dimension.
        # For each pixel, count votes for each label and pick the most common.
        consensus = np.apply_along_axis(
            lambda votes: np.bincount(votes.astype(int)).argmax(),
            axis=0,
            arr=stacked.astype(int),
        )

        # --- Per-segmenter agreement score ---
        scores = {}
        annotator_ids = ctx.get("annotator_ids")
        if annotator_ids is None:
            annotator_ids = [f"segmenter_{i}" for i in range(len(masks))]

        for i, (mask, annotator_id) in enumerate(zip(masks, annotator_ids)):
            # Agreement = fraction of pixels where mask == consensus
            agreement = np.mean(mask.astype(int) == consensus.astype(int))
            scores[annotator_id] = float(agreement)

        # --- Per-pixel uncertainty (disagreement fraction) ---
        disagreement_count = np.sum(
            stacked.astype(int) != consensus[np.newaxis, ...],
            axis=0,
        )
        uncertainty_map = disagreement_count / len(masks)

        # --- Pack into payload dict ---
        payload = {
            "consensus_mask": consensus,
            "scores": scores,
            "uncertainty_map": uncertainty_map,
        }

        # --- Build per_annotator map ---
        per_annotator = {}
        for annotator_id, mask in zip(annotator_ids, masks):
            per_annotator[annotator_id] = mask

        return ConsensusResult(
            payload=payload,
            per_annotator=per_annotator,
            method="staple",
        )


def register() -> None:
    """
    Register the StapleStub for this session.

    Call this from conftest.py (T012) before any test runs.
    """
    register_aggregator("staple", StapleStub())
