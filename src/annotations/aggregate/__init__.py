"""
annotations.aggregate — Pluggable aggregator seam (Task B, T006–T009).

THE SEAM
--------
New aggregation methods register by name via ``register_aggregator()``.
``TypeEntry.default_aggregator`` in ``registry.py`` is a *string*; this
module resolves it.  Zero changes to core modules (model/registry/codecs/
validate) are needed to add a new method.

Public API
----------
Aggregator          Protocol / ABC: one method → ConsensusResult.
register_aggregator register by name (THE extensibility hook).
get_aggregator      look up by name → raises AnnotationError if unknown.
list_aggregators    introspection.
aggregate_set       convenience: AnnotationSet + type_name → ConsensusResult.
"""
from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional

from src.annotations.model import AnnotationSet, ConsensusResult
from src.annotations.exc import AnnotationError
from src.services.errors import FriendlyError


# ---------------------------------------------------------------------------
# Protocol / ABC
# ---------------------------------------------------------------------------

class Aggregator(abc.ABC):
    """
    Pluggable aggregator interface.

    Implement ``aggregate`` and register an instance via
    ``register_aggregator(name, instance)``.

    Parameters (aggregate)
    ----------------------
    payloads : list
        Decoded payloads — one per annotator (same annotation_type).
        May be np.ndarray (mask) or dict (landmark / bbox).
    **ctx    : caller-supplied context (e.g. image_ref, annotation_type).
               Aggregators may use or ignore these.

    Returns
    -------
    ConsensusResult
        ``payload``       — aggregated output.
        ``per_annotator`` — empty dict (or caller-supplied map if populated upstream).
        ``method``        — name string matching the registry key.
    """

    @abc.abstractmethod
    def aggregate(self, payloads: List[Any], **ctx) -> ConsensusResult:
        ...  # pragma: no cover


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_AGGREGATOR_REGISTRY: Dict[str, Aggregator] = {}


def register_aggregator(name: str, aggregator: Aggregator) -> None:
    """
    Register an aggregator under *name*.

    This is the ONLY change needed to add a new method — zero edits to
    core modules (model/registry/codecs/validate).

    Parameters
    ----------
    name        : Unique string key (e.g. ``'reference'``, ``'staple'``).
    aggregator  : Instance of a class implementing the Aggregator interface.
    """
    _AGGREGATOR_REGISTRY[name] = aggregator


def get_aggregator(name: str) -> Aggregator:
    """
    Return the registered Aggregator for *name*.

    Raises AnnotationError (friendly, no raw traceback) if not found.
    """
    if name not in _AGGREGATOR_REGISTRY:
        raise AnnotationError(FriendlyError(
            title="Unknown aggregator",
            message=(
                f"No aggregator registered under '{name}'. "
                f"Known aggregators: {list(_AGGREGATOR_REGISTRY)}."
            ),
            recourse=[
                "Use 'reference' (built-in) or register a custom aggregator via "
                "register_aggregator(name, instance) before calling aggregate_set().",
            ],
        ))
    return _AGGREGATOR_REGISTRY[name]


def list_aggregators() -> List[str]:
    """Return sorted list of all registered aggregator names."""
    return sorted(_AGGREGATOR_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Convenience entry point
# ---------------------------------------------------------------------------

def aggregate_set(
    annotation_set: AnnotationSet,
    type_name: str,
    aggregator_name: Optional[str] = None,
) -> ConsensusResult:
    """
    Aggregate all latest annotations of *type_name* in *annotation_set*.

    Algorithm
    ---------
    1. Call ``annotation_set.latest_per_annotator()`` → keep entries where
       ``annotation_type == type_name``.
    2. Resolve aggregator: use *aggregator_name* if given, else look up
       ``TypeEntry.default_aggregator`` from the annotation type registry.
    3. Call ``aggregator.aggregate(payloads, image_ref=..., annotation_type=...)``.
    4. Return the ConsensusResult with ``method`` set by the aggregator.

    Parameters
    ----------
    annotation_set   : AnnotationSet to aggregate over.
    type_name        : Registered annotation type name (e.g. 'binary_segmentation').
    aggregator_name  : Override aggregator name.  If None, the type's
                       ``default_aggregator`` string is used.

    Returns
    -------
    ConsensusResult  — ``derived=True`` is NOT set here (caller may wrap in
                       an Annotation if persistence is needed); the method
                       field records which aggregator ran.
    """
    # Gather latest payloads for this type
    latest = annotation_set.latest_per_annotator()
    matching = {
        ann_id: ann
        for (ann_id, ann_type), ann in latest.items()
        if ann_type == type_name
    }
    payloads = [ann.payload for ann in matching.values()]

    # Resolve aggregator name
    if aggregator_name is None:
        from src.annotations.registry import get_type
        entry = get_type(type_name)
        aggregator_name = entry.default_aggregator

    agg = get_aggregator(aggregator_name)
    return agg.aggregate(
        payloads,
        image_ref=annotation_set.image_ref,
        annotation_type=type_name,
    )


# ---------------------------------------------------------------------------
# Auto-register built-ins on import
# ---------------------------------------------------------------------------
# Import triggers ReferenceAggregator's module-level register_aggregator() call.
from src.annotations.aggregate import reference as _ref_module  # noqa: E402, F401
