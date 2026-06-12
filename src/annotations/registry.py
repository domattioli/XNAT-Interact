"""
Annotation type registry.

Maps annotation type names to their codec, payload validator, and default
aggregator NAME. The aggregator name is a string resolved later from a
separate aggregator registry (Task B). No aggregator classes are imported here.

Built-in types:
    binary_segmentation — single-channel uint8 mask (0/1)
    label_map           — multi-label int32 mask
    landmark            — {x, y} point dict
    bbox                — {x, y, w, h} bounding-box dict
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List

from src.services.errors import FriendlyError
from src.annotations.exc import AnnotationError


def _raise(fe: FriendlyError) -> None:
    """Raise AnnotationError wrapping a FriendlyError. Never returns."""
    raise AnnotationError(fe)


@dataclass
class TypeEntry:
    """
    Registry entry for an annotation type.

    Fields
    ------
    codec               : Name of the codec used to encode/decode payloads.
    validator           : Callable that validates a decoded payload (raises on failure).
    default_aggregator  : NAME of the default aggregator (string only — resolved
                          later from the Task-B aggregator registry; NOT imported here).
    """
    codec: str
    validator: Callable
    default_aggregator: str


_REGISTRY: Dict[str, TypeEntry] = {}


def register_type(
    name: str,
    codec: str,
    validator: Callable,
    default_aggregator: str,
) -> None:
    """
    Register an annotation type.

    Parameters
    ----------
    name                : Unique type name (e.g. 'binary_segmentation').
    codec               : Codec name (must match a registered codec in codecs.py).
    validator           : Callable(payload) -> None; raises AnnotationError on bad payload.
    default_aggregator  : Name string for the default aggregator (Task B resolves this).
    """
    _REGISTRY[name] = TypeEntry(
        codec=codec,
        validator=validator,
        default_aggregator=default_aggregator,
    )


def get_type(name: str) -> TypeEntry:
    """
    Return the TypeEntry for the given annotation type name.

    Raises AnnotationError if the type is not registered.
    """
    if name not in _REGISTRY:
        _raise(FriendlyError(
            title="Unknown annotation type",
            message=f"No annotation type registered under '{name}'. Known types: {list(_REGISTRY)}.",
            recourse=[
                "Use one of the built-in types: binary_segmentation, label_map, landmark, bbox.",
                "Register a custom type via register_type() if needed.",
            ],
        ))
    return _REGISTRY[name]


def list_types() -> List[str]:
    """Return sorted list of all registered annotation type names."""
    return sorted(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Register the 4 built-in annotation types
# ---------------------------------------------------------------------------
# Import validators and codec names here — not aggregator classes.
from src.annotations.validate import (  # noqa: E402
    validate_mask_payload,
    validate_landmark_payload,
    validate_bbox_payload,
)

register_type(
    name="binary_segmentation",
    codec="rle",
    validator=validate_mask_payload,
    default_aggregator="reference",
)

register_type(
    name="label_map",
    codec="rle",
    validator=validate_mask_payload,
    default_aggregator="reference",
)

register_type(
    name="landmark",
    codec="json_scalar",
    validator=validate_landmark_payload,
    default_aggregator="reference",
)

register_type(
    name="bbox",
    codec="json_scalar",
    validator=validate_bbox_payload,
    default_aggregator="reference",
)
