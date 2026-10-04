"""
Annotator names for trained models (spec 019, FR-005).

A model's predictions are stored as one more annotator.  The annotation engine
only accepts annotator names made of letters, digits, underscores and hyphens,
so a model called ``knee-seg`` at version ``3`` becomes the annotator
``model__knee-seg__v3``.

The model name and the version may hold only letters, digits and hyphens.
Because neither part can contain an underscore, the double underscores in the
built name are always separators, and the name can be read back without doubt.
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

from src.annotations.exc import AnnotationError
from src.services.errors import FriendlyError

MODEL_PREFIX = "model__"

_PART_RE = re.compile(r"^[A-Za-z0-9-]+$")
_MAX_NAME = 64
_MAX_VERSION = 32
_MODEL_ID_RE = re.compile(r"^model__([A-Za-z0-9-]+)__v([A-Za-z0-9-]+)$")


def _check_part(what: str, value: object, max_len: int) -> str:
    """Return *value* when it is a valid name part; otherwise raise a friendly error."""
    if not isinstance(value, str) or not value or len(value) > max_len or not _PART_RE.match(value):
        suggestion = re.sub(r"[^A-Za-z0-9-]+", "-", str(value)).strip("-")[:max_len] or "my-model"
        raise AnnotationError(FriendlyError(
            title=f"Model {what} cannot be used",
            message=(f"The model {what} '{value}' may hold only letters, digits and hyphens "
                     f"(1 to {max_len} characters)."),
            recourse=[f"Use hyphens instead of other characters, for example '{suggestion}'.",
                      "Change the value in run.model in analysis.json and run the command again."],
        ))
    return value


def model_annotator_id(name: str, version: str) -> str:
    """Build the annotator name of a model: ``model__<name>__v<version>``."""
    name = _check_part("name", name, _MAX_NAME)
    version = _check_part("version", version, _MAX_VERSION)
    return f"{MODEL_PREFIX}{name}__v{version}"


def parse_model_annotator_id(aid: str) -> Optional[Tuple[str, str]]:
    """Read a model annotator name back into ``(name, version)``; ``None`` when it is not a model's name."""
    if not isinstance(aid, str):
        return None
    match = _MODEL_ID_RE.match(aid)
    if match is None:
        return None
    name, version = match.group(1), match.group(2)
    if len(name) > _MAX_NAME or len(version) > _MAX_VERSION:
        return None
    return name, version


def is_model_annotator(aid: str) -> bool:
    """True when *aid* is the annotator name of a model, False for a person's name."""
    return parse_model_annotator_id(aid) is not None
