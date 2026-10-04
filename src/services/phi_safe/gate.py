"""
Output gate: an allowlist grammar for everything the PHI-safe probes emit.

The grammar is closed. A value is accepted only if it is one of:

- ``bool``
- ``int`` (not a float, not a numpy scalar unless converted first)
- a string that is a member of :data:`ENUMS`
- a string of exactly 8 lowercase hex characters, only under ``device_digest``
- a box: a tuple or list of exactly four ints
- a list of accepted values
- a dict whose keys are all in :data:`ALLOWED_KEYS` and whose values are accepted

Nothing else passes. There is no blocklist and no pattern matching on free
text, because a blocklist can be wrong and a grammar cannot leak what it has
no production for.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Dict, FrozenSet, List, Union

ALLOWED_KEYS: FrozenSet[str] = frozenset({
    # report envelope
    "schema", "blocked", "error_class", "probes",
    # census
    "files", "dicom", "parsed", "parse_failed", "non_dcm_extension", "secondary_capture",
    "burned_in_yes", "multiframe", "empty_study_date", "modalities", "identifying_tags",
    # identifying tag presence counts
    "PatientName", "PatientID", "PatientBirthDate", "OtherPatientIDs", "PatientAddress",
    "AccessionNumber", "InstitutionName", "ReferringPhysicianName", "PerformingPhysicianName",
    "OperatorsName", "StudyDate", "SeriesDate", "AcquisitionDate", "ContentDate", "StudyID",
    "StationName", "InstitutionAddress", "DeviceSerialNumber",
    # modality counts
    "RF", "XA", "OT", "CR", "DX", "SC", "other",
    # devices and detectors
    "devices", "device_digest", "profiled", "frames_sampled", "tesseract", "onnx_model",
    # verdicts
    "verdicts", "verdict", "reason", "tiers_fired", "regions",
    # banner finder
    "banner", "rows", "cols", "frames_used", "boxes", "constant_fraction_pct",
    # intake acceptance
    "intake", "accepted", "rejected_extension",
})

ENUMS: FrozenSet[str] = frozenset({
    "phi-safe-report/1",
    "confirmed", "redact", "abort", "quarantine",
    "unprofiled_no_evidence", "phi_outside_mask", "profile_detector_disagree", "clean", "other",
    "none",
})

_HEX8 = re.compile(r"^[0-9a-f]{8}$")
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")

Accepted = Union[bool, int, str, List[Any], Dict[str, Any]]


class GateError(ValueError):
    """Raised when a value is outside the allowlist grammar. Carries no payload."""

    def __init__(self) -> None:
        super().__init__("value outside the PHI-safe output grammar")


def digest(text: str) -> str:
    """Return the first 8 hex characters of sha256 over ``text``.

    Use only on values that are not PHI (a device model key). A digest of a
    low-entropy PHI value could be brute-forced, so the probes never call this
    on a patient tag.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:8]


def check(value: Any, *, key: str | None = None) -> Accepted:
    """Validate ``value`` against the grammar and return a plain-Python copy.

    Raises :class:`GateError` on the first violation. The error carries no
    part of the offending value.
    """
    if key is not None and key not in ALLOWED_KEYS:
        raise GateError()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return int(value)
    if hasattr(value, "item") and not isinstance(value, (str, bytes)):
        # numpy scalar: unwrap, then re-check so floats are still refused
        return check(value.item(), key=key)
    if isinstance(value, str):
        if value in ENUMS:
            return value
        # An 8-digit date is also 8 hex characters, so a digest passes only
        # under the one key that is defined to hold one.
        if key == "device_digest" and _HEX8.match(value):
            return value
        if key == "error_class" and _IDENT.match(value):
            return value
        raise GateError()
    if isinstance(value, (tuple, list)):
        items = list(value)
        if (
            len(items) == 4
            and all(isinstance(i, int) and not isinstance(i, bool) for i in items)
            and key == "boxes"
        ):
            return [int(i) for i in items]
        if key == "boxes":
            # Under ``boxes`` only a box, or a flat list of boxes, is legal.
            out_boxes = []
            for i in items:
                if not (
                    isinstance(i, (tuple, list)) and len(i) == 4
                    and all(isinstance(c, int) and not isinstance(c, bool) for c in i)
                ):
                    raise GateError()
                out_boxes.append([int(c) for c in i])
            return out_boxes
        return [check(i) for i in items]
    if isinstance(value, dict):
        out: Dict[str, Accepted] = {}
        for k, v in value.items():
            if not isinstance(k, str) or k not in ALLOWED_KEYS:
                raise GateError()
            out[k] = check(v, key=k)
        return out
    raise GateError()


class SafeReport:
    """Accumulates gated key/value pairs and renders them as JSON."""

    def __init__(self) -> None:
        self._data: Dict[str, Accepted] = {"schema": "phi-safe-report/1"}

    def put(self, key: str, value: Any) -> None:
        self._data[key] = check(value, key=key)

    def to_dict(self) -> Dict[str, Accepted]:
        return dict(check(self._data))

    def render(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


def blocked(exc: BaseException) -> str:
    """Render the fixed report used when a run fails for any reason."""
    report = SafeReport()
    report.put("blocked", True)
    name = type(exc).__name__
    report.put("error_class", name if _IDENT.match(name) else "other")
    return report.render()
