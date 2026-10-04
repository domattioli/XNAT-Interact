"""
Model predictions as annotators (spec 019).

A student runs a trained model on one downloaded case and writes
``predictions.json``: one entry per annotation type, each naming the input
frame (by its image identity hash), the annotation type and the payload.  The
intake turns those entries into an ``AnnotationSet`` whose annotator is the
model (``model__<name>__v<version>``) and hands it to the annotation engine's
own ``upload_annotation_set``, so annotation files are still written in one
place only.

This module holds the checks and the conversion:

- ``check_model_fields``: ``run.model`` is present and complete (FR-004, FR-005).
- ``load_entries`` and ``check_entries``: every prediction entry is usable (FR-007).
- ``check_dataset_link``: the training dataset is on the server (FR-006).
- ``build_annotation_set``: entries to an ``AnnotationSet`` (FR-008, FR-014).
- ``verify_annotation_set``: download the set back and compare (FR-011).

Mask payloads are written in ``predictions.json`` in run-length form,
``{"shape": [rows, cols], "runs": [[value, length], ...]}``, or as a plain list
of rows of whole numbers.  Point and box payloads are plain objects such as
``{"x": 10, "y": 20}``.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import numpy as np

from app.logic.download_manifest import sha256_of_file
from src.annotations.exc import AnnotationError
from src.annotations.model import Annotation, AnnotationSet
from src.annotations.model_identity import model_annotator_id
from src.services.analysis_intake.errors import IntakeRefusal, refuse
from src.services.analysis_intake.provenance import to_pyxnat_qs

PREDICTIONS_FILE = "predictions.json"
MODEL_FIELDS = ("name", "version", "dataset_query_string")
TOOL_NAME = "model_predictions"
DESCRIPTOR_NAME = "analysis.json"

_MASK_CODECS = ("rle",)


def _model_help() -> List[str]:
    return ["Open analysis.json and fill in run.model with name, version and dataset_query_string.",
            "Copy dataset_query_string from the catalog row of the training dataset the model was trained on.",
            "Run 'publish-analysis --init model_predictions <new folder>' to see a ready-made example."]


def check_model_fields(descriptor: Dict[str, Any]) -> Dict[str, str]:
    """
    Return ``run.model`` after checking it (FR-004, FR-005).

    The block must hold exactly ``name``, ``version`` and ``dataset_query_string``,
    all filled in, and the name and version must make a valid annotator name.
    """
    model = descriptor.get("run", {}).get("model")
    fields = ", ".join(MODEL_FIELDS)
    if not isinstance(model, dict):
        raise refuse("The model is not described",
                     f"A model_predictions result needs run.model in analysis.json with the fields {fields}.",
                     _model_help())
    extra = sorted(set(model) - set(MODEL_FIELDS))
    missing = [f for f in MODEL_FIELDS if not isinstance(model.get(f), str) or not model.get(f, "").strip()]
    if extra or missing:
        problem = (f"these fields are missing or empty: {', '.join(missing)}" if missing
                   else f"these fields are not allowed: {', '.join(extra)}")
        raise refuse("The model is not described completely",
                     f"run.model in analysis.json must hold exactly {fields}; {problem}.", _model_help())
    try:
        model_annotator_id(model["name"], model["version"])
    except AnnotationError as exc:
        raise IntakeRefusal(exc.friendly)
    return {f: model[f] for f in MODEL_FIELDS}


def load_entries(folder: Path) -> List[Dict[str, Any]]:
    """Read the entries of ``predictions.json`` (the schema check has already run)."""
    path = Path(folder) / PREDICTIONS_FILE
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise refuse("predictions.json could not be read",
                     "predictions.json is missing or is not valid JSON.",
                     ["Check that your model script wrote predictions.json into the folder as JSON."])
    entries = data.get("entries") if isinstance(data, dict) else None
    if not isinstance(entries, list) or not entries:
        raise refuse("predictions.json has no entries",
                     "predictions.json must hold a non-empty list called 'entries'.",
                     ["Write one entry per annotation type, as shown in the quickstart."])
    return entries


def _entry_problem(number: int, why: str) -> IntakeRefusal:
    return refuse("A prediction entry cannot be used",
                  f"In predictions.json, entry {number} {why} Nothing was uploaded.",
                  ["Fix that entry (entries are counted from 1) and run the command again."])


def _mask_from_json(payload: Any) -> np.ndarray:
    """Turn a mask written in predictions.json into a whole-number array, or raise ValueError."""
    if isinstance(payload, dict):
        if set(payload) != {"shape", "runs"}:
            raise ValueError("a mask needs exactly 'shape' and 'runs'")
        shape, runs = payload["shape"], payload["runs"]
        if (not isinstance(shape, list) or len(shape) != 2
                or not all(isinstance(n, int) and not isinstance(n, bool) and n > 0 for n in shape)):
            raise ValueError("'shape' must be two whole numbers above zero")
        values: List[np.ndarray] = []
        for run in runs if isinstance(runs, list) else [None]:
            if (not isinstance(run, list) or len(run) != 2
                    or not all(isinstance(n, int) and not isinstance(n, bool) for n in run) or run[1] < 1):
                raise ValueError("each run must be [value, length] with whole numbers")
            values.append(np.full(run[1], run[0], dtype=np.int64))
        flat = np.concatenate(values) if values else np.array([], dtype=np.int64)
        if flat.size != shape[0] * shape[1]:
            raise ValueError("the runs do not add up to rows times columns")
        arr = flat.reshape(shape[0], shape[1])
    elif isinstance(payload, list):
        arr = np.array(payload)
        if arr.ndim != 2 or arr.size == 0 or arr.dtype.kind not in "iu":
            raise ValueError("a mask must be a list of equally long rows of whole numbers")
    else:
        raise ValueError("a mask must be written as shape plus runs")
    if arr.min() >= 0 and arr.max() <= 255:
        return arr.astype(np.uint8)
    return arr.astype(np.int32)


def decode_payload(annotation_type: str, payload: Any) -> Any:
    """
    Turn an entry's JSON payload into the payload the annotation engine stores.

    Raises ``AnnotationError`` when the type is not registered and
    ``ValueError`` (or ``AnnotationError``) when the payload does not fit the
    type or its codec.
    """
    from src.annotations.codecs import get_codec
    from src.annotations.registry import get_type
    entry = get_type(annotation_type)
    value = _mask_from_json(payload) if entry.codec in _MASK_CODECS else payload
    entry.validator(value)
    get_codec(entry.codec).encode(value)          # the payload must encode with the type's codec
    return value


def check_entries(entries: List[Dict[str, Any]], source_hashes: List[Dict[str, Any]]) -> List[Any]:
    """
    Check every prediction entry before anything is written (FR-007).

    Returns the decoded payloads in entry order.  The first problem is refused,
    naming the entry number (counted from 1).
    """
    from src.annotations.registry import list_types
    known = set()
    for h in source_hashes or []:
        if isinstance(h, dict):
            known |= {h.get("image_identity_hash"), h.get("sha256")}
    known.discard(None)
    seen_types: Dict[str, int] = {}
    payloads: List[Any] = []
    for number, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise _entry_problem(number, "is not an object with image_identity_hash, annotation_type and payload.")
        frame = entry.get("image_identity_hash")
        if frame not in known:
            raise _entry_problem(number, "names a frame (image_identity_hash) that is not one of the downloaded "
                                         "input frames in the download record.")
        atype = entry.get("annotation_type")
        if atype not in list_types():
            raise _entry_problem(number, f"has the annotation type '{atype}', which is not registered "
                                         f"(registered: {', '.join(list_types())}).")
        if atype in seen_types:
            raise _entry_problem(number, f"repeats the annotation type '{atype}' of entry {seen_types[atype]}; "
                                         "write one entry per annotation type.")
        seen_types[atype] = number
        try:
            payloads.append(decode_payload(atype, entry.get("payload")))
        except AnnotationError as exc:
            raise _entry_problem(number, f"has a payload that does not fit '{atype}': {exc.friendly.message}")
        except (ValueError, TypeError) as exc:
            raise _entry_problem(number, f"has a payload that does not fit '{atype}': {exc}.")
    return payloads


def _dataset_help() -> List[str]:
    return ["Open the ANALYSES catalog, find the row of the training dataset, and copy its address "
            "(QUERY_STRING followed by /resources/ and RESOURCE_LABEL) into run.model.dataset_query_string.",
            "Publish the training dataset first if it is not on XNAT yet."]


def check_dataset_link(gateway, dataset_query_string: str) -> None:
    """
    Refuse unless the training dataset named in ``run.model`` is on the server (FR-006).

    The address is ``<project query string>/resources/<dataset label>``; the
    resource must list an ``analysis.json``.  Nothing is written here.
    """
    address = str(dataset_query_string or "")
    owner, sep, dataset_label = address.rpartition("/resources/")
    if not sep or not owner or not dataset_label or "/" in dataset_label:
        raise refuse("Training dataset address is not valid",
                     f"run.model.dataset_query_string '{address}' is not of the form "
                     "<project address>/resources/<dataset label>. Nothing was uploaded.", _dataset_help())
    # The catalog writes the project address as /project/<ID>; a REST copy such as
    # /data/projects/<ID> names the same place.
    if owner.startswith("/data/"):
        owner = owner[len("/data"):]
    owner = to_pyxnat_qs(owner)
    try:
        files = gateway.list_files(owner, dataset_label)
    except Exception:  # noqa: BLE001 - an unknown answer must not let the publish go ahead
        raise refuse("Training dataset could not be checked",
                     f"XNAT could not list the training dataset at '{address}', so it is not known to exist. "
                     "Nothing was uploaded.", _dataset_help() + ["Check your connection to XNAT and try again."])
    if DESCRIPTOR_NAME not in [Path(str(f)).name for f in (files or [])]:
        raise refuse("Training dataset not found",
                     f"There is no published training dataset at '{address}' (no analysis.json there). "
                     "Nothing was uploaded.", _dataset_help())


def build_annotation_set(scan_qs: str, run: Dict[str, Any], entries: List[Dict[str, Any]]) -> AnnotationSet:
    """
    Build the model's ``AnnotationSet`` for one scan (FR-008, FR-014).

    One ``Annotation`` per entry: the annotator is the model's name, the tool is
    ``model_predictions``, the version is 1 and ``derived`` is false (a model's
    output is a direct annotation, not a consensus).
    """
    model = run["model"]
    annotator = model_annotator_id(model["name"], model["version"])
    payloads = check_entries(entries, run.get("source_hashes") or [])
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    aset = AnnotationSet(image_ref=scan_qs)
    for entry, payload in zip(entries, payloads):
        aset.add(Annotation(annotator, TOOL_NAME, entry["annotation_type"], now, 1, derived=False, payload=payload))
    return aset


def _same_payload(a: Any, b: Any) -> bool:
    if isinstance(a, np.ndarray) or isinstance(b, np.ndarray):
        return isinstance(a, np.ndarray) and isinstance(b, np.ndarray) and a.shape == b.shape and np.array_equal(a, b)
    return a == b


def verify_annotation_set(outcome, gateway, entries: List[Dict[str, Any]]) -> List[str]:
    """
    Download the published set and its ``analysis.json`` back and compare (FR-011).

    Returns a list of problems (empty when everything matches).  Payloads are
    compared after decoding; ``analysis.json`` is compared by content hash.
    The temporary folder is always removed.
    """
    model = outcome.descriptor["run"]["model"]
    annotator = model_annotator_id(model["name"], model["version"])
    from src.annotations.io_xnat import download_annotation_set
    problems: List[str] = []
    tmp = Path(tempfile.mkdtemp(prefix="xnat_verify_"))
    try:
        got = download_annotation_set(gateway, outcome.query_string, tmp / "set", resource_label=outcome.resource_label)
        if not got.ok or got.annotation_set is None:
            problems.append("the annotation set could not be downloaded back from XNAT to check it")
        else:
            downloaded = {(a.annotator_id, a.annotation_type): a for a in got.annotation_set.annotations}
            for number, entry in enumerate(entries, start=1):
                kind = entry["annotation_type"]
                back = downloaded.get((annotator, kind))
                if back is None:
                    problems.append(f"entry {number} ('{kind}') is missing on the server")
                elif not _same_payload(back.payload, decode_payload(kind, entry["payload"])):
                    problems.append(f"entry {number} ('{kind}') on the server differs from your copy")
        want = outcome.files.get(DESCRIPTOR_NAME)
        dest = tmp / DESCRIPTOR_NAME
        try:
            gateway.get_file_copy(outcome.query_string, outcome.resource_label, DESCRIPTOR_NAME, dest)
            same = dest.is_file() and sha256_of_file(dest) == want
        except Exception:  # noqa: BLE001
            same = False
        if not same:
            problems.append(f"'{DESCRIPTOR_NAME}' on the server differs from your copy")
        return problems
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
