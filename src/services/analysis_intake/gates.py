"""
The checks that run before anything leaves the laptop (spec 016, FR-006, FR-010 to FR-013).

1. Declaration check: every declared output is present and nothing else is.
2. Validation gate: structured outputs match their schema and row counts.
3. PHI gate: the type's PHI policies, failing closed when a check cannot run.
"""
from __future__ import annotations

import csv
import fnmatch
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from app.logic.download_manifest import image_identity_of_file, sha256_of_file
from src.services.analysis_intake.errors import refuse
from src.services.analysis_intake.types import AnalysisType, OutputSpec

# Files that may sit in the output folder but are never uploaded.
COMPANION_FILES = {"analysis.json", "analysis.yaml", "download_manifest.json", ".DS_Store"}
TEXT_SUFFIXES = {".csv", ".json", ".txt", ".md", ".yaml", ".yml"}


# ---------------------------------------------------------------------------
# 1. Declaration check
# ---------------------------------------------------------------------------

def _match(spec: OutputSpec, rel: str) -> bool:
    return fnmatch.fnmatchcase(rel, spec.pattern)


def check_declared_outputs(atype: AnalysisType, output_folder: Path) -> List[str]:
    """Return the output files to upload (relative paths), or refuse (FR-006)."""
    folder = Path(output_folder)
    rels = sorted(p.relative_to(folder).as_posix() for p in folder.rglob("*")
                  if p.is_file() and p.name not in COMPANION_FILES and not p.name.startswith(".analysis-"))
    extra = [r for r in rels if not any(_match(s, r) for s in atype.outputs)]
    if extra:
        raise refuse("Unexpected file in the output folder",
                     f"These files are not outputs of '{atype.type_name}': {', '.join(extra)}. "
                     "Stray files are the usual way patient details leak, so nothing was uploaded.",
                     ["Remove or move those files out of the folder.",
                      "If the type should include them, ask the maintainer to extend the type file."])
    missing = [s.pattern for s in atype.outputs if not any(_match(s, r) for r in rels)]
    if missing:
        raise refuse("Expected output is missing",
                     f"The folder has no file matching: {', '.join(missing)}.",
                     ["Check that your analysis wrote all its outputs into this folder."])
    return rels


def spec_for(atype: AnalysisType, rel: str) -> Optional[OutputSpec]:
    """The first output spec whose pattern matches *rel*."""
    for spec in atype.outputs:
        if _match(spec, rel):
            return spec
    return None


# ---------------------------------------------------------------------------
# 2. Validation gate (a small JSON Schema subset, so no extra package is needed)
# ---------------------------------------------------------------------------

_TYPE_CHECKS: Dict[str, Callable[[Any], bool]] = {
    "object": lambda v: isinstance(v, dict),
    "array": lambda v: isinstance(v, list),
    "string": lambda v: isinstance(v, str),
    "integer": lambda v: isinstance(v, int) and not isinstance(v, bool),
    "number": lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    "boolean": lambda v: isinstance(v, bool),
    "null": lambda v: v is None,
}


def schema_problem(value: Any, schema: Dict[str, Any], where: str = "") -> Optional[str]:
    """Return the first way *value* breaks *schema*, in plain words, or None."""
    expected = schema.get("type")
    if expected is not None:
        kinds = expected if isinstance(expected, list) else [expected]
        if not any(_TYPE_CHECKS.get(k, lambda v: True)(value) for k in kinds):
            return f"{where or 'value'} should be {' or '.join(kinds)}"
    if "enum" in schema and value not in schema["enum"]:
        return f"{where or 'value'} must be one of {schema['enum']}"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            return f"{where or 'value'} is below the minimum {schema['minimum']}"
        if "maximum" in schema and value > schema["maximum"]:
            return f"{where or 'value'} is above the maximum {schema['maximum']}"
    if isinstance(value, str):
        if "minLength" in schema and len(value) < schema["minLength"]:
            return f"{where or 'value'} is too short"
        if "pattern" in schema and not re.search(schema["pattern"], value):
            return f"{where or 'value'} does not have the expected form"
    if isinstance(value, dict):
        for req in schema.get("required", []):
            if req not in value:
                return f"'{req}' is missing"
        props = schema.get("properties", {})
        if schema.get("additionalProperties") is False:
            for key in value:
                if key not in props:
                    return f"'{key}' is not an allowed column or field"
        for key, sub in props.items():
            if key in value:
                problem = schema_problem(value[key], sub, f"'{key}'")
                if problem:
                    return problem
    if isinstance(value, list) and isinstance(schema.get("items"), dict):
        for i, item in enumerate(value):
            problem = schema_problem(item, schema["items"], f"item {i}")
            if problem:
                return problem
    return None


def _convert_cell(raw: str, prop: Dict[str, Any]) -> Any:
    if raw is None or raw == "":
        return None
    kinds = prop.get("type")
    kinds = kinds if isinstance(kinds, list) else [kinds]
    if "integer" in kinds:
        try:
            return int(raw)
        except ValueError:
            pass
    if "number" in kinds:
        try:
            return float(raw)
        except ValueError:
            pass
    return raw


def _load_schema(atype: AnalysisType, spec: OutputSpec) -> Dict[str, Any]:
    path = atype.schema_path(spec)
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise refuse("Output schema is missing",
                     f"The type '{atype.type_name}' points to a schema file '{spec.schema}' that could not be read.",
                     ["Ask the maintainer to fix the type file."])


def _invalid(rel: str, where: str, problem: str):
    return refuse("Output file does not match its type",
                  f"In '{rel}', {where}: {problem}.",
                  ["Fix the output (or the analysis that wrote it) and run the command again."])


def validate_outputs(atype: AnalysisType, descriptor: Dict[str, Any], output_folder: Path, files: List[str]) -> None:
    """Check structured outputs against their schema and row counts (FR-010)."""
    folder = Path(output_folder)
    run = descriptor["run"]
    hashes = run.get("source_hashes") or []
    identities = {h.get("image_identity_hash") for h in hashes if h.get("image_identity_hash")}
    for rel in files:
        spec = spec_for(atype, rel)
        if spec is None:
            continue
        if spec.format == "csv" and (spec.schema or spec.rows_per_input):
            schema = _load_schema(atype, spec) if spec.schema else {}
            row_schema = schema.get("items", schema) if schema else {}
            props = row_schema.get("properties", {})
            try:
                with open(folder / rel, newline="", encoding="utf-8") as fh:
                    rows = list(csv.DictReader(fh))
            except (OSError, UnicodeDecodeError, csv.Error):
                raise _invalid(rel, "the whole file", "it could not be read as a CSV table")
            for i, row in enumerate(rows):
                if None in row:
                    raise _invalid(rel, f"row {i + 1} (line {i + 2})", "it has more cells than the header")
                values = {k: _convert_cell(v, props.get(k, {})) for k, v in row.items() if v not in (None, "")}
                problem = schema_problem(values, row_schema) if row_schema else None
                if problem:
                    raise _invalid(rel, f"row {i + 1} (line {i + 2})", problem)
                ident = row.get("image_identity_hash")
                if ident and identities and ident not in identities:
                    raise _invalid(rel, f"row {i + 1} (line {i + 2})",
                                   "its image_identity_hash is not one of the input frames")
            if spec.rows_per_input and len(rows) != len(hashes):
                raise _invalid(rel, "the whole file",
                               f"it has {len(rows)} rows but the analysis used {len(hashes)} input frames")
        elif spec.format == "json" and spec.schema:
            schema = _load_schema(atype, spec)
            try:
                doc = json.loads((folder / rel).read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, ValueError):
                raise _invalid(rel, "the whole file", "it is not valid JSON")
            problem = schema_problem(doc, schema)
            if problem:
                raise _invalid(rel, "the content", problem)


# ---------------------------------------------------------------------------
# 3. PHI gate
# ---------------------------------------------------------------------------

def pixel_kind(path: Path) -> Optional[str]:
    """Return 'dicom', 'png', 'jpeg', 'mp4' or 'npz' when the file holds pixels, judged by content."""
    try:
        with open(path, "rb") as fh:
            head = fh.read(132)
    except OSError:
        return "unreadable"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "png"
    if head.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    if len(head) >= 12 and head[4:8] == b"ftyp":
        return "mp4"
    if len(head) >= 132 and head[128:132] == b"DICM":
        return "dicom"
    if head.startswith(b"PK\x03\x04") and path.suffix.lower() == ".npz":
        return "npz"
    if path.suffix.lower() in {".dcm", ".dicom"} or not path.suffix:
        try:
            import pydicom
            pydicom.dcmread(str(path), force=False, stop_before_pixels=True)
            return "dicom"
        except Exception:  # noqa: BLE001 - not DICOM
            return None
    return None


class ClassifierUnavailable(Exception):
    """The PHI text classifier could not be loaded on this machine."""


def default_classifier() -> Callable[[str], Tuple[bool, List[str]]]:
    """
    Return the production text classifier, or raise ``ClassifierUnavailable``.

    ``classify_text`` quietly reports "no PHI" when its language engine cannot
    load, which would let names through.  So the engine is loaded here first,
    and any failure stops the gate (fail closed, FR-012).
    """
    try:
        from src.services.pixel_deid import verdict
        verdict._get_analyzer()
    except Exception as exc:  # noqa: BLE001 - any failure means the check cannot run
        raise ClassifierUnavailable(type(exc).__name__) from exc
    return verdict.classify_text


_MACHINE_TOKEN_RE = re.compile(r"^([0-9a-fA-F]{16,}|[0-9][0-9.]*|/\S*|-?\d+(\.\d+)?([eE][-+]?\d+)?)$")


def _is_machine_token(text: str) -> bool:
    return bool(_MACHINE_TOKEN_RE.match(text.strip()))


def _text_items(path: Path, rel: str) -> List[Tuple[str, str]]:
    """Return (where, text) pairs to scan from one text file."""
    suffix = path.suffix.lower()
    raw = path.read_text(encoding="utf-8")
    items: List[Tuple[str, str]] = []
    if suffix == ".csv":
        for line_no, row in enumerate(csv.reader(raw.splitlines()), start=1):
            for cell in row:
                if cell.strip() and not _is_machine_token(cell):
                    items.append((f"line {line_no}", cell))
    elif suffix == ".json":
        def walk(node: Any, where: str) -> None:
            if isinstance(node, dict):
                for k, v in node.items():
                    walk(v, f"{where}.{k}" if where else str(k))
            elif isinstance(node, list):
                for i, v in enumerate(node):
                    walk(v, f"{where}[{i}]")
            elif isinstance(node, str) and node.strip() and not _is_machine_token(node):
                items.append((f"field {where or '(top)'}", node))
        walk(json.loads(raw), "")
    else:
        for line_no, line in enumerate(raw.splitlines(), start=1):
            if line.strip():
                items.append((f"line {line_no}", line))
    return items


def _phi_found(rel: str, where: str, categories: List[str]):
    kinds = ", ".join(categories) or "possible PHI"
    return refuse("Possible patient information found",
                  f"In '{rel}', {where} looks like it holds {kinds}. The text is not shown here, and nothing was uploaded.",
                  ["Remove the patient detail from that place and run the command again.",
                   "If you are sure it is not patient information, ask the Data Librarian."])


def _scan_text(classifier, rel: str, items: List[Tuple[str, str]]) -> None:
    for where, text in items:
        try:
            is_phi, categories = classifier(text)
        except Exception:  # noqa: BLE001 - a failing check must block, not pass
            raise refuse("PHI text check failed",
                         f"The patient-information check could not finish on '{rel}', so nothing was uploaded.",
                         ["Try again; if it keeps failing, contact the Data Librarian."])
        if is_phi:
            raise _phi_found(rel, where, list(categories or []))


def _npz_is_mask(path: Path) -> bool:
    try:
        import numpy as np
        with np.load(str(path), allow_pickle=False) as data:
            for key in data.files:
                arr = data[key]
                if arr.dtype.kind not in "biu":
                    return False
        return True
    except Exception:  # noqa: BLE001 - unreadable means not a valid mask
        return False


def phi_gate(atype: AnalysisType, descriptor: Dict[str, Any], output_folder: Path, files: List[str], *,
             classifier: Optional[Callable[[str], Tuple[bool, List[str]]]] = None,
             confirmer: Optional[Callable[[str], Any]] = None) -> str:
    """
    Apply the type's PHI policies (FR-011 to FR-013).

    Returns "not_required" or "confirmed" (the human pixel confirmation).
    Refuses on any finding, and on any check that cannot run.
    """
    folder = Path(output_folder)
    policy = set(atype.phi_policy)
    run = descriptor["run"]
    hashes = run.get("source_hashes") or []
    known = {h.get("image_identity_hash") for h in hashes if h.get("image_identity_hash")}
    known |= {h.get("sha256") for h in hashes if h.get("sha256")}

    for rel in files:
        path = folder / rel
        kind = pixel_kind(path)
        if kind == "unreadable":
            raise refuse("File could not be checked", f"'{rel}' could not be opened, so it cannot be checked for patient information.",
                         ["Check the file and run the command again."])
        if kind is None:
            continue
        if "no_pixels" in policy:
            raise refuse("Image found in a no-image result",
                         f"'{rel}' holds image data ({kind}), but '{atype.type_name}' results must not contain images. Nothing was uploaded.",
                         ["Remove the image from the folder; copied source images are the usual cause."])
        if "pixels_from_source_only" in policy:
            spec = spec_for(atype, rel)
            if kind == "npz":
                if not (spec and spec.mask and _npz_is_mask(path)):
                    raise refuse("Array file is not a plain mask",
                                 f"'{rel}' must be a declared mask holding only whole-number labels.",
                                 ["Save masks as whole-number (label) arrays, or ask the maintainer to declare the file as a mask."])
                continue
            ident = image_identity_of_file(path) if kind == "dicom" else None
            if ident not in known and sha256_of_file(path) not in known:
                raise refuse("Image is not one of the input frames",
                             f"'{rel}' is an image that does not match any downloaded input frame, so it may carry patient details.",
                             ["Only upload images that are unchanged input frames or declared masks."])

    if "text_scan" in policy:
        if classifier is None:
            try:
                classifier = default_classifier()
            except ClassifierUnavailable:
                raise refuse("PHI text check is not available",
                             "The check for patient names and dates in text cannot run on this computer, so nothing was uploaded.",
                             ["Ask the Data Librarian to install the PHI checking tools (requirements-pixeldeid.txt).",
                              "Or contact the Data Librarian to publish this result for you."])
        notes = run.get("notes") or ""
        _scan_text(classifier, "analysis.json", [(f"notes line {i}", line) for i, line in
                                                  enumerate(notes.splitlines(), start=1) if line.strip()])
        params = run.get("parameters") or {}
        _scan_text(classifier, "analysis.json", [(f"parameter '{k}'", v) for k, v in params.items()
                                                  if isinstance(v, str) and v.strip() and not _is_machine_token(v)])
        for rel in files:
            path = folder / rel
            if path.suffix.lower() not in TEXT_SUFFIXES:
                continue
            try:
                items = _text_items(path, rel)
            except (OSError, UnicodeDecodeError, ValueError):
                raise refuse("File could not be checked",
                             f"'{rel}' could not be read as text, so it cannot be checked for patient information.",
                             ["Save the file as plain UTF-8 text and run the command again."])
            _scan_text(classifier, rel, items)

    if "pixels_from_source_only" in policy:
        if confirmer is None:
            from src.xnat_experiment_data import _interactive_pixel_review_confirmer as confirmer  # noqa: N813
        context = (f"Analysis result '{atype.type_name}' for case {run.get('case_uid')}: please confirm that the "
                   "images in this folder show no burned-in patient details.")
        try:
            answer = confirmer(context)
        except Exception:  # noqa: BLE001 - no clear yes means no
            answer = None
        decision = answer[0] if isinstance(answer, tuple) else answer
        if getattr(decision, "value", decision) != "confirmed":
            raise refuse("Image check not confirmed",
                         "A person must confirm that the images show no patient details before upload, and that did not happen.",
                         ["Review the images, then run the command again and confirm."])
        return "confirmed"
    return "not_required"
