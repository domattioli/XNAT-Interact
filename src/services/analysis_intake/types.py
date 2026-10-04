"""
Analysis types (spec 016, FR-001, FR-002).

An analysis type is a reviewed JSON file in ``analysis_types/`` that says what
one kind of result looks like: its output files, where it goes on XNAT, and
which PHI checks it needs.  The tool only reads these files; new types arrive
as repository changes reviewed by the maintainer.

Validation is done by hand here so the tool needs no extra package at run
time.  The offline tests also check every type file against the JSON Schema in
``analysis_types/schemas/analysis-type.schema.json``.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.services.analysis_intake.errors import refuse

REPO_ROOT = Path(__file__).resolve().parents[3]
ANALYSIS_TYPES_DIR = REPO_ROOT / "analysis_types"

PLACEMENTS = ("assessor", "scan_resource", "project_resource")
PHI_POLICIES = ("no_pixels", "pixels_from_source_only", "text_scan")
OUTPUT_FORMATS = ("csv", "json", "txt", "md", "npz", "rle", "png", "dcm")

_TYPE_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{1,47}$")
_INPUT_RE = re.compile(r"^(source_frames|annotations|dataset|assessor:[a-z][a-z0-9_]*)$")
_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_TEMPLATE_RE = re.compile(r"^[A-Za-z0-9_\-{}]+$")
_SCHEMA_REF_RE = re.compile(r"^schemas/[A-Za-z0-9_.-]+\.json$")

_TOP_KEYS = {
    "type_name", "type_version", "description", "inputs", "outputs", "placement",
    "resource_label", "phi_policy", "publish_via_intake", "label_template", "multi_case",
}
_REQUIRED = ("type_name", "type_version", "description", "inputs", "outputs",
             "placement", "resource_label", "phi_policy")
_OUTPUT_KEYS = {"pattern", "format", "schema", "rows_per_input", "mask"}


@dataclass(frozen=True)
class OutputSpec:
    """One expected output file pattern of a type."""
    pattern: str
    format: str
    schema: Optional[str] = None
    rows_per_input: bool = False
    mask: bool = False


@dataclass(frozen=True)
class AnalysisType:
    """A loaded, checked analysis type file."""
    type_name: str
    type_version: int
    description: str
    inputs: Tuple[str, ...]
    outputs: Tuple[OutputSpec, ...]
    placement: str
    resource_label: str
    phi_policy: Tuple[str, ...]
    publish_via_intake: bool = True
    label_template: str = "{type_name}"
    folder: Path = field(default=ANALYSIS_TYPES_DIR, compare=False)
    # Spec 018: True when one result is built from several cases (a training
    # dataset).  Such a result lives on the project, so only the
    # project_resource placement may set it.
    multi_case: bool = False

    def base_label(self, case_uid: str) -> str:
        """The un-versioned label, from ``label_template`` (FR-015)."""
        return self.label_template.format(type_name=self.type_name, case_uid=case_uid)

    def schema_path(self, output: OutputSpec) -> Optional[Path]:
        """Where the output's JSON Schema file lives, if it declares one."""
        return (self.folder / output.schema) if output.schema else None


def _bad(path: Path, field_name: str, why: str):
    return refuse(
        "Analysis type file is not valid",
        f"The analysis type file '{path.name}' has a problem in the field '{field_name}': {why}",
        ["Ask the maintainer to correct this type file; type files change only through a reviewed pull request."],
    )


def check_type_data(data: Any, path: Path) -> AnalysisType:
    """Check one parsed type file and return it as an ``AnalysisType``."""
    if not isinstance(data, dict):
        raise _bad(path, "(whole file)", "it must be a JSON object.")
    for key in _REQUIRED:
        if key not in data:
            raise _bad(path, key, "this field is required.")
    for key in data:
        if key not in _TOP_KEYS:
            raise _bad(path, key, "this field is not allowed.")
    name = data["type_name"]
    if not isinstance(name, str) or not _TYPE_NAME_RE.match(name):
        raise _bad(path, "type_name", "use lowercase letters, digits and underscores.")
    if path.suffix == ".json" and path.stem != name:
        raise _bad(path, "type_name", f"it must match the file name ('{path.stem}').")
    version = data["type_version"]
    if not isinstance(version, int) or isinstance(version, bool) or version < 1:
        raise _bad(path, "type_version", "it must be a whole number of 1 or more.")
    desc = data["description"]
    if not isinstance(desc, str) or not desc.strip() or len(desc) > 200:
        raise _bad(path, "description", "write one plain sentence of at most 200 characters.")
    inputs = data["inputs"]
    if not isinstance(inputs, list) or not inputs or not all(isinstance(i, str) and _INPUT_RE.match(i) for i in inputs):
        raise _bad(path, "inputs", "allowed values are source_frames, annotations, dataset, assessor:<type>.")
    outputs_raw = data["outputs"]
    if not isinstance(outputs_raw, list) or not outputs_raw:
        raise _bad(path, "outputs", "list at least one expected output.")
    outputs: List[OutputSpec] = []
    for item in outputs_raw:
        if not isinstance(item, dict) or "pattern" not in item or "format" not in item:
            raise _bad(path, "outputs", "each output needs a 'pattern' and a 'format'.")
        extra = set(item) - _OUTPUT_KEYS
        if extra:
            raise _bad(path, "outputs", f"unknown output field(s): {', '.join(sorted(extra))}.")
        pattern = item["pattern"]
        if not isinstance(pattern, str) or not pattern or pattern.startswith("/") or ".." in pattern:
            raise _bad(path, "outputs", "a pattern must be a relative file pattern without '..'.")
        if item["format"] not in OUTPUT_FORMATS:
            raise _bad(path, "outputs", f"format must be one of {', '.join(OUTPUT_FORMATS)}.")
        schema = item.get("schema")
        if schema is not None and (not isinstance(schema, str) or not _SCHEMA_REF_RE.match(schema)):
            raise _bad(path, "outputs", "a schema must be written as schemas/<name>.json.")
        for flag in ("rows_per_input", "mask"):
            if flag in item and not isinstance(item[flag], bool):
                raise _bad(path, "outputs", f"'{flag}' must be true or false.")
        outputs.append(OutputSpec(pattern, item["format"], schema,
                                  bool(item.get("rows_per_input", False)), bool(item.get("mask", False))))
    placement = data["placement"]
    if placement not in PLACEMENTS:
        raise _bad(path, "placement", f"allowed values are {', '.join(PLACEMENTS)}.")
    label = data["resource_label"]
    if not isinstance(label, str) or not _LABEL_RE.match(label):
        raise _bad(path, "resource_label", "use letters, digits and underscores, starting with a letter.")
    policy = data["phi_policy"]
    if (not isinstance(policy, list) or not policy or len(set(policy)) != len(policy)
            or not all(p in PHI_POLICIES for p in policy)):
        raise _bad(path, "phi_policy", f"allowed values are {', '.join(PHI_POLICIES)}, each at most once.")
    if "no_pixels" in policy and "pixels_from_source_only" in policy:
        raise _bad(path, "phi_policy", "no_pixels and pixels_from_source_only cannot be used together.")
    via = data.get("publish_via_intake", True)
    if not isinstance(via, bool):
        raise _bad(path, "publish_via_intake", "it must be true or false.")
    template = data.get("label_template", "{type_name}")
    if not isinstance(template, str) or not _TEMPLATE_RE.match(template):
        raise _bad(path, "label_template", "use letters, digits, '_', '-' and the placeholders {type_name} or {case_uid}.")
    try:
        template.format(type_name="x", case_uid="y")
    except (KeyError, IndexError, ValueError):
        raise _bad(path, "label_template", "only the placeholders {type_name} and {case_uid} are allowed.")
    multi_case = data.get("multi_case", False)
    if not isinstance(multi_case, bool):
        raise _bad(path, "multi_case", "it must be true or false.")
    if multi_case and placement != "project_resource":
        raise _bad(path, "multi_case", "only a type with placement project_resource can span several cases.")
    return AnalysisType(name, version, desc, tuple(inputs), tuple(outputs), placement, label,
                        tuple(policy), via, template, path.parent, multi_case)


def load_type_file(path: Path) -> AnalysisType:
    """Read and check one type file."""
    path = Path(path)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError) as exc:
        raise _bad(path, "(whole file)", f"it could not be read ({type(exc).__name__}).")
    except json.JSONDecodeError as exc:
        raise _bad(path, "(whole file)", f"it is not valid JSON (line {exc.lineno}).")
    return check_type_data(data, path)


def load_types(folder: Optional[Path] = None) -> Dict[str, AnalysisType]:
    """Load every ``*.json`` type file in *folder* (default ``analysis_types/``)."""
    folder = Path(folder) if folder is not None else ANALYSIS_TYPES_DIR
    if not folder.is_dir():
        raise refuse(
            "No analysis types found",
            f"The folder of analysis types '{folder.name}' does not exist.",
            ["Run the tool from a complete copy of XNAT-Interact, or ask the maintainer."],
        )
    types: Dict[str, AnalysisType] = {}
    for path in sorted(folder.glob("*.json")):
        atype = load_type_file(path)
        types[atype.type_name] = atype
    return types
