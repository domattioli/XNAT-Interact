"""
The analysis descriptor, ``analysis.json`` (spec 016, FR-003 to FR-005).

The analyst writes four fields (case_uid, code_ref, parameters, notes); the
tool fills in the rest.  The tool always writes JSON.  It reads
``analysis.yaml`` only when a YAML reader is installed, which it is not in the
standard project environment.
"""
from __future__ import annotations

import json
import os
import re
import tempfile
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from src.services.analysis_intake.errors import refuse
from src.services.analysis_intake.types import AnalysisType, load_types

DESCRIPTOR_JSON = "analysis.json"
DESCRIPTOR_YAML = "analysis.yaml"
DESCRIPTOR_VERSION = "1"

_ANALYST_RUN_KEYS = {"case_uid", "code_ref", "parameters", "notes", "supersedes"}
_TOOL_RUN_KEYS = {
    "source_hashes", "input_refs", "manifest_run_id", "provenance", "producer",
    "experiment_query_string", "label", "placement_used", "fallback_reason",
    "pixel_confirmation", "intake_started_at", "tool_version",
}
_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.\-]*__v[0-9]+$")

_TEMPLATE_HINT = "Run 'publish-analysis --init <type_name> <folder>' to get a ready-made analysis.json."


def _yaml_reader() -> Optional[Callable[[str], Any]]:
    try:
        import yaml  # type: ignore  # optional; not part of the standard environment
    except Exception:  # noqa: BLE001 - any import problem means "no YAML reader"
        return None
    return yaml.safe_load


def load_descriptor(output_folder: Path, *, yaml_loader: Optional[Callable[[str], Any]] = None) -> Dict[str, Any]:
    """Read the descriptor from *output_folder*.  Does not check it against a type yet."""
    folder = Path(output_folder)
    if not folder.is_dir():
        raise refuse("Folder not found", f"The output folder '{folder}' does not exist.",
                     ["Check the folder path and try again."])
    json_path = folder / DESCRIPTOR_JSON
    yaml_path = folder / DESCRIPTOR_YAML
    if json_path.is_file():
        try:
            data = json.loads(json_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise refuse("analysis.json is not valid JSON",
                         f"analysis.json could not be read: the JSON breaks at line {exc.lineno}, column {exc.colno}.",
                         ["Fix that line (a missing comma or quote is the usual cause).", _TEMPLATE_HINT])
        except (OSError, UnicodeDecodeError):
            raise refuse("analysis.json could not be read", "analysis.json exists but could not be opened as text.",
                         [_TEMPLATE_HINT])
    elif yaml_path.is_file():
        reader = yaml_loader or _yaml_reader()
        if reader is None:
            raise refuse("YAML descriptor cannot be read here",
                         "This folder has analysis.yaml, but this computer has no YAML reader installed.",
                         ["Write the same fields as JSON in a file named analysis.json.", _TEMPLATE_HINT])
        try:
            data = reader(yaml_path.read_text(encoding="utf-8"))
        except Exception:  # noqa: BLE001 - any parse problem is shown the same way
            raise refuse("analysis.yaml could not be read", "analysis.yaml is not valid YAML.",
                         ["Fix the file, or write it as analysis.json instead.", _TEMPLATE_HINT])
    else:
        raise refuse("No analysis.json in this folder",
                     f"The folder '{folder.name}' has no analysis.json describing the result.", [_TEMPLATE_HINT])
    if not isinstance(data, dict):
        raise refuse("Descriptor is not valid", "The descriptor must be a JSON object with fields.", [_TEMPLATE_HINT])
    return data


def _field_problem(field: str, why: str):
    return refuse("Descriptor is missing information",
                  f"In analysis.json, the field '{field}' {why}",
                  ["Open analysis.json, fix that field, and run the command again."])


def check_descriptor(descriptor: Dict[str, Any], types: Dict[str, AnalysisType]) -> AnalysisType:
    """Check the descriptor against its type file (FR-005) and return the type."""
    version = descriptor.get("descriptor_version", DESCRIPTOR_VERSION)
    if version != DESCRIPTOR_VERSION:
        raise _field_problem("descriptor_version", f"must be \"{DESCRIPTOR_VERSION}\".")
    extra = set(descriptor) - {"descriptor_version", "type_name", "type_version", "run"}
    if extra:
        raise _field_problem(sorted(extra)[0], "is not a known field.")
    name = descriptor.get("type_name")
    if not isinstance(name, str) or not name:
        raise _field_problem("type_name", "is required.")
    if name not in types:
        raise refuse("Unknown analysis type",
                     f"analysis.json names the type '{name}', which is not registered.",
                     [f"Registered types: {', '.join(sorted(types)) or 'none'}.",
                      "Ask the maintainer to register a new type through a pull request."])
    atype = types[name]
    tver = descriptor.get("type_version")
    if tver != atype.type_version:
        raise refuse("Analysis type version does not match",
                     f"analysis.json says type_version {tver!r}, but '{name}' is at version {atype.type_version}.",
                     [f"Set type_version to {atype.type_version} if your outputs follow the current type."])
    run = descriptor.get("run")
    if not isinstance(run, dict):
        raise _field_problem("run", "is required and must hold case_uid and code_ref.")
    for key in run:
        if key not in _ANALYST_RUN_KEYS | _TOOL_RUN_KEYS:
            raise _field_problem(f"run.{key}", "is not a known field.")
    for key in ("case_uid", "code_ref"):
        value = run.get(key)
        if not isinstance(value, str) or not value.strip():
            raise _field_problem(f"run.{key}", "must be filled in.")
    if "parameters" in run and not isinstance(run["parameters"], dict):
        raise _field_problem("run.parameters", "must be a set of name and value pairs ({}).")
    if "notes" in run and not isinstance(run["notes"], str):
        raise _field_problem("run.notes", "must be text.")
    sup = run.get("supersedes")
    if sup not in (None, "") and (not isinstance(sup, str) or not _LABEL_RE.match(sup)):
        raise _field_problem("run.supersedes", "must be a published label such as knee_flexion_angle__v1, or empty.")
    return atype


def write_descriptor(descriptor: Dict[str, Any], output_folder: Path) -> Path:
    """Write *descriptor* as ``analysis.json`` (always JSON, FR-004), replacing it atomically."""
    folder = Path(output_folder)
    dest = folder / DESCRIPTOR_JSON
    data = (json.dumps(descriptor, indent=2, sort_keys=False) + "\n").encode("utf-8")
    fd, tmp = tempfile.mkstemp(prefix=".analysis-", suffix=".json", dir=str(folder))
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, dest)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)
    return dest


def descriptor_template(atype: AnalysisType) -> Dict[str, Any]:
    """The skeleton a student fills in."""
    return {
        "descriptor_version": DESCRIPTOR_VERSION,
        "type_name": atype.type_name,
        "type_version": atype.type_version,
        "run": {"case_uid": "", "code_ref": "", "parameters": {}, "notes": "", "supersedes": None},
    }


def write_descriptor_template(type_name: str, output_folder: Path, *, types: Optional[Dict[str, AnalysisType]] = None) -> Path:
    """Write an ``analysis.json`` skeleton for *type_name* (US4).  Never overwrites."""
    types = types if types is not None else load_types()
    if type_name not in types:
        raise refuse("Unknown analysis type", f"There is no registered analysis type called '{type_name}'.",
                     [f"Registered types: {', '.join(sorted(types)) or 'none'}."])
    folder = Path(output_folder)
    folder.mkdir(parents=True, exist_ok=True)
    if (folder / DESCRIPTOR_JSON).exists():
        raise refuse("analysis.json already exists",
                     f"The folder '{folder.name}' already has an analysis.json, so the template was not written.",
                     ["Edit the existing analysis.json, or move it away and run the template command again."])
    return write_descriptor(descriptor_template(types[type_name]), folder)
