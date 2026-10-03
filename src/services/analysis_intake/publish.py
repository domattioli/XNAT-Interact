"""
Publish and verify (spec 016, FR-015 to FR-018, FR-020, FR-024).

``publish_analysis`` puts the outputs and the filled ``analysis.json`` on XNAT
as a new version (never overwriting), then downloads everything back and
compares content hashes.  A publish counts as done only when that check passes.
"""
from __future__ import annotations

import copy
import json
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.logic.download_manifest import sha256_of_file
from src.services import xnat_conventions as conventions
from src.services.analysis_intake.errors import IntakeRefusal, refuse
from src.services.analysis_intake.types import AnalysisType
from src.services.analysis_intake.provenance import to_pyxnat_qs

DESCRIPTOR_NAME = "analysis.json"
ASSESSOR_XSI_TYPE = "xnat:assessorData"
_MAX_VERSIONS = 9999


@dataclass
class PublishOutcome:
    """Where a result went and what was sent."""
    label: str
    placement_used: str
    query_string: str
    resource_label: str
    files: Dict[str, str] = field(default_factory=dict)   # server file name -> sha256
    fallback_reason: Optional[str] = None
    descriptor: Dict[str, Any] = field(default_factory=dict)
    verified: bool = False
    problems: List[str] = field(default_factory=list)


def is_permission_error(exc: BaseException) -> bool:
    """True when *exc* (or its cause) says the server refused for permission reasons (HTTP 401 or 403)."""
    seen = set()
    current: Optional[BaseException] = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        for holder in (current, getattr(current, "response", None)):
            status = getattr(holder, "status_code", None) or getattr(holder, "status", None)
            if status in (401, 403):
                return True
        friendly = getattr(current, "friendly", None)
        text = " ".join(str(x) for x in (current, getattr(friendly, "title", ""), getattr(friendly, "message", ""))).lower()
        if any(word in text for word in ("403", "401", "forbidden", "permission", "not authorized", "unauthorized")):
            return True
        current = current.__cause__ or current.__context__
    return False


def _gateway_refusal(exc: BaseException, what: str) -> IntakeRefusal:
    friendly = getattr(exc, "friendly", None)
    if friendly is not None and getattr(friendly, "message", None):
        return IntakeRefusal(friendly)
    return refuse("XNAT did not accept the upload",
                  f"The server could not {what}. Nothing was marked as published.",
                  ["Check your connection to XNAT and try again.",
                   "If it keeps failing, contact the Data Librarian."])


def _scan_label_taken(gateway, scan_qs: str, label: str) -> bool:
    try:
        return bool(gateway.list_files(scan_qs, label))
    except Exception as exc:  # noqa: BLE001 - unknown means we must not risk an overwrite
        raise _gateway_refusal(exc, "list the existing versions")


def _next_scan_label(gateway, scan_qs: str, base: str) -> str:
    for n in range(1, _MAX_VERSIONS):
        label = f"{base}__v{n}"
        if not _scan_label_taken(gateway, scan_qs, label):
            return label
    raise refuse("Too many versions", f"'{base}' already has {_MAX_VERSIONS} versions on this case.",
                 ["Contact the Data Librarian."])


def _supersedes_exists(gateway, exp_qs: str, scan_qs: Optional[str], label: str) -> bool:
    try:
        if label in gateway.list_assessors(exp_qs):
            return True
        if scan_qs and gateway.list_files(scan_qs, label):
            return True
    except Exception as exc:  # noqa: BLE001
        raise _gateway_refusal(exc, "check the version this run replaces")
    return False


def _write_final_descriptor(descriptor: Dict[str, Any], staging: Path) -> Path:
    path = staging / DESCRIPTOR_NAME
    path.write_text(json.dumps(descriptor, indent=2) + "\n", encoding="utf-8")
    return path


def _session_hidden_from_listing(gateway, exp_qs: str, scan_qs: str) -> bool:
    """True when the scan is reachable but its parent session is not visible to ``exists``."""
    try:
        return (not gateway.exists(exp_qs)) and bool(gateway.exists(scan_qs))
    except Exception:  # noqa: BLE001 - an unknown answer must not change the normal path
        return False


def publish_analysis(descriptor: Dict[str, Any], folder: Path, gateway, *, atype: Optional[AnalysisType] = None,
                     files: Optional[List[str]] = None, verify: bool = True) -> PublishOutcome:
    """
    Publish one checked result folder (placement dispatch, keep-all versions, permission fallback, verify).

    *descriptor* must already hold the tool-filled provenance; the gates must already have passed.
    """
    folder = Path(folder)
    if atype is None:
        from src.services.analysis_intake.types import load_types
        atype = load_types()[descriptor["type_name"]]
    if not atype.publish_via_intake:
        raise refuse("This type is not published here",
                     f"'{atype.type_name}' results are uploaded with their own tool, not with publish-analysis.",
                     ["Use the annotation upload in XNAT-Interact for annotation sets."])
    if files is None:
        from src.services.analysis_intake.gates import check_declared_outputs
        files = check_declared_outputs(atype, folder)
    run = descriptor["run"]
    exp_qs = run.get("experiment_query_string")
    if not exp_qs:
        raise refuse("Unknown XNAT session",
                     "The intake could not tell which XNAT session this result belongs to.",
                     ["Download the case with XNAT-Interact so a download record is written, and use it with --manifest."])
    refs = run.get("input_refs") or []
    # Manifest paths are REST plural; pyxnat selects on the singular form.
    scan_qs = to_pyxnat_qs(refs[0]) if refs else None
    sup = run.get("supersedes")
    if sup and not _supersedes_exists(gateway, exp_qs, scan_qs, sup):
        raise refuse("Replaced version not found",
                     f"analysis.json says this run replaces '{sup}', but that label does not exist on this case.",
                     ["Check the label in 'supersedes', or leave it empty."])

    staging = Path(tempfile.mkdtemp(prefix="xnat_intake_"))
    try:
        final = copy.deepcopy(descriptor)
        local: Dict[str, Path] = {rel: folder / rel for rel in files}
        placement = atype.placement
        fallback_reason: Optional[str] = None
        outcome: Optional[PublishOutcome] = None

        if placement == "assessor" and scan_qs and _session_hidden_from_listing(gateway, exp_qs, scan_qs):
            # Found on the local test server (spec 014 SPEC-ISSUE-15 family): the
            # session's files are reachable but the session itself is not listed,
            # so the gateway would refuse the assessor as "parent experiment not
            # found". Store the result on the scan instead and say why.
            fallback_reason = ("The server does not list this session type, so an assessor cannot be attached; "
                               "the result was stored as a scan resource instead.")
            placement = "scan_resource"

        if placement == "assessor":
            base = atype.base_label(run["case_uid"])
            try:
                label = conventions.next_assessor_label(gateway, exp_qs, base)
            except Exception as exc:  # noqa: BLE001
                raise _gateway_refusal(exc, "list the existing versions")
            final["run"].update({"label": label, "placement_used": "assessor", "fallback_reason": None})
            local[DESCRIPTOR_NAME] = _write_final_descriptor(final, staging)
            entries = [(atype.resource_label, name, str(path)) for name, path in local.items()]
            try:
                gateway.create_assessor(exp_qs, label, xsi_type=ASSESSOR_XSI_TYPE, files=entries)
                outcome = PublishOutcome(label, "assessor", f"{exp_qs}/assessor/{label}", atype.resource_label)
            except IntakeRefusal:
                raise
            except Exception as exc:  # noqa: BLE001
                if not is_permission_error(exc):
                    raise _gateway_refusal(exc, "create the result")
                fallback_reason = "The server refused to create an assessor for this account, so the result was stored as a scan resource."
                local.pop(DESCRIPTOR_NAME, None)

        if outcome is None:
            if not scan_qs:
                raise refuse("Unknown scan", "The intake could not tell which scan this result belongs to.",
                             ["Use the download record (--manifest) of the download that holds this case."])
            label = _next_scan_label(gateway, scan_qs, atype.resource_label)
            final["run"].update({"label": label, "placement_used": "scan_resource", "fallback_reason": fallback_reason})
            local[DESCRIPTOR_NAME] = _write_final_descriptor(final, staging)
            for name, path in local.items():
                try:
                    gateway.insert_file(scan_qs, label, name, Path(path).read_bytes(), content="ANALYSIS", format=Path(name).suffix.lstrip(".").upper())
                except Exception as exc:  # noqa: BLE001
                    raise _gateway_refusal(exc, f"store '{name}'")
            outcome = PublishOutcome(label, "scan_resource", scan_qs, label, fallback_reason=fallback_reason)

        outcome.files = {name: sha256_of_file(Path(path)) for name, path in local.items()}
        outcome.descriptor = final
        if verify:
            outcome.problems = verify_published(outcome, gateway)
            outcome.verified = not outcome.problems
        return outcome
    finally:
        shutil.rmtree(staging, ignore_errors=True)


def verify_published(outcome: PublishOutcome, gateway) -> List[str]:
    """
    Download everything just written and compare content hashes (FR-018).

    Returns a list of problems (empty when every file matches).  The temporary
    download folder is always removed.
    """
    problems: List[str] = []
    tmp = Path(tempfile.mkdtemp(prefix="xnat_verify_"))
    try:
        try:
            listed = set(gateway.list_files(outcome.query_string, outcome.resource_label))
            paths = gateway.download_resource(outcome.query_string, outcome.resource_label, tmp)
        except Exception:  # noqa: BLE001
            return ["the result could not be downloaded back from XNAT to check it"]
        got: Dict[str, str] = {}
        for p in paths:
            p = Path(p)
            if p.is_file():
                got.setdefault(p.name, sha256_of_file(p))
        for name, sha in sorted(outcome.files.items()):
            base = Path(name).name
            if name not in listed and base not in listed:
                problems.append(f"'{name}' is missing on the server")
            elif got.get(base) != sha:
                problems.append(f"'{name}' on the server differs from your copy")
        return problems
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
