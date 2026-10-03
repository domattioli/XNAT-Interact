"""
Analysis intake (spec 016): one safe path for putting analysis results on XNAT.

``run_intake(folder, ...)`` runs the whole cycle in order:

1. read ``analysis.json`` and its analysis type,
2. check that the folder holds exactly the declared outputs,
3. fill in which downloaded inputs were used (from the spec 015 download record),
4. validate structured outputs,
5. run the PHI gate,
6. publish as a new version, 7. download it back and compare,
8. add a row to the ANALYSES catalog.

Every foreseeable problem comes back as a plain-language message with a next
step, never a traceback.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.services.errors import FriendlyError, handle
from src.services.analysis_intake.errors import IntakeRefusal, refuse
from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR, AnalysisType, load_types
from src.services.analysis_intake.descriptor import (
    check_descriptor, load_descriptor, write_descriptor, write_descriptor_template,
)
from src.services.analysis_intake.provenance import MANIFEST_FILENAME, fill_provenance
from src.services.analysis_intake.gates import check_declared_outputs, phi_gate, validate_outputs
from src.services.analysis_intake.publish import PublishOutcome, publish_analysis, verify_published
from src.services.analysis_intake.catalog import ANALYSES_TABLE, record_in_catalog

__all__ = [
    "ANALYSIS_TYPES_DIR", "ANALYSES_TABLE", "AnalysisType", "IntakeRefusal", "IntakeResult", "PublishOutcome",
    "check_declared_outputs", "fill_provenance", "load_descriptor", "load_types", "phi_gate",
    "publish_analysis", "record_in_catalog", "run_intake", "validate_outputs", "verify_published",
    "write_descriptor_template",
]


@dataclass
class IntakeResult:
    """What happened.  ``status`` is refused, dry_run_ok, not_verified, catalog_failed or done."""
    status: str
    label: Optional[str] = None
    friendly: Optional[FriendlyError] = None
    warnings: List[str] = field(default_factory=list)
    descriptor: Optional[Dict[str, Any]] = None
    outcome: Optional[PublishOutcome] = None


def _outcome_from_descriptor(descriptor: Dict[str, Any]) -> PublishOutcome:
    run = descriptor["run"]
    return PublishOutcome(label=run["label"], placement_used=run.get("placement_used", ""), query_string="",
                          resource_label="", descriptor=descriptor, verified=True)


def run_intake(output_folder: Path, *, gateway=None, config_tables=None, username: Optional[str] = None,
               manifest: Optional[Path] = None, inputs: Optional[Path] = None, dry_run: bool = False,
               classifier: Optional[Callable] = None, confirmer: Optional[Callable] = None,
               types_dir: Optional[Path] = None) -> IntakeResult:
    """Run the whole intake cycle on *output_folder*.  See the module docstring."""
    folder = Path(output_folder)
    warnings: List[str] = []
    try:
        types = load_types(types_dir)
        descriptor = load_descriptor(folder)
        atype = check_descriptor(descriptor, types)
        run = descriptor["run"]

        # A folder whose analysis.json already records a confirmed publish only
        # needs its catalog row (the retry path after a catalog failure).
        if run.get("label") and run.get("placement_used"):
            if dry_run:
                return IntakeResult("dry_run_ok", run["label"], warnings=["This result was already published."],
                                    descriptor=descriptor)
            if config_tables is None:
                raise refuse("Catalog not available", "This result is already published; the catalog could not be opened.",
                             ["Log in and run the command again."])
            record_in_catalog(_outcome_from_descriptor(descriptor), descriptor, config_tables)
            return IntakeResult("done", run["label"], warnings=["This result was already published; the catalog is now up to date."],
                                descriptor=descriptor)

        if not atype.publish_via_intake and not dry_run:
            raise refuse("This type is not published here",
                         f"'{atype.type_name}' results are uploaded with their own tool, not with publish-analysis.",
                         ["Use the annotation upload in XNAT-Interact for annotation sets."])

        files = check_declared_outputs(atype, folder)
        if manifest is None and inputs is None and (folder / MANIFEST_FILENAME).is_file():
            manifest = folder / MANIFEST_FILENAME
        descriptor, more = fill_provenance(descriptor, manifest=manifest, inputs=inputs, username=username)
        warnings.extend(more)
        validate_outputs(atype, descriptor, folder, files)
        descriptor["run"]["pixel_confirmation"] = phi_gate(atype, descriptor, folder, files,
                                                          classifier=classifier, confirmer=confirmer)
        if dry_run:
            return IntakeResult("dry_run_ok", warnings=warnings, descriptor=descriptor)
        if gateway is None:
            raise refuse("Not connected to XNAT", "The checks passed, but there is no XNAT connection to publish with.",
                         ["Log in and run the command again."])

        outcome = publish_analysis(descriptor, folder, gateway, atype=atype, files=files)
        if outcome.fallback_reason:
            warnings.append(outcome.fallback_reason)
        if not outcome.verified:
            return IntakeResult("not_verified", outcome.label, refuse(
                "Upload not confirmed",
                f"'{outcome.label}' was sent to XNAT, but the check afterwards found: {'; '.join(outcome.problems)}. "
                "It is NOT counted as published and no catalog row was added.",
                ["Run the command again to publish a fresh version.",
                 "Tell the Data Librarian the label above so the bad copy can be reviewed."]).friendly,
                warnings, outcome.descriptor, outcome)

        write_descriptor(outcome.descriptor, folder)
        if config_tables is None:
            return IntakeResult("catalog_failed", outcome.label, refuse(
                "Catalog not updated", "Your result is on XNAT and confirmed, but the catalog could not be opened.",
                ["Run the same command again later to add the catalog row."]).friendly,
                warnings, outcome.descriptor, outcome)
        try:
            record_in_catalog(outcome, outcome.descriptor, config_tables)
        except IntakeRefusal as exc:
            return IntakeResult("catalog_failed", outcome.label, exc.friendly, warnings, outcome.descriptor, outcome)
        return IntakeResult("done", outcome.label, None, warnings, outcome.descriptor, outcome)
    except IntakeRefusal as exc:
        return IntakeResult("refused", friendly=exc.friendly, warnings=warnings)
    except Exception as exc:  # noqa: BLE001 - last safety net: never show a traceback (Principle II)
        return IntakeResult("refused", friendly=handle(
            exc, title="The analysis intake stopped unexpectedly",
            message="Something went wrong that the tool did not expect. Nothing was marked as published.",
            recourse=["Try again.", "If it happens again, send the diagnostic log named below to the Data Librarian."],
            context="operation=run_intake"), warnings=warnings)
