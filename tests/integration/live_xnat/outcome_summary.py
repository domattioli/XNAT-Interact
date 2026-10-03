"""
FR-019: Outcome summary writer for live-XNAT integration tests.

Produces machine-readable per-case JSON matching contracts/outcome-summary.schema.json.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


PHASES = (
    "ingestion",
    "deidentification",
    "annotation",
    "segmentation",
    "staple",
    "integrity_validation",
)


@dataclass
class PipelinePhaseRecord:
    """
    Result record for one pipeline phase (ingestion, de-id, annotation, etc.).

    Fields mirror the JSON schema in contracts/outcome-summary.schema.json.
    """

    case_name: str
    phase: str  # one of: ingestion, deidentification, annotation, segmentation, staple, integrity_validation
    status: str  # one of: success, failure, partial
    timestamp: str  # ISO-8601 datetime string
    files_affected: List[str] = field(default_factory=list)
    xnat_resource_links: List[str] = field(default_factory=list)
    error_detail: Optional[str] = field(default=None)


@dataclass
class OutcomeSummary:
    """
    Complete outcome summary for one case run.

    Serialises to JSON matching contracts/outcome-summary.schema.json.
    """

    case_name: str
    run_timestamp: str  # ISO-8601
    files_processed: int
    files_recovered: int
    files_rejected: int
    checksum_pass_count: int
    checksum_fail_count: int
    uid_collisions_resolved: int
    uid_collisions_recorded: int = 0
    absent_tags: Dict[str, List[str]] = field(default_factory=dict)
    dispositions: Dict[str, str] = field(default_factory=dict)
    scan_layout: Dict[str, Any] = field(default_factory=dict)
    phases: List[PipelinePhaseRecord] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to schema-compatible dict."""
        return {
            "case_name": self.case_name,
            "run_timestamp": self.run_timestamp,
            "files_processed": self.files_processed,
            "files_recovered": self.files_recovered,
            "files_rejected": self.files_rejected,
            "checksum_pass_count": self.checksum_pass_count,
            "checksum_fail_count": self.checksum_fail_count,
            "uid_collisions_resolved": self.uid_collisions_resolved,
            "uid_collisions_recorded": self.uid_collisions_recorded,
            "absent_tags": self.absent_tags,
            "dispositions": self.dispositions,
            "scan_layout": self.scan_layout,
            "phases": [self._phase_dict(p) for p in self.phases],
        }

    @staticmethod
    def _phase_dict(phase: "PipelinePhaseRecord") -> Dict[str, Any]:
        """Serialize one phase with its list fields sorted.

        Server enumeration order is not stable across runs, so the lists are
        sorted on write to keep the repeat-run comparison (SC-008) order-free.
        """
        data = asdict(phase)
        data["files_affected"] = sorted(data.get("files_affected", []))
        data["xnat_resource_links"] = sorted(data.get("xnat_resource_links", []))
        return data

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class OutcomeSummaryWriter:
    """
    Accumulates phase records and writes final outcome summary to JSON.

    Usage
    -----
    writer = OutcomeSummaryWriter()
    writer.add_phase_record(PipelinePhaseRecord(...))
    writer.add_phase_record(PipelinePhaseRecord(...))
    writer.write_outcome(
        case_name="KNEE_2025",
        files_processed=65,
        checksum_pass_count=65,
        ...
    )
    """

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Initialize the writer.

        Parameters
        ----------
        output_dir
            Directory where outcome JSONs will be written.
            Defaults to tests/integration/live_xnat/outcomes/.
        """
        if output_dir is None:
            output_dir = (
                Path(__file__).parent / "outcomes"
            )
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self._records: List[PipelinePhaseRecord] = []

    def add_phase_record(self, record: PipelinePhaseRecord) -> None:
        """Append a phase record."""
        self._records.append(record)

    def write_outcome(
        self,
        case_name: str,
        files_processed: int,
        files_recovered: int,
        files_rejected: int,
        checksum_pass_count: int,
        checksum_fail_count: int,
        uid_collisions_resolved: int,
        uid_collisions_recorded: int = 0,
        absent_tags: Optional[Dict[str, List[str]]] = None,
        dispositions: Optional[Dict[str, str]] = None,
        scan_layout: Optional[Dict[str, Any]] = None,
    ) -> Path:
        """
        Write accumulated phase records + outcome stats to JSON.

        Parameters
        ----------
        case_name
            One of "KNEE_2025", "HIP_2024", "RADIOFLUORO_2026".
        files_processed
            Total files ingested.
        files_recovered
            Files that recovered from error (e.g., truncated → repaired).
        files_rejected
            Files that failed irreparably.
        checksum_pass_count
            Files that passed checksum validation.
        checksum_fail_count
            Files that failed checksum validation.
        uid_collisions_resolved
            Number of UID collisions that were deduped.

        Returns
        -------
        Path
            Path to the written JSON file.
        """
        self._check_phase_records(case_name)
        run_timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

        summary = OutcomeSummary(
            case_name=case_name,
            run_timestamp=run_timestamp,
            files_processed=files_processed,
            files_recovered=files_recovered,
            files_rejected=files_rejected,
            checksum_pass_count=checksum_pass_count,
            checksum_fail_count=checksum_fail_count,
            uid_collisions_resolved=uid_collisions_resolved,
            uid_collisions_recorded=uid_collisions_recorded,
            absent_tags=dict(absent_tags or {}),
            dispositions=dict(dispositions or {}),
            scan_layout=dict(scan_layout or {}),
            phases=list(self._records),
        )

        # Filename: <case_name>-<timestamp>.json
        # Use filename-safe timestamp (replace colons with hyphens)
        safe_timestamp = run_timestamp.replace(":", "-")
        filename = f"{case_name}-{safe_timestamp}.json"
        output_path = self.output_dir / filename

        output_path.write_text(summary.to_json(), encoding="utf-8")
        return output_path

    def _check_phase_records(self, case_name: str) -> None:
        """FR-019: exactly one record per phase, all belonging to ``case_name``."""
        phases = [r.phase for r in self._records]
        if sorted(phases) != sorted(PHASES):
            raise ValueError(
                f"Outcome summary for {case_name} needs one record for each of "
                f"{PHASES}; got {phases}."
            )
        foreign = {r.case_name for r in self._records} - {case_name}
        if foreign:
            raise ValueError(f"Outcome summary for {case_name} holds records of {sorted(foreign)}.")

    def clear(self) -> None:
        """Clear accumulated records (for next case)."""
        self._records.clear()
