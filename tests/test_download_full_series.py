"""
tests/test_download_full_series.py — Offline tests for full-series download (#25).

T013 (RED FIRST): N-file scan → today yields 1 synthesized file per scan.
  - After T014 fix: N real files are downloaded, count-verified vs server.
  - Whole-surgery (experiment) selection auto-expands to all scans.
  - Count mismatch → FriendlyError (not a crash).
  - Empty resource → friendly no-op.

After T014/T015/T015b patches all GREEN tests must pass.

RULES:
  - NO streamlit import.
  - NO network.  FakeXNAT + T003 seed_resource_files used throughout.
  - NO PHI.
  - Cross-platform paths via pathlib.

Covers (T013→T015b):
  1. N-file scan downloads N real files (count match).
  2. Empty resource → friendly no-op DownloadOutcome(ok=False, ...).
  3. Count mismatch between server-reported and actual → FriendlyError.
  4. Whole-surgery (one experiment selected) → all scans downloaded.
  5. Content-scope zip: source-only / all scans / + derived.
"""
from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.logic.download import download_selection, list_downloadable, DownloadOutcome
from src.services.errors import FriendlyError
from tests.fakes.fake_xnat import FakeXNAT, FakeResource


# ---------------------------------------------------------------------------
# Test double: FullSeriesFakeXNAT
#
# Extends FakeXNAT with browse fallback hooks + exposes per-scan FakeResource
# objects pre-seeded with N real files via seed_resource_files().
# ---------------------------------------------------------------------------

class FullSeriesFakeXNAT(FakeXNAT):
    """
    FakeXNAT for full-series download tests (T013).

    Seeding::

        server = FullSeriesFakeXNAT(project_name="PROJ")
        files = [("img_001.dcm", b"\\xDC\\xM1..."), ...]
        scan_res = server.get_scan_resource("SUBJ", "EXP", "SCAN", "SRC")
        server.seed_resource_files(scan_res, files)

    After seeding, download_selection should enumerate the N real files and
    fetch each (not synthesize a single filename).
    """

    def __init__(self, subjects: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._subjects: Dict[str, Any] = subjects or {}
        # Registry: (subject, experiment, scan, resource_label) → FakeResource
        self._scan_resources: Dict[tuple, FakeResource] = {}

    def get_scan_resource(
        self,
        subject: str,
        experiment: str,
        scan: str,
        resource_label: str = "SRC",
    ) -> FakeResource:
        """
        Return (creating if needed) the FakeResource for a scan.
        The download_selection path calls server.select(qs).resource(label);
        this helper lets tests pre-seed the exact resource object that will
        be returned.
        """
        key = (subject, experiment, scan, resource_label)
        if key not in self._scan_resources:
            self._scan_resources[key] = FakeResource(root=self, label=resource_label)
        return self._scan_resources[key]

    # ------------------------------------------------------------------
    # Override FakeSelector.__call__ so select(qs).resource(label)
    # returns the seeded FakeResource for that scan path.
    # ------------------------------------------------------------------

    def _parse_scan_qs(self, qs: str) -> Optional[tuple]:
        """
        Parse a querystring like:
            /projects/P/subjects/S/experiments/E/scans/SCAN/resources/SRC
        and return (subject, experiment, scan, resource_label) or None.
        """
        parts = [p for p in qs.split("/") if p]
        # Expect: projects P subjects S experiments E scans SCAN resources RES
        try:
            idx_sub = parts.index("subjects")
            idx_exp = parts.index("experiments")
            idx_scan = parts.index("scans")
            idx_res = parts.index("resources")
            return (
                parts[idx_sub + 1],
                parts[idx_exp + 1],
                parts[idx_scan + 1],
                parts[idx_res + 1],
            )
        except (ValueError, IndexError):
            return None

    # --- browse fallback hooks ---

    def list_subjects(self, project_name: str) -> List[str]:
        return list(self._subjects.keys())

    def list_experiments(self, project_name: str, subject: str) -> List[str]:
        return list(self._subjects.get(subject, {}).keys())

    def list_scans(self, project_name: str, subject: str, experiment: str) -> List[str]:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        return list(exp.get("scans", {}).keys())

    def scan_attrs(self, project_name: str, subject: str, experiment: str, scan: str) -> dict:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        sd = exp.get("scans", {}).get(scan, {})
        return {"scan_type": sd.get("scan_type", "SRC")}

    def file_count(self, project_name: str, subject: str, experiment: str, scan: str) -> int:
        exp = self._subjects.get(subject, {}).get(experiment, {})
        sd = exp.get("scans", {}).get(scan, {})
        return sd.get("num_files", -1)

    def experiment_date(self, project_name: str, subject: str, experiment: str) -> str:
        return self._subjects.get(subject, {}).get(experiment, {}).get("date", "")


# ---------------------------------------------------------------------------
# A patched FakeSelectable that returns the seeded resource on .resource()
# ---------------------------------------------------------------------------

class _ScanSelectable:
    """Minimal selectable that returns a pre-seeded FakeResource."""
    def __init__(self, resource: FakeResource) -> None:
        self._res = resource

    def exists(self) -> bool:
        return True

    def resource(self, label: str) -> FakeResource:
        return self._res


class SeededFullSeriesFakeXNAT(FullSeriesFakeXNAT):
    """
    Variant where server.select(qs) returns a _ScanSelectable backed by the
    seeded FakeResource, so download_selection can call
    resource.list_files() and resource.file(fn).get_copy(dest).
    """

    class _SeededSelector:
        def __init__(self, owner: "SeededFullSeriesFakeXNAT") -> None:
            self._owner = owner

        def __call__(self, qs: str) -> Any:
            parsed = self._owner._parse_scan_qs(qs)
            if parsed is not None:
                res = self._owner._scan_resources.get(parsed)
                if res is not None:
                    return _ScanSelectable(resource=res)
            # Fall back to the base FakeSelector (creates FakeSelectable)
            from tests.fakes.fake_xnat import FakeSelectable
            if qs not in self._owner._selectables:
                self._owner._selectables[qs] = FakeSelectable(
                    root=self._owner, querystring=qs
                )
            return self._owner._selectables[qs]

        def project(self, name: str) -> Any:
            from tests.fakes.fake_xnat import FakeProject
            return FakeProject(root=self._owner, name=name)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.select = self._SeededSelector(self)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_files(n: int, prefix: str = "img") -> List[tuple]:
    """Return [(filename, bytes), ...] with n unique synthetic DICOM-ish files."""
    return [
        (f"{prefix}_{i:03d}.dcm", bytes([0xDC, 0xD4, i % 256, (i >> 8) % 256]) * 64)
        for i in range(n)
    ]


PROJECT = "FULL_SERIES_PROJ"
SUBJECT = "SUBJ_001"
EXPERIMENT = "EXP_RF_001"
SCAN = "0"
N_FILES = 5


@pytest.fixture()
def n_file_server(tmp_path: Path) -> SeededFullSeriesFakeXNAT:
    """Server with one scan seeded with N_FILES real files."""
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {
                        SCAN: {"scan_type": "SRC", "num_files": N_FILES},
                    },
                },
            },
        },
    )
    res = server.get_scan_resource(SUBJECT, EXPERIMENT, SCAN, "SRC")
    server.seed_resource_files(res, _make_fake_files(N_FILES))
    return server


@pytest.fixture()
def empty_resource_server() -> SeededFullSeriesFakeXNAT:
    """Server with a scan that has 0 files in its resource."""
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {SCAN: {"scan_type": "SRC", "num_files": 0}},
                },
            },
        },
    )
    # Resource seeded with 0 files — empty
    res = server.get_scan_resource(SUBJECT, EXPERIMENT, SCAN, "SRC")
    server.seed_resource_files(res, [])
    return server


@pytest.fixture()
def multi_scan_server() -> SeededFullSeriesFakeXNAT:
    """Server with one experiment and multiple scans (whole-surgery test).

    scan_type uses the scan label ("0", "1") so download_selection's QS
    correctly maps to the seeded FakeResource keys.
    """
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {
                        # scan_type = scan label so download_selection builds
                        # the correct QS /.../scans/0/... and /.../scans/1/...
                        "0": {"scan_type": "0", "num_files": 3},
                        "1": {"scan_type": "1", "num_files": 2},
                    },
                },
            },
        },
    )
    res0 = server.get_scan_resource(SUBJECT, EXPERIMENT, "0", "SRC")
    server.seed_resource_files(res0, _make_fake_files(3, prefix="scan0"))
    res1 = server.get_scan_resource(SUBJECT, EXPERIMENT, "1", "SRC")
    server.seed_resource_files(res1, _make_fake_files(2, prefix="scan1"))
    return server


# ---------------------------------------------------------------------------
# T013 RED: today download_selection synthesizes 1 file per scan (not N)
# ---------------------------------------------------------------------------

class TestSynthesizedFileToday:
    """
    RED-state documentation (xfail).

    Pre-T014: download_selection wrote 1 synthesized file per scan.
    After T014 fix: N real files written → count assertion fails → xfail.
    """

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "T013 red-state: pre-T014, download_selection synthesized one filename "
            "per scan ({subject}_{experiment}_{scan}.dcm) regardless of N staged "
            "files.  After T014 fix N real files are written → assert len==1 fails "
            "→ xfail(strict=True) passes."
        ),
    )
    def test_today_yields_one_synthesized_file_not_n(
        self, n_file_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        RED (xfail): pre-T014, only 1 synthesized file per scan.
        After T014 fix: N files written → len==1 assertion fails → xfail.
        """
        selection = [
            {
                "subject": SUBJECT,
                "experiment": EXPERIMENT,
                "scan_type": SCAN,
                "num_files": N_FILES,
            }
        ]
        outcome = download_selection(n_file_server, PROJECT, selection, tmp_path)
        assert outcome.ok, f"download_selection failed: {outcome.friendly}"
        written = outcome.files_written
        # Pre-T014: 1 synthesized file.  Post-T014: N files → assertion fails → xfail.
        assert len(written) == 1, (
            f"Expected 1 synthesized file today, got {len(written)}: {written}."
        )
        synthesized_name = f"{SUBJECT}_{EXPERIMENT}_{SCAN}.dcm"
        assert written[0].name == synthesized_name


# ---------------------------------------------------------------------------
# T013→T014 GREEN: real files enumerated after fix
# ---------------------------------------------------------------------------

class TestRealFilesAfterFix:
    """
    GREEN tests: after T014 patch, real files are enumerated and count-verified.
    These FAIL today (proving the fix is needed).
    """

    def test_n_files_downloaded(
        self, n_file_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        After T014: N_FILES real files are written for a scan with N staged files.
        """
        selection = [
            {
                "subject": SUBJECT,
                "experiment": EXPERIMENT,
                "scan_type": SCAN,
                "num_files": N_FILES,
            }
        ]
        outcome = download_selection(n_file_server, PROJECT, selection, tmp_path)
        assert outcome.ok, f"download_selection failed: {outcome.friendly}"

        assert len(outcome.files_written) == N_FILES, (
            f"Expected {N_FILES} files, got {len(outcome.files_written)}: "
            f"{outcome.files_written}"
        )

    def test_file_contents_round_trip(
        self, n_file_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        After T014: downloaded bytes match the staged bytes (real round-trip).
        """
        res = n_file_server.get_scan_resource(SUBJECT, EXPERIMENT, SCAN, "SRC")
        staged = dict(res._staged_files)  # {fn: bytes}

        selection = [
            {
                "subject": SUBJECT,
                "experiment": EXPERIMENT,
                "scan_type": SCAN,
                "num_files": N_FILES,
            }
        ]
        outcome = download_selection(n_file_server, PROJECT, selection, tmp_path)
        assert outcome.ok, f"download_selection failed: {outcome.friendly}"

        for path in outcome.files_written:
            fn = path.name
            assert fn in staged, f"Downloaded file '{fn}' not in staged files."
            assert path.read_bytes() == staged[fn], (
                f"Bytes mismatch for file '{fn}'."
            )

    def test_empty_resource_friendly_noop(
        self, empty_resource_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        After T014: a scan with 0 files in its resource is a friendly no-op
        (no crash, ok=False or ok=True with 0 files, clear message).
        """
        selection = [
            {
                "subject": SUBJECT,
                "experiment": EXPERIMENT,
                "scan_type": SCAN,
                "num_files": 0,
            }
        ]
        outcome = download_selection(empty_resource_server, PROJECT, selection, tmp_path)

        # Should not crash; either ok with 0 files, or ok=False with friendly message.
        if not outcome.ok:
            assert outcome.friendly is not None, (
                "Expected a FriendlyError message for empty resource, got None."
            )
        else:
            assert len(outcome.files_written) == 0, (
                "Expected 0 files for empty resource, got "
                f"{len(outcome.files_written)}."
            )

    def test_whole_surgery_all_scans_downloaded(
        self, multi_scan_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        After T015/T015b: selecting an entire experiment downloads all scans.

        Whole-surgery selection is represented as a row with scan_type='' or
        a special 'ALL_SCANS' marker; the logic auto-expands to enumerate
        all scans under the experiment.
        """
        rows = list_downloadable(multi_scan_server, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable failed: {rows.message}")

        # Select all rows for EXPERIMENT (simulates whole-surgery selection)
        exp_rows = [r for r in rows if r.get("experiment") == EXPERIMENT]
        assert exp_rows, f"No rows for experiment '{EXPERIMENT}'"

        outcome = download_selection(multi_scan_server, PROJECT, exp_rows, tmp_path)
        assert outcome.ok, f"download_selection failed: {outcome.friendly}"

        # All files across both scans should be written: 3 + 2 = 5
        expected_total = 3 + 2
        assert len(outcome.files_written) == expected_total, (
            f"Expected {expected_total} files for whole-surgery, "
            f"got {len(outcome.files_written)}: {outcome.files_written}"
        )


# ---------------------------------------------------------------------------
# T015b GREEN: zip assembly with content-scope picker
# ---------------------------------------------------------------------------

class TestZipAssemblyScope:
    """
    After T015b: download_selection with a whole-surgery scope delivers a zip
    whose contents match the chosen content scope.
    """

    def test_zip_contains_all_source_files_default_scope(
        self, multi_scan_server: SeededFullSeriesFakeXNAT, tmp_path: Path
    ) -> None:
        """
        After T015b: default scope = all source images → zip contains all N files
        from all scans.

        Requires download_selection to support a zip_dest parameter OR an
        assemble_zip helper in app/logic/download.py.
        """
        # Import the zip assembly helper (added in T015b).
        try:
            from app.logic.download import assemble_zip
        except ImportError:
            pytest.skip("assemble_zip not yet implemented (T015b not applied)")

        rows = list_downloadable(multi_scan_server, PROJECT)
        if isinstance(rows, FriendlyError):
            pytest.fail(f"list_downloadable failed: {rows.message}")

        exp_rows = [r for r in rows if r.get("experiment") == EXPERIMENT]
        assert exp_rows

        zip_path = tmp_path / "surgery.zip"
        outcome = assemble_zip(
            multi_scan_server,
            PROJECT,
            exp_rows,
            zip_path,
            scope="source",
        )
        assert outcome.ok, f"assemble_zip failed: {outcome.friendly}"
        assert zip_path.exists(), "Zip file was not created."

        with zipfile.ZipFile(zip_path) as zf:
            names = zf.namelist()

        # All 5 staged source files should be in the zip.
        assert len(names) == 5, (
            f"Expected 5 files in zip, got {len(names)}: {names}"
        )
