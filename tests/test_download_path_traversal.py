"""
tests/test_download_path_traversal.py — C2 (#33) zip-slip / path-traversal guard.

RULES:
  - NO `import streamlit`.
  - NO network; FakeXNAT seeded with a hostile server-supplied filename.
  - NO PHI.

Covers the #33 CRITICAL-C2 finding: download.py joined server-supplied
filenames (resource.list_files()) under the destination without sanitizing
``..``, so a filename like ``../../x`` would write outside the destination
(path-write traversal) and later crash ``relative_to(tmp_path)`` during zip
assembly.

Fix: ``app.logic.download._safe_resource_join`` confines every server filename
to its base dir, raising ValueError on traversal so the caller surfaces a
FriendlyError instead of writing through it.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pytest

from app.logic.download import _safe_resource_join, download_selection
from src.services.errors import FriendlyError

# Reuse the seeded full-series harness (browse hooks + per-scan FakeResource).
from tests.test_download_full_series import (
    SeededFullSeriesFakeXNAT,
    PROJECT,
    SUBJECT,
    EXPERIMENT,
    SCAN,
)


# ---------------------------------------------------------------------------
# Unit tests: _safe_resource_join
# ---------------------------------------------------------------------------

class TestSafeResourceJoin:
    def test_plain_filename_ok(self, tmp_path: Path) -> None:
        out = _safe_resource_join(tmp_path, "image_001.dcm")
        assert out == tmp_path / "image_001.dcm"
        assert out.resolve().is_relative_to(tmp_path.resolve())

    def test_nested_subdir_ok(self, tmp_path: Path) -> None:
        out = _safe_resource_join(tmp_path, "sub/dir/image_001.dcm")
        assert out.resolve().is_relative_to(tmp_path.resolve())

    def test_parent_traversal_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="path-traversal"):
            _safe_resource_join(tmp_path, "../../evil.dcm")

    def test_deep_traversal_rejected(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="path-traversal"):
            _safe_resource_join(tmp_path, "../../../../etc/passwd")

    def test_absolute_path_confined_not_escaped(self, tmp_path: Path) -> None:
        # Leading slash is stripped → joined relative to base, never absolute.
        out = _safe_resource_join(tmp_path, "/etc/passwd")
        assert out.resolve().is_relative_to(tmp_path.resolve())


# ---------------------------------------------------------------------------
# Integration: download_selection refuses a hostile server filename
# ---------------------------------------------------------------------------

@pytest.fixture()
def traversal_server() -> SeededFullSeriesFakeXNAT:
    """Scan resource whose list_files() returns a path-traversal filename."""
    server = SeededFullSeriesFakeXNAT(
        project_name=PROJECT,
        subjects={
            SUBJECT: {
                EXPERIMENT: {
                    "date": "2026-01-01",
                    "scans": {SCAN: {"scan_type": "SRC", "num_files": 1}},
                },
            },
        },
    )
    res = server.get_scan_resource(SUBJECT, EXPERIMENT, SCAN, "SRC")
    # Single hostile filename — escapes the per-subject dest dir.
    server.seed_resource_files(res, [("../../../../evil.dcm", b"\xDC\xD4\x00\x00" * 16)])
    return server


def _selection() -> List[dict]:
    return [{"subject": SUBJECT, "experiment": EXPERIMENT, "scan_type": SCAN, "num_files": 1}]


def test_download_selection_blocks_traversal(traversal_server, tmp_path: Path) -> None:
    outcome = download_selection(traversal_server, PROJECT, _selection(), tmp_path)
    assert outcome.ok is False, "Traversal filename must block the download"
    assert isinstance(outcome.friendly, FriendlyError)
    assert "Unsafe filename" in outcome.friendly.title


def test_download_selection_traversal_writes_nothing_outside_dest(
    traversal_server, tmp_path: Path
) -> None:
    dest = tmp_path / "dl"
    dest.mkdir()
    outcome = download_selection(traversal_server, PROJECT, _selection(), dest)
    assert outcome.ok is False
    # No file landed at the traversal target (tmp_path/evil.dcm or above dest).
    assert not (tmp_path / "evil.dcm").exists()
    for p in outcome.files_written:
        assert p.resolve().is_relative_to(dest.resolve())
