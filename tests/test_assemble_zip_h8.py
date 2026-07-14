"""
tests/test_assemble_zip_h8.py — Test hardening for assemble_zip (#33 H8).

H8: never emit empty zip + handle packing failures gracefully.

RULES:
  - NO streamlit import.
  - NO network.  SeededFullSeriesFakeXNAT + _make_fake_files used throughout.
  - NO PHI.
  - Cross-platform paths via pathlib.

Tests:
  1. test_happy_path_zip_created — server with 2 files, zip is created.
  2. test_empty_selection_no_zip — selection yields no files, no zip emitted.
  3. test_pack_failure_cleans_up_and_no_raise — zip_dest is a directory, returns
     FriendlyError instead of raising.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.logic.download import assemble_zip
from tests.test_download_full_series import SeededFullSeriesFakeXNAT, _make_fake_files


def test_happy_path_zip_created(tmp_path: Path) -> None:
    """Server with 2 files → zip is created, outcome.ok=True."""
    server = SeededFullSeriesFakeXNAT(
        project_name="H8_PROJ",
        subjects={
            "SUBJ_H8": {
                "EXP_H8": {
                    "date": "2026-01-01",
                    "scans": {"0": {"scan_type": "0", "num_files": 2}},
                }
            }
        },
    )
    res = server.get_scan_resource("SUBJ_H8", "EXP_H8", "0", "SRC")
    server.seed_resource_files(res, _make_fake_files(2, prefix="src"))
    selection = [
        {
            "subject": "SUBJ_H8",
            "experiment": "EXP_H8",
            "scan_type": "0",
            "num_files": 2,
        }
    ]

    zip_path = tmp_path / "out.zip"
    outcome = assemble_zip(server, "H8_PROJ", selection, zip_path, scope="source")

    assert outcome.ok is True
    assert zip_path.exists()


def test_empty_selection_no_zip(tmp_path: Path) -> None:
    """Selection yields no files → no zip emitted, outcome.ok=False, friendly set."""
    server = SeededFullSeriesFakeXNAT(
        project_name="H8_PROJ",
        subjects={
            "SUBJ_H8": {
                "EXP_H8": {
                    "date": "2026-01-01",
                    "scans": {"0": {"scan_type": "0", "num_files": 0}},
                }
            }
        },
    )
    res = server.get_scan_resource("SUBJ_H8", "EXP_H8", "0", "SRC")
    server.seed_resource_files(res, _make_fake_files(0))
    selection = [
        {
            "subject": "SUBJ_H8",
            "experiment": "EXP_H8",
            "scan_type": "0",
            "num_files": 0,
        }
    ]

    zip_path = tmp_path / "empty.zip"
    outcome = assemble_zip(server, "H8_PROJ", selection, zip_path, scope="source")

    assert outcome.ok is False
    assert outcome.friendly is not None
    assert not zip_path.exists()


def test_pack_failure_cleans_up_and_no_raise(tmp_path: Path) -> None:
    """zip_dest is a directory → ZipFile fails, cleaned up, FriendlyError returned."""
    server = SeededFullSeriesFakeXNAT(
        project_name="H8_PROJ",
        subjects={
            "SUBJ_H8": {
                "EXP_H8": {
                    "date": "2026-01-01",
                    "scans": {"0": {"scan_type": "0", "num_files": 2}},
                }
            }
        },
    )
    res = server.get_scan_resource("SUBJ_H8", "EXP_H8", "0", "SRC")
    server.seed_resource_files(res, _make_fake_files(2, prefix="src"))
    selection = [
        {
            "subject": "SUBJ_H8",
            "experiment": "EXP_H8",
            "scan_type": "0",
            "num_files": 2,
        }
    ]

    # Pass a directory as zip_dest; ZipFile("w") will fail.
    zip_dir = tmp_path / "adir"
    zip_dir.mkdir()
    outcome = assemble_zip(server, "H8_PROJ", selection, zip_dir, scope="source")

    # Should not raise, should return a FriendlyError.
    assert outcome.ok is False
    assert outcome.friendly is not None
    # Directory is untouched.
    assert zip_dir.is_dir()
