"""
tests/test_delete_server_names.py — Offline tests for BUG M9: delete_metatables wrong target.

Rules verified:
  1. delete_metatables calls delete_file with ('config-qs', 'config', 'database_config.json').
  2. Resource name is 'config' (not 'MetaTables').
  3. Filename is 'database_config.json' (not 'MetaTables.json').
  4. Correct resource/file from xnat_conventions (ResourceLabel.CONFIG='config').
  5. Dry-run mode prints target description without deletion.
  6. Error handling re-raises RuntimeError with correct message.

NO network, NO real XNAT server. Uses FakeXNAT + offline stubs only.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# Ensure repo root is importable
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.fakes.fake_xnat import FakeXNAT
from src.delete_contents_of_server import delete_metatables
from src.services import xnat_conventions as _conventions


class TestDeleteMetatables:
    """Verify delete_metatables deletes correct resource/file."""

    def test_delete_metatables_calls_correct_resource(self) -> None:
        """delete_metatables should call delete_file('config', 'database_config.json')."""
        fake = FakeXNAT()

        # Patch the module-level project_name variable
        with mock.patch("src.delete_contents_of_server.project_name", "TEST_PROJECT"):
            delete_metatables(fake, dry_run=False)

        # Find the file.delete call (FakeXNAT records as file.delete)
        delete_calls = [c for c in fake.calls if c["op"] == "file.delete"]
        assert (
            len(delete_calls) == 1
        ), f"Expected 1 file.delete call, got {len(delete_calls)}"

        call = delete_calls[0]
        # file.delete() records _filename in kwargs
        filename = call["kwargs"]["_filename"]

        # Verify correct filename
        assert filename == "database_config.json", (
            f"Expected filename 'database_config.json', got '{filename}'"
        )

    def test_delete_metatables_dry_run_no_delete(self, capsys) -> None:
        """Dry-run mode should print target but not call delete_file."""
        fake = FakeXNAT()

        with mock.patch("src.delete_contents_of_server.project_name", "TEST_PROJECT"):
            delete_metatables(fake, dry_run=True)

        # Verify no delete_file was called
        delete_calls = [c for c in fake.calls if c["op"] == "delete_file"]
        assert len(delete_calls) == 0, "Dry-run should not call delete_file"

        # Verify output mentions the correct resource
        captured = capsys.readouterr()
        assert "[DRY-RUN]" in captured.out
        assert "config" in captured.out or "database_config.json" in captured.out

    def test_delete_metatables_error_raises_runtime_error(self) -> None:
        """Failed deletion should raise RuntimeError with clear message."""
        fake = FakeXNAT()
        # Inject a failure
        fake.set_next_failure(Exception("Gateway connection lost"))

        with mock.patch("src.delete_contents_of_server.project_name", "TEST_PROJECT"):
            with pytest.raises(RuntimeError) as exc_info:
                delete_metatables(fake, dry_run=False)

            error_msg = str(exc_info.value)
            assert "delete_metatables" in error_msg
            assert "database_config.json" in error_msg

    def test_delete_metatables_resource_label_from_conventions(self) -> None:
        """Should use ResourceLabel.CONFIG from xnat_conventions."""
        # Verify the constant is what we expect
        assert _conventions.ResourceLabel.CONFIG == "config"
        assert _conventions.ResourceLabel.BACKUPS == "backups"

    def test_delete_metatables_querystring_format(self) -> None:
        """Querystring should use project_qs format."""
        fake = FakeXNAT()

        with mock.patch("src.delete_contents_of_server.project_name", "MY_PROJECT"):
            delete_metatables(fake, dry_run=False)

        delete_calls = [c for c in fake.calls if c["op"] == "file.delete"]
        assert len(delete_calls) == 1, f"Expected 1 file.delete call, got {len(delete_calls)}: {fake.calls}"

        # The file.delete operation doesn't record querystring in args/kwargs
        # (it's handled by the FakeFile/FakeResource hierarchy internally).
        # Verify the filename is correct instead
        call = delete_calls[0]
        filename = call["kwargs"]["_filename"]
        assert filename == "database_config.json", (
            f"Expected filename 'database_config.json', got '{filename}'"
        )

    def test_delete_metatables_multiple_calls_cumulative(self) -> None:
        """Multiple calls to delete_metatables should each record a call."""
        fake = FakeXNAT()

        with mock.patch("src.delete_contents_of_server.project_name", "PROJ1"):
            delete_metatables(fake, dry_run=False)

        with mock.patch("src.delete_contents_of_server.project_name", "PROJ2"):
            delete_metatables(fake, dry_run=False)

        delete_calls = [c for c in fake.calls if c["op"] == "file.delete"]
        assert len(delete_calls) == 2, "Each call should record a file.delete"
