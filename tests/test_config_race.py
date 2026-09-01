"""
tests/test_config_race.py — Offline tests for T029: config lost-update race detection.

Tests the detect-and-refuse path in ConfigTables._check_for_lost_update().

Rules verified:
  1. No conflict  → _check_for_lost_update() passes silently; no ValueError raised.
  2. Conflict     → _check_for_lost_update() raises ValueError containing the
                    "changed since you loaded it" message.
  3. No put       → when a conflict is detected, file.put is NOT called.
  4. No baseline  → when _server_fingerprint_at_load is absent, the check is skipped.
  5. Fingerprint  → _fingerprint_file returns a stable 64-char hex sha256.

NO network, NO PHI, NO real XNAT server.  Uses FakeXNAT + unittest.mock only.
"""
from __future__ import annotations

import hashlib
import json
import sys
import types
from pathlib import Path
from typing import Any

import pytest

# ---------------------------------------------------------------------------
# Ensure repo root is importable
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers — build a minimal ConfigTables-like object without a live server
# ---------------------------------------------------------------------------

def _make_json_bytes(data: Any) -> bytes:
    """Return canonical JSON bytes for *data*."""
    return json.dumps(data, sort_keys=True).encode("utf-8")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _build_stub_config_tables(
    fake_server: FakeXNAT,
    initial_content: bytes,
    *,
    project_name: str = "FAKE_PROJECT",
    config_folder: str = "config",
    config_fn: str = "database_config.json",
    tmp_path: Path,
) -> Any:
    """
    Return a minimal stub object that has the same attributes used by
    ConfigTables._check_for_lost_update() and _fingerprint_file().

    We attach the real methods from ConfigTables directly so we're testing the
    production code path, not a re-implementation.
    """
    # Import the production methods we want to test.
    # We import ConfigTables class definition only to borrow the two methods.
    # We do NOT call ConfigTables.__init__ (that would need a live server).
    from src.utilities import ConfigTables as _CT

    # Lay down the initial "server" copy in the FakeXNAT's file-content store
    fake_server.set_file_content(config_fn, initial_content)

    # Create a local config file for the stub
    local_cfg = tmp_path / config_fn
    local_cfg.write_bytes(initial_content)

    # Build a namespace object (like a blank ConfigTables instance)
    stub = types.SimpleNamespace()
    stub.xnat_connection = types.SimpleNamespace()
    stub.xnat_connection.server = fake_server
    stub.xnat_connection.gateway = fake_server
    stub.xnat_connection.xnat_project_name = project_name
    stub.xnat_config_folder_name = config_folder
    stub.config_fn = config_fn
    stub.config_ffn = str(local_cfg)

    # Fingerprint captured at download time (same as initial_content)
    stub._server_fingerprint_at_load = _sha256(initial_content)

    # Bind the two production methods onto the stub
    stub._fingerprint_file = lambda ffn: _CT._fingerprint_file(stub, ffn)
    stub._check_for_lost_update = lambda: _CT._check_for_lost_update(stub)

    return stub


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def initial_bytes() -> bytes:
    return _make_json_bytes({"tables": {}, "metadata": {"version": 1}})


@pytest.fixture
def changed_bytes() -> bytes:
    """Different content — simulates a concurrent edit."""
    return _make_json_bytes({"tables": {}, "metadata": {"version": 2, "edited_by": "other_user"}})


@pytest.fixture
def fake(initial_bytes):
    return FakeXNAT(project_name="FAKE_PROJECT")


# ---------------------------------------------------------------------------
# T1 — No conflict: fingerprints match → no error raised
# ---------------------------------------------------------------------------

class TestNoConflict:
    def test_matching_fingerprint_passes_silently(self, fake, initial_bytes, tmp_path):
        """When server copy has NOT changed, _check_for_lost_update must not raise."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        # Server still has the same bytes → no conflict
        fake.set_file_content("database_config.json", initial_bytes)
        # Must not raise
        stub._check_for_lost_update()

    def test_no_put_recorded_on_clean_check(self, fake, initial_bytes, tmp_path):
        """After a successful (no-conflict) check, no file.put must have been made."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        fake.set_file_content("database_config.json", initial_bytes)
        stub._check_for_lost_update()
        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        assert len(put_calls) == 0, "No file.put should happen during a conflict check"


# ---------------------------------------------------------------------------
# T2 — Conflict: server copy changed → ValueError raised, no put
# ---------------------------------------------------------------------------

class TestConflictDetected:
    def test_raises_value_error_on_conflict(self, fake, initial_bytes, changed_bytes, tmp_path):
        """When server copy changed since download, must raise ValueError."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        # Simulate another user modifying the server copy between our download and upload
        fake.set_file_content("database_config.json", changed_bytes)
        with pytest.raises(ValueError):
            stub._check_for_lost_update()

    def test_error_message_mentions_catalog(self, fake, initial_bytes, changed_bytes, tmp_path):
        """Error message must tell the user what happened in plain language."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        fake.set_file_content("database_config.json", changed_bytes)
        with pytest.raises(ValueError, match="changed since you loaded it"):
            stub._check_for_lost_update()

    def test_no_put_on_conflict(self, fake, initial_bytes, changed_bytes, tmp_path):
        """The refused conflict must NOT result in a file.put to the server."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        fake.set_file_content("database_config.json", changed_bytes)
        try:
            stub._check_for_lost_update()
        except ValueError:
            pass
        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        assert len(put_calls) == 0, "Conflict detection must NOT write to the server"

    def test_get_copy_recorded_once_on_conflict(self, fake, initial_bytes, changed_bytes, tmp_path):
        """Exactly one get_copy (the re-fetch for fingerprint comparison) is made."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        fake.set_file_content("database_config.json", changed_bytes)
        try:
            stub._check_for_lost_update()
        except ValueError:
            pass
        get_calls = [c for c in fake.calls if c["op"] == "file.get_copy"]
        assert len(get_calls) == 1, "Exactly one re-fetch get_copy expected"


# ---------------------------------------------------------------------------
# T3 — No baseline: skip check when fingerprint never recorded
# ---------------------------------------------------------------------------

class TestNoBaseline:
    def test_no_baseline_skips_check(self, fake, initial_bytes, tmp_path):
        """If _server_fingerprint_at_load was never set, check must be a no-op."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        # Delete the baseline (first-run init path)
        del stub._server_fingerprint_at_load
        # Must not raise even though server has different bytes
        fake.set_file_content("database_config.json", b'{"completely": "different"}')
        stub._check_for_lost_update()  # must be silent

    def test_none_baseline_skips_check(self, fake, initial_bytes, tmp_path):
        """Explicitly setting baseline to None also skips the check."""
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        stub._server_fingerprint_at_load = None
        fake.set_file_content("database_config.json", b'{"other": "content"}')
        stub._check_for_lost_update()  # must be silent


# ---------------------------------------------------------------------------
# T4 — _fingerprint_file returns stable sha256
# ---------------------------------------------------------------------------

class TestFingerprintFile:
    def test_returns_64_char_hex(self, fake, initial_bytes, tmp_path):
        stub = _build_stub_config_tables(fake, initial_bytes, tmp_path=tmp_path)
        fp = stub._fingerprint_file(tmp_path / "database_config.json")
        assert isinstance(fp, str)
        assert len(fp) == 64
        assert all(c in "0123456789abcdef" for c in fp)

    def test_same_content_same_fingerprint(self, fake, initial_bytes, tmp_path):
        stub = _build_stub_config_tables(fake, initial_bytes, tmp_path=tmp_path)
        f1 = tmp_path / "a.json"
        f2 = tmp_path / "b.json"
        f1.write_bytes(initial_bytes)
        f2.write_bytes(initial_bytes)
        assert stub._fingerprint_file(f1) == stub._fingerprint_file(f2)

    def test_different_content_different_fingerprint(self, fake, initial_bytes, changed_bytes, tmp_path):
        stub = _build_stub_config_tables(fake, initial_bytes, tmp_path=tmp_path)
        f1 = tmp_path / "orig.json"
        f2 = tmp_path / "modified.json"
        f1.write_bytes(initial_bytes)
        f2.write_bytes(changed_bytes)
        assert stub._fingerprint_file(f1) != stub._fingerprint_file(f2)

    def test_fingerprint_matches_manual_sha256(self, fake, initial_bytes, tmp_path):
        stub = _build_stub_config_tables(fake, initial_bytes, tmp_path=tmp_path)
        f = tmp_path / "verify.json"
        f.write_bytes(initial_bytes)
        expected = hashlib.sha256(initial_bytes).hexdigest()
        assert stub._fingerprint_file(f) == expected


# ---------------------------------------------------------------------------
# T5 — Integration: simulate the full download→edit→concurrent-edit→upload sequence
# ---------------------------------------------------------------------------

class TestFullRaceScenario:
    def test_concurrent_edit_refused_end_to_end(self, fake, initial_bytes, changed_bytes, tmp_path):
        """
        Simulate: user A downloads, user B modifies server copy, user A tries to upload.
        Asserts: upload attempt raises ValueError and no file.put is recorded.
        """
        # User A downloads (fingerprint captured)
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )

        # User B modifies the server copy
        fake.set_file_content("database_config.json", changed_bytes)
        fake.reset_calls()  # clear download-phase calls

        # User A tries to upload → must be refused
        with pytest.raises(ValueError, match="changed since you loaded it"):
            stub._check_for_lost_update()

        # No put should have been issued
        put_calls = [c for c in fake.calls if c["op"] == "file.put"]
        assert len(put_calls) == 0

    def test_sequential_edits_pass(self, fake, initial_bytes, tmp_path):
        """
        Simulate: user A downloads, user A uploads (no concurrent change).
        Asserts: check passes, then fingerprint can be updated for next round.
        """
        stub = _build_stub_config_tables(
            fake, initial_bytes, tmp_path=tmp_path
        )
        fake.set_file_content("database_config.json", initial_bytes)

        # First upload check passes
        stub._check_for_lost_update()

        # Update baseline to reflect the push (simulating post-push state)
        stub._server_fingerprint_at_load = _sha256(initial_bytes)

        # Second download of same content → still passes
        stub._check_for_lost_update()
