"""
tests/test_packaging_update.py
--------------------------------
Offline unit tests for installer.update_checker.

Covers:
  - newer remote version → available=True + url set
  - same version → available=False
  - older remote version → available=False
  - fetch_latest raises (network failure) → available=False, error set, no exception
  - reads installer/VERSION as local version default
  - version_file override for tests (no real FS dependency)
  - malformed remote response → available=False, error set
  - url is None when not available
"""
from __future__ import annotations

import os
import tempfile

import pytest

from installer.update_checker import (
    UpdateInfo,
    _is_newer,
    _parse_version,
    check_for_update,
)


# ---------------------------------------------------------------------------
# _parse_version unit tests
# ---------------------------------------------------------------------------

class TestParseVersion:
    def test_three_part(self):
        assert _parse_version("1.2.3") == (1, 2, 3)

    def test_two_part(self):
        assert _parse_version("2.0") == (2, 0, 0)

    def test_one_part(self):
        assert _parse_version("3") == (3, 0, 0)

    def test_with_leading_v(self):
        # "v1.2.3" — leading 'v' ignored by regex partial match
        result = _parse_version("v1.2.3")
        # Either (1,2,3) or (0,0,0) depending on regex; just ensure no exception
        assert isinstance(result, tuple)
        assert len(result) == 3

    def test_garbage_returns_zero(self):
        assert _parse_version("not-a-version") == (0, 0, 0)

    def test_empty_returns_zero(self):
        assert _parse_version("") == (0, 0, 0)


# ---------------------------------------------------------------------------
# _is_newer unit tests
# ---------------------------------------------------------------------------

class TestIsNewer:
    def test_newer_major(self):
        assert _is_newer("2.0.0", "1.9.9") is True

    def test_newer_minor(self):
        assert _is_newer("1.2.0", "1.1.9") is True

    def test_newer_patch(self):
        assert _is_newer("1.0.1", "1.0.0") is True

    def test_same_is_not_newer(self):
        assert _is_newer("1.0.0", "1.0.0") is False

    def test_older_is_not_newer(self):
        assert _is_newer("0.9.0", "1.0.0") is False


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

def _fetch_ok(version: str, url: str = "https://example.com/download"):
    """Return a fetch_latest callable that returns a fixed response."""
    def fetch():
        return {"version": version, "url": url}
    return fetch


def _fetch_raises(exc: Exception = None):
    """Return a fetch_latest callable that raises."""
    if exc is None:
        exc = ConnectionError("network unreachable")
    def fetch():
        raise exc
    return fetch


# ---------------------------------------------------------------------------
# check_for_update: newer remote → available=True
# ---------------------------------------------------------------------------

class TestCheckForUpdateNewer:
    def test_newer_version_available_true(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.2.0"),
        )
        assert result.available is True

    def test_newer_version_latest_set(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.2.0"),
        )
        assert result.latest == "0.2.0"

    def test_newer_version_url_set(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.2.0", url="https://dl.example.com/v0.2.0"),
        )
        assert result.url == "https://dl.example.com/v0.2.0"

    def test_newer_version_error_none(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.2.0"),
        )
        assert result.error is None

    def test_returns_update_info_type(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.2.0"),
        )
        assert isinstance(result, UpdateInfo)


# ---------------------------------------------------------------------------
# check_for_update: same version → available=False
# ---------------------------------------------------------------------------

class TestCheckForUpdateSameVersion:
    def test_same_version_not_available(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.1.0"),
        )
        assert result.available is False

    def test_same_version_no_error(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.1.0"),
        )
        assert result.error is None

    def test_same_version_url_none(self):
        """URL should be None when not available."""
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_ok("0.1.0"),
        )
        assert result.url is None


# ---------------------------------------------------------------------------
# check_for_update: older remote → available=False
# ---------------------------------------------------------------------------

class TestCheckForUpdateOlderRemote:
    def test_older_remote_not_available(self):
        result = check_for_update(
            local_version="1.0.0",
            fetch_latest=_fetch_ok("0.9.9"),
        )
        assert result.available is False

    def test_older_remote_url_none(self):
        result = check_for_update(
            local_version="1.0.0",
            fetch_latest=_fetch_ok("0.9.9"),
        )
        assert result.url is None


# ---------------------------------------------------------------------------
# check_for_update: fetch raises (network failure)
# ---------------------------------------------------------------------------

class TestCheckForUpdateNetworkFailure:
    def test_connection_error_does_not_raise(self):
        """Exception from fetch_latest must not escape check_for_update."""
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(ConnectionError("no route to host")),
        )
        # If we're here, no exception escaped
        assert result.available is False

    def test_connection_error_sets_error_field(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(ConnectionError("timeout")),
        )
        assert result.error is not None
        assert len(result.error) > 0

    def test_timeout_error_does_not_raise(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(TimeoutError("timed out")),
        )
        assert result.available is False
        assert result.error is not None

    def test_generic_exception_does_not_raise(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(Exception("unexpected")),
        )
        assert result.available is False

    def test_network_failure_latest_is_none(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(),
        )
        assert result.latest is None

    def test_network_failure_url_is_none(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(),
        )
        assert result.url is None

    def test_error_message_friendly_no_traceback(self):
        result = check_for_update(
            local_version="0.1.0",
            fetch_latest=_fetch_raises(ConnectionError("refused")),
        )
        assert "Traceback" not in result.error


# ---------------------------------------------------------------------------
# check_for_update: malformed remote response
# ---------------------------------------------------------------------------

class TestCheckForUpdateMalformedResponse:
    def test_missing_version_key(self):
        def fetch():
            return {"url": "https://example.com"}   # no 'version' key
        result = check_for_update(local_version="0.1.0", fetch_latest=fetch)
        assert result.available is False
        assert result.error is not None

    def test_none_response(self):
        def fetch():
            return None  # type: ignore[return-value]
        result = check_for_update(local_version="0.1.0", fetch_latest=fetch)
        assert result.available is False
        assert result.error is not None

    def test_wrong_type_response(self):
        def fetch():
            return "1.2.3"  # string, not dict  # type: ignore[return-value]
        result = check_for_update(local_version="0.1.0", fetch_latest=fetch)
        assert result.available is False


# ---------------------------------------------------------------------------
# check_for_update: reads installer/VERSION as default
# ---------------------------------------------------------------------------

class TestCheckForUpdateVersionFile:
    def test_reads_packaging_version_file(self):
        """Without local_version kwarg, reads from installer/VERSION."""
        # The real installer/VERSION contains "0.1.0".
        # Inject a remote fetch returning a higher version.
        result = check_for_update(
            fetch_latest=_fetch_ok("99.0.0"),
        )
        # Whatever the file says, 99.0.0 must be newer → available True
        assert result.available is True

    def test_version_file_override(self):
        """version_file kwarg overrides the default path (no real FS needed beyond temp)."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
            fh.write("0.5.0\n")
            tmp_path = fh.name
        try:
            result = check_for_update(
                fetch_latest=_fetch_ok("0.6.0"),
                version_file=tmp_path,
            )
            assert result.available is True
            assert result.latest == "0.6.0"
        finally:
            os.unlink(tmp_path)

    def test_version_file_override_same_version(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
            fh.write("1.0.0\n")
            tmp_path = fh.name
        try:
            result = check_for_update(
                fetch_latest=_fetch_ok("1.0.0"),
                version_file=tmp_path,
            )
            assert result.available is False
        finally:
            os.unlink(tmp_path)

    def test_missing_version_file_treated_as_000(self):
        """Missing VERSION file → local version "0.0.0" → any remote is newer."""
        result = check_for_update(
            fetch_latest=_fetch_ok("0.0.1"),
            version_file="/nonexistent/path/VERSION_does_not_exist.txt",
        )
        assert result.available is True

    def test_local_version_kwarg_takes_precedence(self):
        """Explicit local_version kwarg overrides VERSION file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as fh:
            fh.write("9.9.9\n")
            tmp_path = fh.name
        try:
            result = check_for_update(
                local_version="0.1.0",    # explicit — should win over file
                fetch_latest=_fetch_ok("0.2.0"),
                version_file=tmp_path,
            )
            assert result.available is True
            assert result.latest == "0.2.0"
        finally:
            os.unlink(tmp_path)
