"""
Tests for src/services/preflight.py

Verifies:
- check_path_exists returns None for existing path, FriendlyError for missing
- check_server_reachable returns None on success, FriendlyError on connection error
- check_credentials returns None on success, FriendlyError on auth failure
- No exception escapes any check function
- FriendlyError objects carry correct recourse lists
- FakeXNAT used where a server stand-in helps

No real network required.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

from src.services.errors import FriendlyError
from src.services.preflight import (
    check_credentials,
    check_path_exists,
    check_server_reachable,
    check_ssl_certificate,
)
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raises(exc: BaseException):
    """Return a zero-argument callable that always raises *exc*."""
    def _inner():
        raise exc
    return _inner


def _succeeds():
    """Zero-argument callable that does nothing (simulates success)."""
    pass


# ---------------------------------------------------------------------------
# check_path_exists
# ---------------------------------------------------------------------------

class TestCheckPathExists:
    def test_existing_path_returns_none(self, tmp_path: Path) -> None:
        result = check_path_exists(tmp_path)
        assert result is None

    def test_existing_file_returns_none(self, tmp_path: Path) -> None:
        f = tmp_path / "data.txt"
        f.write_text("hello")
        result = check_path_exists(f)
        assert result is None

    def test_missing_path_returns_friendly_error(self, tmp_path: Path) -> None:
        missing = tmp_path / "does_not_exist"
        result = check_path_exists(missing)
        assert isinstance(result, FriendlyError)

    def test_missing_path_has_recourse(self, tmp_path: Path) -> None:
        result = check_path_exists(tmp_path / "ghost")
        assert result is not None
        assert len(result.recourse) >= 1

    def test_missing_path_message_mentions_path(self, tmp_path: Path) -> None:
        missing = tmp_path / "ghost_file.txt"
        result = check_path_exists(missing)
        assert result is not None
        assert str(missing) in result.message or "ghost_file" in result.message

    def test_no_exception_escapes_on_missing_path(self, tmp_path: Path) -> None:
        # Must not raise — must return a FriendlyError
        result = check_path_exists(tmp_path / "no_such_thing")
        assert isinstance(result, FriendlyError)

    def test_accepts_str_path(self, tmp_path: Path) -> None:
        result = check_path_exists(str(tmp_path / "nope"))
        assert isinstance(result, FriendlyError)

    def test_has_log_path(self, tmp_path: Path) -> None:
        result = check_path_exists(tmp_path / "nope")
        assert result is not None
        assert result.diagnostic_log_path is not None


# ---------------------------------------------------------------------------
# check_server_reachable
# ---------------------------------------------------------------------------

class TestCheckServerReachable:
    def test_success_callable_returns_none(self) -> None:
        result = check_server_reachable(_succeeds)
        assert result is None

    def test_fake_xnat_get_returns_none(self) -> None:
        fake = FakeXNAT()
        result = check_server_reachable(lambda: fake.get("/"))
        assert result is None

    def test_connection_error_returns_friendly_error(self) -> None:
        result = check_server_reachable(_raises(ConnectionError("VPN not connected")))
        assert isinstance(result, FriendlyError)

    def test_timeout_returns_friendly_error(self) -> None:
        result = check_server_reachable(_raises(TimeoutError("timeout")))
        assert isinstance(result, FriendlyError)

    def test_friendly_error_mentions_vpn(self) -> None:
        result = check_server_reachable(_raises(OSError("no route to host")))
        assert result is not None
        assert "VPN" in result.message or "vpn" in result.message.lower()

    def test_recourse_includes_retry(self) -> None:
        result = check_server_reachable(_raises(ConnectionError("down")))
        assert result is not None
        retry_hints = [r for r in result.recourse if "retry" in r.lower() or "vpn" in r.lower()]
        assert len(retry_hints) >= 1

    def test_no_exception_escapes(self) -> None:
        result = check_server_reachable(_raises(RuntimeError("surprise")))
        assert isinstance(result, FriendlyError)

    def test_fake_xnat_failure_injection(self) -> None:
        # FakeXNAT.set_next_failure triggers on file/resource ops, not get().
        # Use a raising lambda to simulate a server that rejects the liveness probe.
        exc = ConnectionError("injected failure from FakeXNAT simulation")
        result = check_server_reachable(_raises(exc))
        assert isinstance(result, FriendlyError)

    def test_has_log_path(self) -> None:
        result = check_server_reachable(_raises(ConnectionError("down")))
        assert result is not None
        assert result.diagnostic_log_path is not None


# ---------------------------------------------------------------------------
# check_credentials
# ---------------------------------------------------------------------------

class TestCheckCredentials:
    def test_success_callable_returns_none(self) -> None:
        result = check_credentials(_succeeds)
        assert result is None

    def test_auth_error_returns_friendly_error(self) -> None:
        result = check_credentials(_raises(PermissionError("401 Unauthorized")))
        assert isinstance(result, FriendlyError)

    def test_value_error_returns_friendly_error(self) -> None:
        result = check_credentials(_raises(ValueError("bad credentials")))
        assert isinstance(result, FriendlyError)

    def test_friendly_error_mentions_password(self) -> None:
        result = check_credentials(_raises(PermissionError("bad auth")))
        assert result is not None
        text = result.title + " " + result.message
        assert "password" in text.lower() or "username" in text.lower() or "login" in text.lower()

    def test_recourse_mentions_re_enter_credentials(self) -> None:
        result = check_credentials(_raises(PermissionError("bad auth")))
        assert result is not None
        credential_hints = [
            r for r in result.recourse
            if "password" in r.lower() or "username" in r.lower() or "re-enter" in r.lower()
        ]
        assert len(credential_hints) >= 1

    def test_no_exception_escapes(self) -> None:
        result = check_credentials(_raises(RuntimeError("surprise")))
        assert isinstance(result, FriendlyError)

    def test_has_log_path(self) -> None:
        result = check_credentials(_raises(PermissionError("denied")))
        assert result is not None
        assert result.diagnostic_log_path is not None


# ---------------------------------------------------------------------------
# check_ssl_certificate
# ---------------------------------------------------------------------------

class TestCheckSslCertificate:
    def test_success_callable_returns_none(self) -> None:
        result = check_ssl_certificate(_succeeds)
        assert result is None

    def test_ssl_error_returns_friendly_error(self) -> None:
        import ssl as _ssl
        exc = _ssl.SSLCertVerificationError("certificate has expired")
        result = check_ssl_certificate(_raises(exc))
        assert isinstance(result, FriendlyError)

    def test_friendly_error_names_data_librarian(self) -> None:
        import ssl as _ssl
        exc = _ssl.SSLCertVerificationError("certificate has expired")
        result = check_ssl_certificate(_raises(exc))
        assert result is not None
        combined = result.title + " " + result.message + " ".join(result.recourse)
        assert "Data Librarian" in combined

    def test_non_ssl_exception_returns_none(self) -> None:
        # Non-SSL exceptions are not our responsibility in this check
        result = check_ssl_certificate(_raises(ConnectionError("not ssl")))
        assert result is None

    def test_recourse_includes_contact_librarian(self) -> None:
        import ssl as _ssl
        exc = _ssl.SSLCertVerificationError("expired")
        result = check_ssl_certificate(_raises(exc))
        assert result is not None
        librarian_hints = [r for r in result.recourse if "Data Librarian" in r]
        assert len(librarian_hints) >= 1

    def test_no_exception_escapes(self) -> None:
        import ssl as _ssl
        exc = _ssl.SSLCertVerificationError("expired")
        result = check_ssl_certificate(_raises(exc))
        assert isinstance(result, FriendlyError)
