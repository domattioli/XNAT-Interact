"""
tests/test_app_auth_logic.py — Offline tests for app/logic/auth.attempt_login.

CRITICAL RULES enforced here:
  - NO `import streamlit` anywhere in this file.
  - NO network calls; FakeXNAT replaces the real server.
  - NO credentials in assertions or log captures.
  - Password must not appear in any returned object.

Covers:
  1. Successful login (FakeXNAT connects, .get('/') passes).
  2. VPN down — connect_factory raises ConnectionError.
  3. Bad credentials — creds check raises.
  4. Expired SSL certificate — SSLCertVerificationError.
  5. Password never in returned LoginResult.
"""
from __future__ import annotations

import ssl
from typing import Any

import pytest

# Only import from app/logic (no streamlit) and existing test infrastructure.
from app.logic.auth import attempt_login, LoginResult
from src.services.errors import FriendlyError
from src.services.config import AppConfig
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Helpers / factories
# ---------------------------------------------------------------------------

_FAKE_CONFIG = AppConfig(
    server_url="https://fake-xnat.test/xnat/",
    project_name="FAKE_PROJECT",
)


def _factory_from_fake(fake: FakeXNAT):
    """Return a connect_factory that ignores (url, user, pwd) and returns *fake*."""
    def _factory(url: str, user: str, password: str) -> FakeXNAT:
        # Must not store password anywhere.
        return fake
    return _factory


def _raising_factory(exc: BaseException):
    """Return a connect_factory that raises *exc* (simulates no network)."""
    def _factory(url: str, user: str, password: str) -> Any:
        raise exc
    return _factory


# ---------------------------------------------------------------------------
# Test 1: Successful login
# ---------------------------------------------------------------------------

def test_successful_login_returns_ok():
    """FakeXNAT.get('/') succeeds → LoginResult.ok True, server set."""
    fake = FakeXNAT()
    result = attempt_login(
        "testuser",
        "correct_password",  # noqa: S106 — test cred only, never real
        connect_factory=_factory_from_fake(fake),
        config=_FAKE_CONFIG,
    )

    assert result.ok is True
    assert result.friendly is None
    assert result.server is fake
    assert result.username == "testuser"


# ---------------------------------------------------------------------------
# Test 2: VPN down (connect_factory raises before any probe)
# ---------------------------------------------------------------------------

def test_vpn_down_returns_friendly_error_no_exception():
    """connect_factory raises ConnectionError → ok False, VPN/retry recourse."""
    result = attempt_login(
        "someuser",
        "some_password",  # noqa: S106
        connect_factory=_raising_factory(ConnectionError("Network unreachable")),
        config=_FAKE_CONFIG,
    )

    assert result.ok is False
    assert isinstance(result.friendly, FriendlyError)
    assert result.server is None

    fe = result.friendly
    # Title must mention server / connection
    assert "XNAT server" in fe.title or "reach" in fe.title.lower()
    # Recourse must mention VPN
    recourse_text = " ".join(fe.recourse).lower()
    assert "vpn" in recourse_text or "retry" in recourse_text


def test_vpn_down_does_not_raise():
    """No exception must escape attempt_login for network failures."""
    try:
        result = attempt_login(
            "user",
            "pw",  # noqa: S106
            connect_factory=_raising_factory(OSError("timeout")),
            config=_FAKE_CONFIG,
        )
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"attempt_login raised unexpectedly: {exc}")

    assert result.ok is False


# ---------------------------------------------------------------------------
# Test 3: Bad credentials (server reachable but .get('/') raises on creds)
# ---------------------------------------------------------------------------

def test_bad_credentials_returns_friendly_error():
    """FakeXNAT.get raises PermissionError (bad creds) → ok False, re-enter recourse."""
    fake = FakeXNAT()
    # Inject a failure that fires on the first call to server.get('/')
    # We need the SSL check to pass but credential check to fail.
    # Strategy: monkeypatch fake.get to raise on first call (SSL check returns None
    # because it's not an SSLCertVerificationError), then raise on the reachability
    # check — but that would show a VPN error.
    # Better: let SSL pass, let reachability pass, fail on creds.
    # check_ssl_certificate only catches SSLCertVerificationError — anything else
    # returns None (passes).  check_server_reachable catches ALL exceptions.
    # So we need to make sure SSL check sees a non-SSL error to pass through, AND
    # server_reachable passes, AND credentials fails.
    #
    # Simplest approach: subclass FakeXNAT to raise PermissionError on get().
    call_count = [0]

    class _BadCredsFake(FakeXNAT):
        def get(self, path: str) -> None:
            call_count[0] += 1
            if call_count[0] >= 1:
                # All probes fail with PermissionError (bad creds — not SSL).
                # check_ssl_certificate: PermissionError is not SSLCertVerificationError
                # → returns None (passes SSL check).
                # check_server_reachable: PermissionError → returns FriendlyError
                # → attempt_login returns with VPN-style message.
                #
                # To specifically test credential failure we need to let the first
                # two checks pass (SSL + reachability) and only fail on creds.
                # Since all three checks call server.get('/'), we use call count.
                if call_count[0] <= 2:
                    return None  # SSL check and reachability pass
                raise PermissionError("401 Unauthorized")

    bad_fake = _BadCredsFake()

    result = attempt_login(
        "wronguser",
        "wrongpassword",  # noqa: S106
        connect_factory=_factory_from_fake(bad_fake),
        config=_FAKE_CONFIG,
    )

    assert result.ok is False
    assert isinstance(result.friendly, FriendlyError)
    assert result.server is None

    fe = result.friendly
    # Must mention login/credentials/password in title or recourse
    title_lower = fe.title.lower()
    recourse_text = " ".join(fe.recourse).lower()
    assert (
        "login" in title_lower
        or "credential" in title_lower
        or "password" in recourse_text
        or "username" in recourse_text
    )


# ---------------------------------------------------------------------------
# Test 4: Expired SSL certificate
# ---------------------------------------------------------------------------

def test_expired_cert_returns_friendly_error():
    """FakeXNAT.get raises SSLCertVerificationError → ok False, dated cert recourse."""

    class _SSLFake(FakeXNAT):
        def get(self, path: str) -> None:
            # Raise an SSLCertVerificationError — triggers check_ssl_certificate path.
            raise ssl.SSLCertVerificationError("certificate has expired")

    ssl_fake = _SSLFake()

    result = attempt_login(
        "certuser",
        "certpw",  # noqa: S106
        connect_factory=_factory_from_fake(ssl_fake),
        config=_FAKE_CONFIG,
    )

    assert result.ok is False
    assert isinstance(result.friendly, FriendlyError)
    assert result.server is None

    fe = result.friendly
    # Title must mention certificate / expired
    title_lower = fe.title.lower()
    assert "certificate" in title_lower or "cert" in title_lower or "expired" in title_lower

    # Recourse must mention the Data Librarian
    recourse_text = " ".join(fe.recourse).lower()
    assert "librarian" in recourse_text or "data librarian" in recourse_text


def test_expired_cert_does_not_raise():
    """No exception escapes for SSL cert failures."""
    class _SSLFake(FakeXNAT):
        def get(self, path: str) -> None:
            raise ssl.SSLCertVerificationError("expired")

    try:
        result = attempt_login(
            "u",
            "p",  # noqa: S106
            connect_factory=_factory_from_fake(_SSLFake()),
            config=_FAKE_CONFIG,
        )
    except Exception as exc:  # pragma: no cover
        pytest.fail(f"attempt_login raised for SSL error: {exc}")

    assert result.ok is False


# ---------------------------------------------------------------------------
# Test 5: Password never stored in returned object
# ---------------------------------------------------------------------------

def test_password_never_in_login_result():
    """LoginResult must not carry the password anywhere in public attributes."""
    fake = FakeXNAT()
    secret = "super_secret_password_12345"  # noqa: S105

    result = attempt_login(
        "checkuser",
        secret,
        connect_factory=_factory_from_fake(fake),
        config=_FAKE_CONFIG,
    )

    # Inspect all public attributes of the result dataclass.
    for attr_name in vars(result):
        if attr_name.startswith("_"):
            continue  # private fields excluded
        value = getattr(result, attr_name)
        assert secret not in str(value), (
            f"Password found in LoginResult.{attr_name} — credentials leak!"
        )


def test_password_not_in_friendly_error_fields():
    """Password must not appear in FriendlyError on failure."""
    secret = "another_secret_pw_9876"  # noqa: S105

    result = attempt_login(
        "someuser",
        secret,
        connect_factory=_raising_factory(ConnectionError("no network")),
        config=_FAKE_CONFIG,
    )

    assert result.ok is False
    fe = result.friendly
    assert fe is not None

    for attr_name in ("title", "message"):
        value = getattr(fe, attr_name, "")
        assert secret not in value, (
            f"Password found in FriendlyError.{attr_name} — credentials leak!"
        )

    for step in fe.recourse:
        assert secret not in step, "Password found in FriendlyError.recourse — credentials leak!"
