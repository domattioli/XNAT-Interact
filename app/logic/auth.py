"""
app/logic/auth — pure login logic.

NO streamlit imports here.  This module is imported by tests that run in a
virtualenv where streamlit is NOT installed.  Any st.* call here breaks CI.

Public API
----------
attempt_login(username, password, *, connect_factory) -> LoginResult

connect_factory is a callable:
    connect_factory(url: str, user: str, password: str) -> server_handle

The real default factory is src.services.xnat_gateway.build_server.
Tests inject a FakeXNAT-backed factory.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

from src.services.config import AppConfig
from src.services.errors import FriendlyError
from src.services.preflight import (
    check_credentials,
    check_server_reachable,
    check_ssl_certificate,
)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass
class LoginResult:
    """
    Outcome of attempt_login().

    Fields
    ------
    ok          : True  → login succeeded; connection is live in .server.
    friendly    : None when ok; FriendlyError when ok is False.
    server      : Server handle on success; None on failure.
                  NEVER inspect this field when ok is False.
    username    : The username that was used (never the password).
    """
    ok: bool
    friendly: Optional[FriendlyError]
    server: Any  # pyxnat.Interface or FakeXNAT; None on failure
    username: str
    # Hard rule: password is never stored on this object.
    # The field does not exist — not even as None.


# ---------------------------------------------------------------------------
# Default connect factory (real pyxnat; swapped out in tests)
# ---------------------------------------------------------------------------

def _default_connect_factory(url: str, user: str, password: str):
    """Production: calls xnat_gateway.build_server (imports pyxnat locally)."""
    from src.services.xnat_gateway import build_server  # noqa: PLC0415
    return build_server(url=url, user=user, password=password)


# ---------------------------------------------------------------------------
# Public function
# ---------------------------------------------------------------------------

def attempt_login(
    username: str,
    password: str,
    *,
    connect_factory: Callable[..., Any] = _default_connect_factory,
    config: Optional[AppConfig] = None,
) -> LoginResult:
    """
    Run preflight checks and attempt an authenticated connection.

    Steps
    -----
    1. SSL-cert check (catch expired certs before we even try auth).
    2. Server-reachability check (VPN/network).
    3. Credentials check (authenticated probe).
    4. If all pass → return LoginResult(ok=True, server=<handle>).

    Any foreseeable failure returns LoginResult(ok=False, friendly=<FriendlyError>).
    No exception escapes this function for expected auth/network/cert failures.

    Parameters
    ----------
    username        : XNAT username (not stored in result).
    password        : XNAT password (not stored in result; never logged).
    connect_factory : Callable(url, user, password) → server handle.
                      Real default: xnat_gateway.build_server.
                      Tests: lambda url, u, p: FakeXNAT(...)
    config          : AppConfig instance; defaults to AppConfig.load().

    Returns
    -------
    LoginResult — ok=True with live .server, or ok=False with .friendly set.
    """
    if config is None:
        config = AppConfig.load()

    url = config.server_url

    # Build the server object once (no network until we actually call it).
    try:
        server = connect_factory(url, username, password)
    except Exception as exc:
        # Factory itself raised before any probe — treat as unreachable.
        from src.services.errors import handle  # noqa: PLC0415
        fe = handle(
            exc,
            title="Cannot reach the XNAT server",
            message=(
                "XNAT-Interact could not connect to the XNAT server. "
                "This usually means you are not on the UIowa VPN, "
                "or the server is temporarily down."
            ),
            recourse=[
                "Make sure you are connected to the UIowa VPN, then retry.",
                "Retry the operation.",
                "If the problem persists, contact the Data Librarian.",
                "Copy this message and email it to the Data Librarian.",
            ],
            context="attempt_login/connect_factory",
        )
        return LoginResult(ok=False, friendly=fe, server=None, username=username)

    # ------------------------------------------------------------------
    # 1. SSL certificate check
    # ------------------------------------------------------------------
    ssl_err = check_ssl_certificate(lambda: server.get("/"))
    if ssl_err is not None:
        return LoginResult(ok=False, friendly=ssl_err, server=None, username=username)

    # ------------------------------------------------------------------
    # 2. Server reachability (VPN / network)
    # ------------------------------------------------------------------
    reach_err = check_server_reachable(lambda: server.get("/"))
    if reach_err is not None:
        return LoginResult(ok=False, friendly=reach_err, server=None, username=username)

    # ------------------------------------------------------------------
    # 3. Credentials check — authenticated request
    # ------------------------------------------------------------------
    # Use a lightweight authenticated call to verify credentials.
    # server.get('/') with bad creds raises on some XNAT versions;
    # alternatively, we delegate to check_credentials with the same probe.
    cred_err = check_credentials(lambda: server.get("/"))
    if cred_err is not None:
        return LoginResult(ok=False, friendly=cred_err, server=None, username=username)

    # ------------------------------------------------------------------
    # All checks passed.
    # ------------------------------------------------------------------
    return LoginResult(ok=True, friendly=None, server=server, username=username)
