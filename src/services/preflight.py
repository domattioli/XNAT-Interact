"""
Pre-flight checks for XNAT-Interact.

All functions return ``FriendlyError | None``:
  - ``None``  → check passed
  - ``FriendlyError`` → check failed; show ``render(result)`` to the user

Design principle: every check takes a callable or a plain value so the
real network / filesystem is never touched in tests.  Tests inject
FakeXNAT-based callables or plain raising lambdas.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Union

from src.services.errors import FriendlyError, handle

# ---------------------------------------------------------------------------
# Standard recourse lists
# ---------------------------------------------------------------------------

_RECOURSE_RETRY_VPN = [
    "Make sure you are connected to the UIowa VPN, then retry.",
    "Retry the operation.",
    "If the problem persists, contact the Data Librarian.",
    "Copy this message and email it to the Data Librarian.",
]

_RECOURSE_BAD_CREDENTIALS = [
    "Re-enter your XNAT username and password.",
    "If you recently changed your UIowa password, log in to the XNAT web interface once first, then retry.",
    "Contact the Data Librarian if you continue to have trouble.",
]

_RECOURSE_MISSING_PATH = [
    "Check that the file or folder path is correct.",
    "Make sure the drive containing your data is connected and mounted.",
    "Re-enter the path and retry.",
    "Contact the Data Librarian if the path is unexpectedly missing.",
]

_RECOURSE_SSL = [
    "Contact the Data Librarian to report that the XNAT server certificate has expired.",
    "Do not attempt to upload data until the certificate is renewed.",
    "Copy this message and email it to the Data Librarian.",
]


# ---------------------------------------------------------------------------
# Public checks
# ---------------------------------------------------------------------------

def check_path_exists(path: Union[str, Path]) -> Optional[FriendlyError]:
    """
    Return a FriendlyError if *path* does not exist on the local filesystem.

    Parameters
    ----------
    path : str or Path — the file or directory to check.
    """
    p = Path(path)
    if p.exists():
        return None
    # Construct a synthetic FileNotFoundError for the diagnostic log
    exc = FileNotFoundError(f"Path not found: {path}")
    return handle(
        exc,
        title="File or folder not found",
        message=(
            f"The path '{path}' could not be found on your computer. "
            "Check that the drive is connected and the path is correct."
        ),
        recourse=_RECOURSE_MISSING_PATH,
        context=f"check_path_exists, path={path}",
    )


def check_server_reachable(connect_callable: Callable[[], None]) -> Optional[FriendlyError]:
    """
    Return a FriendlyError if calling *connect_callable* raises any exception.

    Intended use: pass a zero-argument callable that performs a lightweight
    liveness probe (e.g. ``lambda: server.get('/')``).  Tests inject a
    FakeXNAT-based or simply raising lambda.

    Parameters
    ----------
    connect_callable : ``() -> None`` — called once; any exception → failure.
    """
    try:
        connect_callable()
        return None
    except Exception as exc:
        return handle(
            exc,
            title="Cannot reach the XNAT server",
            message=(
                "XNAT-Interact could not connect to the XNAT server. "
                "This usually means you are not on the UIowa VPN, or the server is temporarily down."
            ),
            recourse=_RECOURSE_RETRY_VPN,
            context="check_server_reachable",
        )


def check_credentials(login_callable: Callable[[], None]) -> Optional[FriendlyError]:
    """
    Return a FriendlyError if calling *login_callable* raises any exception.

    Intended use: pass a zero-argument callable that performs an
    authenticated request and raises on auth failure.  Tests inject a
    FakeXNAT-based or raising lambda.

    Parameters
    ----------
    login_callable : ``() -> None`` — called once; any exception → failure.
    """
    try:
        login_callable()
        return None
    except Exception as exc:
        return handle(
            exc,
            title="Login failed",
            message=(
                "Your username or password was not accepted by the XNAT server. "
                "If you recently changed your UIowa password, log in to the XNAT "
                "web interface once first, then retry."
            ),
            recourse=_RECOURSE_BAD_CREDENTIALS,
            context="check_credentials",
        )


def check_ssl_certificate(connect_callable: Callable[[], None]) -> Optional[FriendlyError]:
    """
    Return a FriendlyError specifically for SSL certificate expiry.

    Detects ``ssl.SSLCertVerificationError`` and surfaces a dated,
    actionable message naming the Data Librarian.

    Parameters
    ----------
    connect_callable : ``() -> None`` — called once; SSL errors → friendly error.
    """
    import ssl as _ssl
    try:
        connect_callable()
        return None
    except (_ssl.SSLCertVerificationError,) as exc:
        return handle(
            exc,
            title="XNAT server certificate has expired",
            message=(
                "The XNAT server's security certificate has expired. "
                "You cannot connect until it is renewed. "
                "Contact the Data Librarian (dmattioli / stelong) to renew it."
            ),
            recourse=_RECOURSE_SSL,
            context="check_ssl_certificate",
        )
    except Exception:
        # Not an SSL error — not our responsibility here
        return None
