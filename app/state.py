"""
app/state — session-state helpers for the Streamlit app.

All st.session_state interactions are isolated here.  Non-trivial logic
(e.g. auth result handling) lives in app/logic/* and is called from here
or from pages; the pages themselves stay thin.

NOTE: this module DOES import streamlit.  Do NOT import it from tested
      logic modules (app/logic/*.py) — that would pull streamlit into the
      test environment where it is NOT installed.
"""
from __future__ import annotations

from typing import Any, Optional

import streamlit as st

# ---------------------------------------------------------------------------
# Session-state key constants
# ---------------------------------------------------------------------------

_KEY_AUTHENTICATED   = "xnat_authenticated"
_KEY_USERNAME        = "xnat_username"
_KEY_SERVER          = "xnat_server_handle"
_KEY_CURRENT_PAGE    = "xnat_current_page"

# Valid page names used by the router
PAGE_LOGIN    = "login"
PAGE_BROWSE   = "browse"
PAGE_UPLOAD   = "upload"
PAGE_BATCH    = "batch"
PAGE_DOWNLOAD = "download"
PAGE_LEARN    = "learn"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(key: str, default: Any = None) -> Any:
    return st.session_state.get(key, default)


def _set(key: str, value: Any) -> None:
    st.session_state[key] = value


# ---------------------------------------------------------------------------
# Auth state
# ---------------------------------------------------------------------------

def is_authenticated() -> bool:
    """True if the user has a live authenticated session."""
    return bool(_get(_KEY_AUTHENTICATED, False))


def set_authenticated(username: str, server_handle: Any) -> None:
    """
    Mark the session as authenticated.

    Parameters
    ----------
    username      : Authenticated XNAT username (stored for display only).
    server_handle : Live server object (pyxnat.Interface or FakeXNAT in tests).
                    PHI-free — the handle itself is not PHI.
    """
    _set(_KEY_AUTHENTICATED, True)
    _set(_KEY_USERNAME, username)
    _set(_KEY_SERVER, server_handle)
    _set(_KEY_CURRENT_PAGE, PAGE_BROWSE)


def clear_auth() -> None:
    """Log out: wipe auth state and cached server data.  Redirects to login."""
    for key in (_KEY_AUTHENTICATED, _KEY_USERNAME, _KEY_SERVER):
        if key in st.session_state:
            del st.session_state[key]
    _set(_KEY_CURRENT_PAGE, PAGE_LOGIN)


def get_username() -> Optional[str]:
    """Return the authenticated username, or None if not logged in."""
    return _get(_KEY_USERNAME)


def get_server() -> Optional[Any]:
    """Return the live server handle, or None if not authenticated."""
    return _get(_KEY_SERVER)


# ---------------------------------------------------------------------------
# Page routing
# ---------------------------------------------------------------------------

def get_current_page() -> str:
    """Return the current page name (defaults to login if not set)."""
    return _get(_KEY_CURRENT_PAGE, PAGE_LOGIN)


def set_current_page(page: str) -> None:
    """Navigate to *page* by updating session state."""
    _set(_KEY_CURRENT_PAGE, page)
