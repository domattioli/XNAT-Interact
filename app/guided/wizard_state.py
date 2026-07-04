"""
app/guided/wizard_state — Session-state helpers for the guided (novice) app.

Manages wizard navigation, form accumulation, and task routing.
Keys are namespaced under "guided_*" to avoid collision with app.state.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import streamlit as st


# ---------------------------------------------------------------------------
# Session-state key constants (guided_* prefix for namespacing)
# ---------------------------------------------------------------------------

_KEY_CURRENT_TASK = "guided_current_task"     # None | 'upload' | 'download' | 'annotations'
_KEY_WIZARD_STEP = "guided_wizard_step"       # int, current step number
_KEY_FORM_VALUES = "guided_form_values"       # dict, accumulated form data
_KEY_UPLOADED = "guided_uploaded"             # bool, whether current task completed
_KEY_PRIVACY_AFFIRMED = "guided_privacy_affirmed"  # bool, privacy-check gate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get(key: str, default: Any = None) -> Any:
    return st.session_state.get(key, default)


def _set(key: str, value: Any) -> None:
    st.session_state[key] = value


# ---------------------------------------------------------------------------
# Task routing
# ---------------------------------------------------------------------------

def get_task() -> Optional[str]:
    """Return current task ('upload', 'download', 'annotations', or None)."""
    return _get(_KEY_CURRENT_TASK)


def set_task(task: Optional[str]) -> None:
    """Set current task. Set to None to return home."""
    _set(_KEY_CURRENT_TASK, task)


# ---------------------------------------------------------------------------
# Wizard step
# ---------------------------------------------------------------------------

def get_step() -> int:
    """Return current step number (0-indexed)."""
    return _get(_KEY_WIZARD_STEP, 0)


def set_step(step: int) -> None:
    """Set current step number."""
    _set(_KEY_WIZARD_STEP, step)


# ---------------------------------------------------------------------------
# Form accumulation
# ---------------------------------------------------------------------------

def get_form() -> Dict[str, Any]:
    """Return accumulated form values dict."""
    return _get(_KEY_FORM_VALUES, {})


def update_form(updates: Dict[str, Any]) -> None:
    """Merge updates into the accumulated form dict."""
    current = get_form()
    current.update(updates)
    _set(_KEY_FORM_VALUES, current)


def set_form(form_values: Dict[str, Any]) -> None:
    """Replace the entire form dict."""
    _set(_KEY_FORM_VALUES, form_values)


# ---------------------------------------------------------------------------
# Privacy affirmation (for upload wizard)
# ---------------------------------------------------------------------------

def is_privacy_affirmed() -> bool:
    """Return True if user has affirmed privacy check."""
    return _get(_KEY_PRIVACY_AFFIRMED, False)


def set_privacy_affirmed(affirmed: bool) -> None:
    """Set privacy affirmation state."""
    _set(_KEY_PRIVACY_AFFIRMED, affirmed)


# ---------------------------------------------------------------------------
# Upload state
# ---------------------------------------------------------------------------

def is_uploaded() -> bool:
    """Return True if current task completed successfully."""
    return _get(_KEY_UPLOADED, False)


def set_uploaded(uploaded: bool) -> None:
    """Mark task as uploaded."""
    _set(_KEY_UPLOADED, uploaded)


# ---------------------------------------------------------------------------
# Reset wizard
# ---------------------------------------------------------------------------

def reset_wizard() -> None:
    """Clear all wizard state (task, step, form, privacy, upload flags)."""
    for key in (
        _KEY_CURRENT_TASK,
        _KEY_WIZARD_STEP,
        _KEY_FORM_VALUES,
        _KEY_UPLOADED,
        _KEY_PRIVACY_AFFIRMED,
    ):
        if key in st.session_state:
            del st.session_state[key]
