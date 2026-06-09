"""
Friendly error reporting for XNAT-Interact.

PHI-FREE RULE (HARD CONSTRAINT)
--------------------------------
Diagnostic logs written by ``handle()`` MUST contain ONLY:
  - The exception type (class name)
  - The exception message (str(exc))
  - The Python traceback

They MUST NEVER contain:
  - DICOM datasets or pixel data
  - Patient identifiers (name, DOB, MRN, etc.)
  - Intake-form contents
  - Full data-structure dumps

The ``context`` parameter in ``handle()`` is caller-controlled; callers are
responsible for passing ONLY non-PHI strings (e.g. a file path, an operation
name, a row index).  Passing PHI via ``context`` violates this contract.
"""
from __future__ import annotations

import tempfile
import traceback
from dataclasses import dataclass, field
from typing import List, Optional


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class FriendlyError:
    """
    User-facing error with plain-language message and recourse options.

    Fields
    ------
    title             : Short headline shown to the user.
    message           : One or two plain-language sentences explaining what
                        went wrong, suitable for a research-lab student.
    recourse          : Ordered list of concrete next steps the user can take.
    diagnostic_log_path: Path to the PHI-free diagnostic log file, or None if
                        no log was written (e.g. errors created without a
                        source exception).
    _original_exc     : Original exception kept for internal use only; NOT
                        shown to the user and NOT written to logs directly
                        (the log is written by ``handle()`` before this object
                        is returned).
    """
    title: str
    message: str
    recourse: List[str] = field(default_factory=list)
    diagnostic_log_path: Optional[str] = None
    _original_exc: Optional[BaseException] = field(
        default=None, repr=False, compare=False
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def handle(
    exc: BaseException,
    *,
    title: str,
    message: str,
    recourse: List[str],
    context: Optional[str] = None,
) -> FriendlyError:
    """
    Create a :class:`FriendlyError` and write a PHI-free diagnostic log.

    Parameters
    ----------
    exc      : The original exception.
    title    : Short headline for the user.
    message  : Plain-language explanation for the user.
    recourse : Ordered list of next-step strings.
    context  : Optional caller-controlled non-PHI string appended to the log
               header (e.g. ``"operation=pull_from_xnat, row=42"``).
               **Callers must not pass PHI here.**

    Returns
    -------
    FriendlyError
        Carries the log path so the caller can show it to the user.
    """
    # Build the log content — exc type + message + traceback only
    lines: List[str] = [
        "=== XNAT-Interact Diagnostic Log ===",
        "PHI-FREE: contains only exception type, message, and traceback.",
        "",
    ]
    if context is not None:
        lines.append(f"Context (non-PHI, caller-supplied): {context}")
        lines.append("")
    lines.append(f"Exception type : {type(exc).__qualname__}")
    lines.append(f"Exception      : {exc}")
    lines.append("")
    lines.append("Traceback:")
    lines.append("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))

    log_text = "\n".join(lines)

    # Write to a temp file so the path survives the call
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".txt",
        prefix="xnat_interact_diag_",
        delete=False,
        encoding="utf-8",
    ) as fh:
        fh.write(log_text)
        log_path = fh.name

    return FriendlyError(
        title=title,
        message=message,
        recourse=recourse,
        diagnostic_log_path=log_path,
        _original_exc=exc,
    )


def render(fe: FriendlyError) -> str:
    """
    Return a single user-facing string for display in a terminal or dialog.

    Format::

        *** <title> ***

        <message>

        What you can do:
          1. <recourse[0]>
          2. <recourse[1]>
          ...

        Details saved to: <path>   (omitted if no log)
    """
    parts: List[str] = [
        f"*** {fe.title} ***",
        "",
        fe.message,
        "",
        "What you can do:",
    ]
    for i, step in enumerate(fe.recourse, start=1):
        parts.append(f"  {i}. {step}")

    if fe.diagnostic_log_path:
        parts.append("")
        parts.append(f"Details saved to: {fe.diagnostic_log_path}")

    return "\n".join(parts)
