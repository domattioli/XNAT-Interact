"""
app/logic/learn — CLI-equivalent command builder for Learn mode.

Pure string-builders only.  No streamlit import.  No subprocess/os.exec.
No credentials, no passwords, no PHI in any output string.

Purpose: let curious students see the equivalent CLI invocation for what
they just did in the GUI, so they can learn the command-line path by choice.
The GUI is the supported path; these snippets are informational only.
"""
from __future__ import annotations

from typing import Any, Dict, Optional


# ---------------------------------------------------------------------------
# Upload snippet
# ---------------------------------------------------------------------------

def cli_for_upload(form_values: Dict[str, Any]) -> str:
    """
    Return a CLI-equivalent string for a single-case upload.

    Parameters
    ----------
    form_values : dict with optional keys:
        surgeon   (str) — surgeon identifier (no PHI)
        site      (str) — site code
        procedure (str) — procedure type
        image_dir (str) — local folder path (no PHI in path assumed by caller)
        project   (str) — XNAT project name

    Credentials are NEVER included.  PHI-bearing values (patient name/ID) are
    intentionally absent — the form does not pass them here.
    """
    surgeon   = str(form_values.get("surgeon", "<surgeon>"))
    site      = str(form_values.get("site", "<site>"))
    procedure = str(form_values.get("procedure", "<procedure>"))
    image_dir = str(form_values.get("image_dir", "<path/to/images>"))
    project   = str(form_values.get("project", "<project>"))

    return (
        "python -m xnat_interact upload \\\n"
        f"    --surgeon '{surgeon}' \\\n"
        f"    --site '{site}' \\\n"
        f"    --procedure '{procedure}' \\\n"
        f"    --image-dir '{image_dir}' \\\n"
        f"    --project '{project}'"
    )


# ---------------------------------------------------------------------------
# Batch upload snippet
# ---------------------------------------------------------------------------

def cli_for_batch(xlsx_path: str = "<path/to/batch.xlsx>") -> str:
    """
    Return a CLI-equivalent string for a batch upload.

    Parameters
    ----------
    xlsx_path : path to the batch spreadsheet.  Must not contain PHI
                (the GUI validates and redacts before calling here).
    """
    return (
        "python -m xnat_interact batch-upload \\\n"
        f"    --xlsx '{xlsx_path}' \\\n"
        "    --continue-on-error"
    )


# ---------------------------------------------------------------------------
# Download snippet
# ---------------------------------------------------------------------------

def cli_for_download(
    selection: Optional[Dict[str, Any]] = None,
    dest: str = "<path/to/destination>",
) -> str:
    """
    Return a CLI-equivalent string for downloading selected cases.

    Parameters
    ----------
    selection : dict with optional keys:
        subject    (str) — subject label on XNAT (no PHI — XNAT labels are
                           de-identified identifiers, not patient names)
        experiment (str) — experiment label on XNAT
        project    (str) — XNAT project name
    dest      : local destination folder path
    """
    if selection is None:
        selection = {}

    subject    = str(selection.get("subject", "<subject-label>"))
    experiment = str(selection.get("experiment", "<experiment-label>"))
    project    = str(selection.get("project", "<project>"))

    return (
        "python -m xnat_interact download \\\n"
        f"    --project '{project}' \\\n"
        f"    --subject '{subject}' \\\n"
        f"    --experiment '{experiment}' \\\n"
        f"    --dest '{dest}'"
    )


# ---------------------------------------------------------------------------
# Browse / list snippet
# ---------------------------------------------------------------------------

def cli_for_browse(project: str = "<project>") -> str:
    """Return a CLI-equivalent string for listing server data."""
    return (
        "python -m xnat_interact list \\\n"
        f"    --project '{project}'"
    )


# ---------------------------------------------------------------------------
# Snippet index
# ---------------------------------------------------------------------------

def learn_snippets() -> Dict[str, Dict[str, str]]:
    """
    Return a dict of {action_key: {"label": ..., "command": ..., "description": ...}}.

    Provides static example snippets for the Learn panel.  All placeholder
    values use angle-bracket tokens — never real credentials or PHI.
    """
    return {
        "browse": {
            "label": "List cases on XNAT server",
            "command": cli_for_browse(),
            "description": (
                "List all subjects and experiments in your XNAT project. "
                "Equivalent to opening the Browse screen."
            ),
        },
        "upload": {
            "label": "Upload a single case",
            "command": cli_for_upload({}),
            "description": (
                "Upload one case to XNAT after de-identification. "
                "The GUI guides you through the same steps."
            ),
        },
        "batch_upload": {
            "label": "Batch upload from spreadsheet",
            "command": cli_for_batch(),
            "description": (
                "Upload multiple cases from an .xlsx spreadsheet. "
                "Failures are reported per-row; successful rows continue."
            ),
        },
        "download": {
            "label": "Download a case",
            "command": cli_for_download(),
            "description": (
                "Download a subject's data from XNAT to a local folder."
            ),
        },
    }
