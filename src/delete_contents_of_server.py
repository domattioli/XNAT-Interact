"""
delete_contents_of_server.py — Hard-delete helper for XNAT project contents.

USAGE (script mode):
    python -m src.delete_contents_of_server \\
        --server https://rpacs.iibi.uiowa.edu/xnat/ \\
        --username dmattioli \\
        --method subjects|metatables|both \\
        [--dry-run]

SECURITY REQUIREMENTS
---------------------
- Password is NEVER accepted via --password (or any argv flag).
  It is collected via interactive secure prompt (pwinput / getpass).
- An explicit typed confirmation is required before any deletion runs.
  The user must type the exact project name *or* the word DELETE.
- --dry-run lists what would be deleted and exits without touching the server.
- Deletion failures are NOT swallowed; they are re-raised via
  errors.handle / FriendlyError so the caller always sees what failed.
"""
from __future__ import annotations

import argparse
import sys
from typing import List

from pyxnat import Interface

from src.services.config import AppConfig
from src.services.errors import FriendlyError, handle, render

# ---------------------------------------------------------------------------
# Module-level project name resolved via AppConfig (env > config > default)
# ---------------------------------------------------------------------------
_cfg = AppConfig.load()
project_name: str = _cfg.project_name


# ---------------------------------------------------------------------------
# Core deletion helpers
# ---------------------------------------------------------------------------

def list_subjects(server: Interface) -> List[str]:
    """Return list of subject names in the project (no deletions)."""
    return list(server.select(f"/projects/{project_name}/subjects/*").get())  # type: ignore


def delete_subjects(server: Interface, *, dry_run: bool = False) -> None:
    """Delete all subjects in the project.

    Parameters
    ----------
    server  : Connected pyxnat Interface.
    dry_run : When True, list subjects but perform no deletions.

    Raises
    ------
    RuntimeError  : If any individual subject deletion fails.
    """
    all_s_names = list_subjects(server)

    if dry_run:
        print(f"\n[DRY-RUN] Would delete {len(all_s_names)} subject(s) from project '{project_name}':")
        for s in all_s_names:
            print(f"  - {s}")
        print("[DRY-RUN] No changes made.")
        return

    errors_seen: list[tuple[str, Exception]] = []
    for s in all_s_names:
        si = server.select(f"/projects/{project_name}/subjects/{s}")
        try:
            si.delete()  # type: ignore
        except Exception as exc:
            fe = handle(
                exc,
                title=f"Failed to delete subject '{s}'",
                message=(
                    f"Deletion of subject '{s}' from project '{project_name}' "
                    f"raised {type(exc).__name__}: {exc}"
                ),
                recourse=[
                    "Check that the XNAT connection is still open.",
                    "Verify you have owner/admin rights on the project.",
                    "Retry — if the problem persists contact the Data Librarian.",
                ],
                context=f"delete_subjects, subject={s}",
            )
            print(render(fe), file=sys.stderr)
            errors_seen.append((s, exc))

    if errors_seen:
        names = ", ".join(n for n, _ in errors_seen)
        raise RuntimeError(
            f"delete_subjects: {len(errors_seen)} subject(s) could not be deleted: {names}"
        )


def delete_metatables(server: Interface, *, dry_run: bool = False) -> None:
    """Delete the MetaTables.json resource from the project.

    Parameters
    ----------
    server  : Connected pyxnat Interface.
    dry_run : When True, describe the operation but perform no deletion.

    Raises
    ------
    RuntimeError  : If the deletion fails.
    """
    target = f"project '{project_name}' / resource 'MetaTables' / file 'MetaTables.json'"

    if dry_run:
        print(f"\n[DRY-RUN] Would delete: {target}")
        print("[DRY-RUN] No changes made.")
        return

    project_instance = server.select.project(project_name)
    try:
        project_instance.resource("MetaTables").file("MetaTables.json").delete()
    except Exception as exc:
        fe = handle(
            exc,
            title="Failed to delete MetaTables.json",
            message=(
                f"Deletion of MetaTables.json from project '{project_name}' "
                f"raised {type(exc).__name__}: {exc}"
            ),
            recourse=[
                "Check that the XNAT connection is still open.",
                "Verify you have owner/admin rights on the project.",
                "Retry — if the problem persists contact the Data Librarian.",
            ],
            context="delete_metatables",
        )
        print(render(fe), file=sys.stderr)
        raise RuntimeError(
            f"delete_metatables: MetaTables.json could not be deleted — {type(exc).__name__}: {exc}"
        ) from exc


# ---------------------------------------------------------------------------
# Confirmation gate
# ---------------------------------------------------------------------------

def _prompt_confirmation(project: str) -> bool:
    """Ask the user to type the project name or 'DELETE' to confirm.

    Returns True only when the user types the exact project name or 'DELETE'.
    """
    print(
        f"\n*** DESTRUCTIVE OPERATION WARNING ***\n"
        f"You are about to permanently delete data from XNAT project: '{project}'.\n"
        f"This action cannot be undone.\n\n"
        f"To confirm, type the project name ('{project}') or the word DELETE:\n"
    )
    answer = input("> ").strip()
    return answer == project or answer.upper() == "DELETE"


# ---------------------------------------------------------------------------
# Script entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete subjects / metatables from an XNAT project.",
        epilog=(
            "NOTE: --password has been intentionally removed. "
            "The password is collected via interactive secure prompt only."
        ),
    )
    parser.add_argument("--server",   required=True,  help="Server URL")
    parser.add_argument("--username", required=True,  help="Username")
    # --password intentionally absent — collected via secure prompt below
    parser.add_argument(
        "--method",
        required=True,
        choices=["both", "metatables", "subjects"],
        help="What to delete: subjects, metatables, or both",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="List what would be deleted; make no changes",
    )
    args = parser.parse_args()

    # Collect password via secure interactive prompt — never via argv
    try:
        import pwinput  # type: ignore
        password = pwinput.pwinput(prompt=f"\t{args.username.upper()} Password:\t", mask="*")
    except ImportError:
        import getpass
        password = getpass.getpass(prompt=f"\t{args.username.upper()} Password:\t")

    if not args.dry_run:
        confirmed = _prompt_confirmation(project_name)
        if not confirmed:
            print("Deletion cancelled — confirmation not given.")
            sys.exit(0)

    with Interface(args.server, args.username, password) as xnat:
        if args.method in ["subjects", "both"]:
            delete_subjects(server=xnat, dry_run=args.dry_run)
        if args.method in ["metatables", "both"]:
            delete_metatables(server=xnat, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n\t---\tDeletion of {args.method} successfully completed!\t---")
    else:
        print(f"\n\t---\tDry-run for {args.method} complete. No changes were made.\t---")
