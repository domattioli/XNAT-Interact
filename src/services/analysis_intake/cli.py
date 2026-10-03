"""
Command line for the analysis intake (spec 016, FR-021).

    main.py publish-analysis <folder> [--manifest FILE] [--inputs FOLDER] [--dry-run]
    main.py publish-analysis --init <type_name> <folder>
    main.py publish-analysis --list-types

There is deliberately no password or server option: a real publish logs in
through the normal XNAT-Interact login prompt.  Exit codes: 0 done or dry run
passed, 1 refused, 2 published but not confirmed or not cataloged.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable, List, Optional

from src.services.errors import render
from src.services.analysis_intake import IntakeRefusal, load_types, run_intake, write_descriptor_template


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:  # plain message, no usage dump traceback
        raise IntakeRefusal(__import__("src.services.errors", fromlist=["FriendlyError"]).FriendlyError(
            title="Command not understood", message=message,
            recourse=["Run 'publish-analysis --help' to see the options."]))


def build_parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="main.py publish-analysis",
                     description="Check an analysis result folder and publish it to XNAT.")
    parser.add_argument("folder", nargs="?", help="the output folder holding analysis.json and the result files")
    parser.add_argument("--manifest", help="the download_manifest.json written when you downloaded the case")
    parser.add_argument("--inputs", help="the folder of input frames, when no download record exists")
    parser.add_argument("--dry-run", action="store_true", help="run every check but upload nothing")
    parser.add_argument("--init", metavar="TYPE_NAME", help="write an analysis.json template for this type into the folder")
    parser.add_argument("--list-types", action="store_true", help="list the registered analysis types")
    parser.add_argument("--username", help="your XNAT username (you will be asked for the password)")
    return parser


def _default_connect(username: Optional[str]):
    # Imported here so --init, --list-types and --dry-run need no XNAT libraries or login.
    from main import try_login_and_connection
    validated_login, xnat_connection, config = try_login_and_connection(username=username, verbose=False)
    return validated_login.validated_username, xnat_connection, config


def main(argv: List[str], *, connect: Optional[Callable] = None, out: Callable[[str], None] = print) -> int:
    """Run the subcommand.  Returns the exit code."""
    try:
        args = build_parser().parse_args(argv)
        if args.list_types:
            for name, atype in sorted(load_types().items()):
                out(f"{name} (version {atype.type_version}): {atype.description}")
            return 0
        if not args.folder:
            raise IntakeRefusal(__import__("src.services.errors", fromlist=["FriendlyError"]).FriendlyError(
                title="No folder given", message="Name the output folder to check.",
                recourse=["Example: main.py publish-analysis path/to/results --dry-run"]))
        if args.init:
            path = write_descriptor_template(args.init, Path(args.folder))
            out(f"Wrote {path}. Fill in case_uid and code_ref (and parameters and notes if you like), then run the command on the folder.")
            return 0
        manifest = Path(args.manifest) if args.manifest else None
        inputs = Path(args.inputs) if args.inputs else None
        if args.dry_run:
            result = run_intake(Path(args.folder), username=args.username or "dry-run", manifest=manifest,
                                inputs=inputs, dry_run=True)
            connection = None
        else:
            username, connection, config = (connect or _default_connect)(args.username)
            try:
                result = run_intake(Path(args.folder), gateway=connection.gateway, config_tables=config,
                                    username=username, manifest=manifest, inputs=inputs)
            finally:
                try:
                    connection.close()
                except Exception:  # noqa: BLE001 - closing is best effort
                    pass
    except IntakeRefusal as exc:
        out(render(exc.friendly))
        return 1
    except SystemExit as exc:  # --help
        return int(exc.code or 0)
    for w in result.warnings:
        out(f"Note: {w}")
    if result.status == "dry_run_ok":
        out("Every check passed. Nothing was uploaded (dry run).")
        return 0
    if result.status == "done":
        out(f"Published and confirmed as '{result.label}'. The catalog has been updated.")
        return 0
    out(render(result.friendly))
    return 1 if result.status == "refused" else 2
