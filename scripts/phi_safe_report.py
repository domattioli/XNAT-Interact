#!/usr/bin/env python3
"""
Print a PHI-safe report over a folder of real imaging files.

Usage:
    python scripts/phi_safe_report.py <folder> [--profiles-dir DIR] [--model-dir DIR]

The report is JSON built only from counts, booleans, fixed enum codes, pixel
box coordinates and 8-character digests of device keys. No tag value, file
name or exception text can appear in it. On any failure the output is a fixed
two-field report naming only the exception class.

Exit status is 0 when a report was produced and 2 when the run was blocked.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def main(argv=None) -> int:
    from src.services.phi_safe.gate import blocked
    from src.services.phi_safe.probes import run_all

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("folder")
    parser.add_argument("--profiles-dir", default=str(REPO / "data" / "device_profiles"))
    parser.add_argument("--model-dir", default=str(REPO / "models" / "craft"))
    real_stdout = sys.stdout
    try:
        # argparse echoes argv on error; send that to the sink as well.
        with open(os.devnull, "w") as sink:
            sys.stdout = sys.stderr = sink
            args = parser.parse_args(argv)
        sys.stdout = real_stdout
        folder = Path(args.folder)
        if not folder.is_dir():
            raise NotADirectoryError()
        # Nothing printed by a library between here and the final write may
        # reach the terminal, including argparse's own echo of the path.
        with open(os.devnull, "w") as sink:
            sys.stdout = sys.stderr = sink
            report = run_all(folder, profiles_dir=args.profiles_dir, model_dir=args.model_dir)
            text = report.render()
        sys.stdout = real_stdout
        real_stdout.write(text + "\n")
        return 0
    except BaseException as exc:  # noqa: BLE001 - the report must never carry a message
        sys.stdout = real_stdout
        real_stdout.write(blocked(exc) + "\n")
        return 2


if __name__ == "__main__":
    sys.exit(main())
