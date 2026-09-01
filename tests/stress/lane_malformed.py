"""
Malformed DICOM stress lane — test robustness to corrupted input.

Usage:
    python -m tests.stress.lane_malformed --project STRESS_MAL

For each generator in malformed.py, builds a surgery dir with 2 good frames
+ the malformed file, attempts publish_surgery, and records outcome class:
  - FRIENDLY: GatewayError / FriendlyError-shaped message
  - CRASH: other exception + traceback head
  - ACCEPTED: ok=True (and check if malformed file landed on server)

JSON results to tests/stress/results/malformed_<timestamp>.json with verdict table.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import shutil
from pathlib import Path
from datetime import datetime

# Ensure env vars set BEFORE importing src
os.environ.setdefault("XNAT_SERVER_URL", "http://localhost:8080")
os.environ.setdefault("XNAT_IDENTITY_SALT", "ab12")


def main():
    parser = argparse.ArgumentParser(
        description="Malformed DICOM stress test for XNAT-Interact"
    )
    parser.add_argument(
        "--project",
        type=str,
        default="STRESS_MAL",
        help="XNAT project name",
    )
    parser.add_argument(
        "--url",
        type=str,
        default="http://localhost:8080",
        help="XNAT server URL",
    )
    parser.add_argument(
        "--user",
        type=str,
        default="admin",
        help="XNAT username",
    )
    parser.add_argument(
        "--password",
        type=str,
        default="admin",
        help="XNAT password",
    )

    args = parser.parse_args()
    os.environ["XNAT_PROJECT_NAME"] = args.project

    from tests.stress.driver import (
        connect,
        publish_surgery,
        server_inventory,
        empty_shells,
    )
    from tests.stress.factory import make_surgery
    from tests.stress import malformed

    print("=" * 70)
    print("MALFORMED DICOM STRESS TEST")
    print(f"  project:  {args.project}")
    print(f"  url:      {args.url}")
    print("=" * 70)

    # Connect
    try:
        conn, cfg, login = connect(args.url, args.project, args.user, args.password, verbose=False)
        print(f"Connected to {args.url}")
    except Exception as e:
        print(f"FAILED to connect: {e}")
        sys.exit(1)

    # Prepare results directory
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "timestamp": datetime.now().isoformat(),
        "project": args.project,
        "test_cases": [],
    }

    # Define malformed generators
    generators = [
        ("truncated", malformed.truncated_dicom),
        ("no_instance_number", malformed.no_instance_number),
        ("three_channel", malformed.three_channel),
        ("dup_private_tag", malformed.dup_private_tag),
        ("huge_surgery", malformed.huge_surgery),
        ("not_a_dicom", malformed.not_a_dicom),
    ]

    print("\nTEST CASES:")
    print("-" * 70)

    for case_name, generator in generators:
        uid = f"MAL_{case_name.upper()}"
        print(f"\n{case_name}:")

        result = {
            "case": case_name,
            "uid": uid,
            "outcome_class": "UNKNOWN",
            "error": None,
            "accepted": False,
            "empty_shells_before": [],
            "empty_shells_after": [],
        }

        try:
            # Create temp surgery dir
            temp_dir = Path("/tmp") / f"stress_mal_{case_name}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            surgery_dir = temp_dir / uid
            surgery_dir.mkdir(parents=True, exist_ok=True)

            # Add 2 good frames
            good_surgery = make_surgery(
                f"{uid}_good", seeds={1, 2}, dest_dir=temp_dir
            )
            for dcm_file in good_surgery.glob("*.dcm"):
                shutil.copy(dcm_file, surgery_dir / dcm_file.name)

            # Add malformed file
            bad_file = generator(surgery_dir)
            print(f"  malformed file: {bad_file.name}")

            # Inventory before
            inv_before = server_inventory(args.url, args.user, args.password, args.project)
            result["empty_shells_before"] = empty_shells(inv_before)

            # Attempt publish
            pub_result = publish_surgery(
                conn, cfg, login, surgery_dir, uid, verbose=False
            )

            # Inventory after
            inv_after = server_inventory(args.url, args.user, args.password, args.project)
            result["empty_shells_after"] = empty_shells(inv_after)

            if pub_result["ok"]:
                result["outcome_class"] = "ACCEPTED"
                result["accepted"] = True
                print("  outcome: ACCEPTED (ok=True)")
            else:
                error_msg = pub_result.get("error", "")
                # Classify error
                if (
                    "GatewayError" in error_msg
                    or "FriendlyError" in error_msg
                    or "Friendly" in error_msg
                ):
                    result["outcome_class"] = "FRIENDLY"
                    result["error"] = error_msg[:500]  # Truncate for JSON
                    print("  outcome: FRIENDLY")
                else:
                    result["outcome_class"] = "CRASH"
                    result["error"] = error_msg[:500]
                    print("  outcome: CRASH")

        except Exception as e:
            import traceback
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

            # Classify exception
            if (
                "GatewayError" in type(e).__name__
                or "FriendlyError" in type(e).__name__
            ):
                result["outcome_class"] = "FRIENDLY"
            else:
                result["outcome_class"] = "CRASH"

            result["error"] = error_msg[:500]
            print(f"  outcome: {result['outcome_class']}")

            # Still try to get inventory after
            try:
                inv_after = server_inventory(
                    args.url, args.user, args.password, args.project
                )
                result["empty_shells_after"] = empty_shells(inv_after)
            except Exception:
                pass

        finally:
            # Cleanup
            temp_dir = Path("/tmp") / f"stress_mal_{case_name}"
            shutil.rmtree(temp_dir, ignore_errors=True)

        results["test_cases"].append(result)

    # Summary table
    print("\n" + "=" * 70)
    print("VERDICT TABLE:")
    print("-" * 70)
    print(f"{'case':<20} {'outcome':<10} {'accepted':<10}")
    print("-" * 70)
    for tc in results["test_cases"]:
        acc = "yes" if tc["accepted"] else "no"
        print(f"{tc['case']:<20} {tc['outcome_class']:<10} {acc:<10}")

    # Write JSON results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"malformed_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to: {results_file}")

    print("=" * 70)
    sys.exit(0)


if __name__ == "__main__":
    main()
