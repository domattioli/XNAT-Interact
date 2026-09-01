"""
Dedup review lane — test dedup detection on overlapping cases.

Usage:
    python -m tests.stress.lane_dedup --project STRESS_DEDUP

Base surgery: uid=DEDUP_BASE, seeds={1..6}, publish.
Then for each overlap case (exact/subset/superset/partial/disjoint):
  - Build a NEW uid (DEDUP_<CASE>)
  - Attempt publish
  - Catch DedupReviewRequired specifically
  - Record: ok/error, whether DedupReviewRequired fired, what landed on server

Per #32 Q3 invariant: exact/subset/superset/partial should reject (DedupReviewRequired),
disjoint should accept. Check for empty shells afterward.

JSON results to tests/stress/results/dedup_<timestamp>.json with case table.
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
        description="Dedup stress test for XNAT-Interact"
    )
    parser.add_argument(
        "--project",
        type=str,
        default="STRESS_DEDUP",
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
    from tests.stress.factory import make_surgery, overlap_cases
    from src.xnat_experiment_data import DedupReviewRequired

    print("=" * 70)
    print("DEDUP STRESS TEST")
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

    # STEP 1: Publish base surgery
    base_uid = "DEDUP_BASE"
    base_seeds = {1, 2, 3, 4, 5, 6}

    print(f"\nBASE CASE: {base_uid} (seeds={base_seeds})")
    print("-" * 70)

    temp_dir = Path("/tmp") / "stress_dedup_base"
    temp_dir.mkdir(parents=True, exist_ok=True)
    base_surgery_dir = make_surgery(base_uid, seeds=base_seeds, dest_dir=temp_dir)

    base_result = publish_surgery(conn, cfg, login, base_surgery_dir, base_uid, verbose=False)
    if base_result["ok"]:
        print(f"  {base_uid}: ok")
    else:
        print(f"  {base_uid}: FAILED — {base_result['error'][:100]}")
        print("Cannot continue without base case")
        shutil.rmtree(temp_dir, ignore_errors=True)
        sys.exit(1)

    shutil.rmtree(temp_dir, ignore_errors=True)

    # STEP 2: Overlap test cases
    print("\nOVERLAP TEST CASES:")
    print("-" * 70)

    overlap_test_cases = overlap_cases(base_seeds)

    for case_name, case_seeds in overlap_test_cases.items():
        uid = f"DEDUP_{case_name.upper()}"
        print(f"\n{case_name}:")

        result = {
            "case": case_name,
            "uid": uid,
            "expected": (
                "reject" if case_name in ["exact", "subset", "superset", "partial"]
                else "accept"
            ),
            "observed": "unknown",
            "dedup_review_required": False,
            "error": None,
            "empty_shells_before": [],
            "empty_shells_after": [],
        }

        try:
            # Create temp surgery dir
            temp_dir = Path("/tmp") / f"stress_dedup_{case_name}"
            temp_dir.mkdir(parents=True, exist_ok=True)
            case_surgery_dir = make_surgery(uid, seeds=case_seeds, dest_dir=temp_dir)

            # Inventory before
            inv_before = server_inventory(args.url, args.user, args.password, args.project)
            result["empty_shells_before"] = empty_shells(inv_before)

            # Attempt publish
            pub_result = publish_surgery(
                conn, cfg, login, case_surgery_dir, uid, verbose=False
            )

            # Inventory after
            inv_after = server_inventory(args.url, args.user, args.password, args.project)
            result["empty_shells_after"] = empty_shells(inv_after)

            if pub_result["ok"]:
                result["observed"] = "accept"
                print("  outcome: ACCEPTED (ok=True)")
            else:
                error_msg = pub_result.get("error", "")
                if "DedupReviewRequired" in error_msg:
                    result["dedup_review_required"] = True
                    result["observed"] = "reject"
                    print("  outcome: REJECTED (DedupReviewRequired)")
                else:
                    result["observed"] = "error"
                    result["error"] = error_msg[:300]
                    print("  outcome: ERROR")

        except DedupReviewRequired as e:
            # Caught at publish_surgery level
            result["dedup_review_required"] = True
            result["observed"] = "reject"
            print("  outcome: REJECTED (DedupReviewRequired)")

            # Try to get inventory after
            try:
                inv_after = server_inventory(
                    args.url, args.user, args.password, args.project
                )
                result["empty_shells_after"] = empty_shells(inv_after)
            except Exception:
                pass

        except Exception as e:
            error_msg = f"{type(e).__name__}: {str(e)}"
            result["error"] = error_msg[:300]
            result["observed"] = "error"
            print(f"  outcome: ERROR ({type(e).__name__})")

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
            temp_dir = Path("/tmp") / f"stress_dedup_{case_name}"
            shutil.rmtree(temp_dir, ignore_errors=True)

        results["test_cases"].append(result)

    # Summary table
    print("\n" + "=" * 70)
    print("VERDICT TABLE:")
    print("-" * 70)
    print(f"{'case':<12} {'expected':<8} {'observed':<8} {'match':<6}")
    print("-" * 70)

    mismatches = 0
    for tc in results["test_cases"]:
        match = "✓" if tc["expected"] == tc["observed"] else "✗"
        if tc["expected"] != tc["observed"]:
            mismatches += 1
        print(
            f"{tc['case']:<12} {tc['expected']:<8} {tc['observed']:<8} {match:<6}"
        )

    # Write JSON results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"dedup_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to: {results_file}")

    print("=" * 70)
    if mismatches > 0:
        print(f"FAILURES: {mismatches} test case(s) did not match expected behavior")
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
