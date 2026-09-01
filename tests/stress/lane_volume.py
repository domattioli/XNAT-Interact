"""
Volume stress lane — publish N surgeries sequentially.

Usage:
    python -m tests.stress.lane_volume --surgeries 10 --frames 20 --project STRESS_VOL

Records one line per surgery with timing/status, then prints summary + inventory.
JSON results to tests/stress/results/volume_<timestamp>.json.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime

# Ensure env vars are set BEFORE importing src
os.environ.setdefault("XNAT_SERVER_URL", "http://localhost:8080")
os.environ.setdefault("XNAT_IDENTITY_SALT", "ab12")


def main():
    parser = argparse.ArgumentParser(
        description="Sequential volume stress test for XNAT-Interact"
    )
    parser.add_argument(
        "--surgeries",
        type=int,
        default=10,
        help="Number of surgeries to publish",
    )
    parser.add_argument(
        "--frames",
        type=int,
        default=20,
        help="Frames per surgery",
    )
    parser.add_argument(
        "--project",
        type=str,
        default="STRESS_VOL",
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

    from tests.stress.driver import connect, publish_surgery, server_inventory, empty_shells
    from tests.stress.factory import make_surgery

    print("=" * 70)
    print("VOLUME STRESS TEST")
    print(f"  surgeries:  {args.surgeries}")
    print(f"  frames:     {args.frames}")
    print(f"  project:    {args.project}")
    print(f"  url:        {args.url}")
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

    # Run surgeries
    results = {
        "timestamp": datetime.now().isoformat(),
        "surgeries": args.surgeries,
        "frames": args.frames,
        "project": args.project,
        "publishes": [],
    }

    ok_count = 0
    fail_count = 0

    for i in range(args.surgeries):
        uid = f"STRESS_V_{i+1:04d}"

        # Make surgery directory with frames
        temp_dir = Path("/tmp") / f"stress_vol_{i}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        surgery_dir = make_surgery(uid, seeds=set(range(1, args.frames + 1)), dest_dir=temp_dir)

        # Publish
        pub_result = publish_surgery(conn, cfg, login, surgery_dir, uid, verbose=False)
        results["publishes"].append(pub_result)

        if pub_result["ok"]:
            ok_count += 1
            status = "ok"
        else:
            fail_count += 1
            status = "fail"

        print(
            f"  {uid:20} {status:4} s={pub_result['seconds']:6.2f} "
            f"files={pub_result['n_files']:3}"
        )

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print(f"  ok:       {ok_count}/{args.surgeries}")
    print(f"  fail:     {fail_count}/{args.surgeries}")

    if ok_count > 0:
        times = [r["seconds"] for r in results["publishes"] if r["ok"]]
        print(f"  mean s:   {sum(times)/len(times):.2f}")
        print(f"  min s:    {min(times):.2f}")
        print(f"  max s:    {max(times):.2f}")

    # Server inventory
    print("\nSERVER INVENTORY:")
    try:
        inv = server_inventory(args.url, args.user, args.password, args.project)
        print(f"  subjects:     {inv['subjects_count']}")
        print(f"  experiments:  {inv['experiments_count']}")

        shells = empty_shells(inv)
        if shells:
            print(f"  empty shells: {shells}")
            results["empty_shells"] = shells
        else:
            print("  empty shells: none")
            results["empty_shells"] = []

        results["inventory"] = inv
    except Exception as e:
        print(f"  inventory probe failed: {e}")
        results["inventory_error"] = str(e)

    # Write JSON results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"volume_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to: {results_file}")

    # Exit
    print("=" * 70)
    if fail_count > 0 or results.get("empty_shells"):
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
