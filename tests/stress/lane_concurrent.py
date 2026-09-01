"""
Concurrent stress lane — publish W surgeries in parallel + config race test.

Usage:
    python -m tests.stress.lane_concurrent --workers 4 --frames 10 --project STRESS_CONC

Each worker does its own connect() then publish_surgery in parallel (multiprocessing).
Then all W workers simultaneously update config (user-seeding + push) to test
lost-update guard. Records per-worker exceptions, especially LostUpdateError.
JSON results to tests/stress/results/concurrent_<timestamp>.json.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool


# Ensure env vars set BEFORE importing src
os.environ.setdefault("XNAT_SERVER_URL", "http://localhost:8080")
os.environ.setdefault("XNAT_IDENTITY_SALT", "ab12")


def _worker_publish(args_tuple):
    """
    Multiprocessing worker: connect, publish one surgery, return result.

    Each worker must establish its own connection (pyxnat not shareable across processes).
    """
    (
        worker_id,
        url,
        project,
        user,
        pwd,
        uid,
        dicom_dir,
        frames,
    ) = args_tuple

    import os as _os
    _os.environ["XNAT_SERVER_URL"] = url
    _os.environ["XNAT_PROJECT_NAME"] = project
    _os.environ.setdefault("XNAT_IDENTITY_SALT", "ab12")

    from tests.stress.driver import connect, publish_surgery
    from tests.stress.factory import make_surgery

    result = {
        "worker_id": worker_id,
        "uid": uid,
        "ok": False,
        "error": None,
        "seconds": 0.0,
        "n_files": 0,
    }

    start = time.time()
    try:
        # Each worker establishes its own connection
        conn, cfg, login = connect(url, project, user, pwd, verbose=False)

        # Make surgery directory
        temp_dir = Path("/tmp") / f"stress_conc_{worker_id}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        surgery_dir = make_surgery(uid, seeds=set(range(1, frames + 1)), dest_dir=temp_dir)

        # Publish
        pub_result = publish_surgery(conn, cfg, login, surgery_dir, uid, verbose=False)
        result.update(pub_result)

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

    except Exception as e:
        import traceback
        result["error"] = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"

    result["seconds"] = time.time() - start
    return result


def _worker_config_race(args_tuple):
    """
    Multiprocessing worker: connect, then simultaneously update config.

    Tests lost-update guard by having all workers try to add a user simultaneously.
    """
    (worker_id, url, project, user, pwd) = args_tuple

    import os as _os
    _os.environ["XNAT_SERVER_URL"] = url
    _os.environ["XNAT_PROJECT_NAME"] = project
    _os.environ.setdefault("XNAT_IDENTITY_SALT", "ab12")

    from tests.stress.driver import connect

    result = {
        "worker_id": worker_id,
        "ok": False,
        "error": None,
        "exception_type": None,
    }

    try:
        conn, cfg, login = connect(url, project, user, pwd, verbose=False)

        # Attempt simultaneous user registration (uses LostUpdateError guard if present)
        test_user = f"RACE_USER_{worker_id}"
        is_registered = cfg.is_user_registered(test_user)

        if not is_registered:
            cfg.add_new_item("REGISTERED_USERS", test_user)
            cfg.push_to_xnat(verbose=False)

        result["ok"] = True

    except Exception as e:
        result["exception_type"] = type(e).__name__
        result["error"] = str(e)

    return result


def main():
    parser = argparse.ArgumentParser(
        description="Concurrent stress test for XNAT-Interact"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of concurrent workers",
    )
    parser.add_argument(
        "--frames",
        type=int,
        default=10,
        help="Frames per surgery",
    )
    parser.add_argument(
        "--project",
        type=str,
        default="STRESS_CONC",
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

    print("=" * 70)
    print("CONCURRENT STRESS TEST")
    print(f"  workers:  {args.workers}")
    print(f"  frames:   {args.frames}")
    print(f"  project:  {args.project}")
    print(f"  url:      {args.url}")
    print("=" * 70)

    # Prepare results directory
    results_dir = Path(__file__).parent / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    results = {
        "timestamp": datetime.now().isoformat(),
        "workers": args.workers,
        "frames": args.frames,
        "project": args.project,
        "phase1_publishes": [],
        "phase2_config_races": [],
    }

    # PHASE 1: Concurrent publishes
    print("\nPHASE 1: Concurrent surgery publishes")
    print("-" * 70)

    worker_args = []
    for w in range(args.workers):
        uid = f"STRESS_C_{w+1:04d}"
        worker_args.append(
            (
                w,
                args.url,
                args.project,
                args.user,
                args.password,
                uid,
                None,  # dicom_dir (created in worker)
                args.frames,
            )
        )

    with Pool(processes=args.workers) as pool:
        phase1_results = pool.map(_worker_publish, worker_args)

    for res in phase1_results:
        results["phase1_publishes"].append(res)
        status = "ok" if res["ok"] else "fail"
        print(
            f"  worker {res['worker_id']:2} {res['uid']:20} {status:4} "
            f"s={res['seconds']:6.2f}"
        )

    ok_count = sum(1 for r in phase1_results if r["ok"])
    print(f"\nPhase 1: {ok_count}/{args.workers} ok")

    # PHASE 2: Concurrent config updates (race test)
    print("\nPHASE 2: Concurrent config updates (lost-update race)")
    print("-" * 70)

    race_args = []
    for w in range(args.workers):
        race_args.append((w, args.url, args.project, args.user, args.password))

    with Pool(processes=args.workers) as pool:
        phase2_results = pool.map(_worker_config_race, race_args)

    for res in phase2_results:
        results["phase2_config_races"].append(res)
        status = "ok" if res["ok"] else "fail"
        exc_type = res.get("exception_type", "none")
        print(
            f"  worker {res['worker_id']:2} {status:4} exception={exc_type}"
        )

    race_ok_count = sum(1 for r in phase2_results if r["ok"])
    race_err_count = sum(
        1 for r in phase2_results if r.get("exception_type") == "LostUpdateError"
    )
    print(f"\nPhase 2: {race_ok_count}/{args.workers} ok, "
          f"{race_err_count} LostUpdateError")

    # Write JSON results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = results_dir / f"concurrent_{timestamp}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults written to: {results_file}")

    print("=" * 70)
    if ok_count < args.workers:
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
