"""
Lane: P5 annotation round-trip stress test against live XNAT server.

Purpose: Prove src/annotations upload/download against real server
(assessor path was fixed this session: gateway resolves assessor label→accession ID).

Steps:
  1. driver.connect() to fresh project; publish 1 surgery.
  2. Build a small AnnotationSet for that experiment via generic importer.
  3. Upload via io_xnat.upload_annotation_set (real gateway).
  4. Download back via io_xnat.download_annotation_set; compare round-trip.
  5. Upload v2 (modified annotation); verify manifest/versioning behavior.
  6. Print verdict table + write tests/stress/results/annotations_<ts>.json.

Exit 0 if round-trip byte-equal; nonzero if any step fails.
Report: verdict table verbatim + any src findings with file:line.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.annotations.model import Annotation, AnnotationSet
from src.annotations.io_xnat import upload_annotation_set, download_annotation_set
from src.annotations.importers.generic import from_mask_array
from src.annotations.validate import validate_annotator_id
from tests.stress import driver, factory


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

XNAT_URL = "http://localhost:8080"
XNAT_USER = "admin"
XNAT_PASSWORD = "admin"

PROJECT_ENV = os.environ.get("XNAT_PROJECT_NAME", "ANNOT_LANE")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sparse_mask(seed: int = 0, rows: int = 64, cols: int = 64) -> np.ndarray:
    """Generate a sparse binary mask for testing."""
    rng = np.random.default_rng(seed)
    mask = np.zeros((rows, cols), dtype=np.uint8)
    n_fg = max(1, int(rows * cols * 0.10))  # 10% foreground
    flat_idx = rng.choice(rows * cols, size=n_fg, replace=False)
    mask.ravel()[flat_idx] = 1
    return mask


def _make_annotation_set_from_surgery(
    experiment_id: str, annotators: List[str]
) -> AnnotationSet:
    """Build a small AnnotationSet using generic importer."""
    aset = AnnotationSet(image_ref=experiment_id)

    for i, annotator_id in enumerate(annotators):
        # Validate annotator_id (PHI guard)
        validate_annotator_id(annotator_id)

        # Create mask via generic importer
        mask = _make_sparse_mask(seed=i, rows=64, cols=64)
        ann = from_mask_array(
            mask,
            annotator_id=annotator_id,
            tool="test_stress",
            annotation_type="binary_segmentation",
            created_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            version=1,
        )
        aset.add(ann)

    return aset


# ---------------------------------------------------------------------------
# Step 1: Connect & Publish Surgery
# ---------------------------------------------------------------------------

def step_publish_surgery(
    project: str, verbose: bool = False
) -> tuple[bool, Optional[str], Optional[str], Dict[str, Any]]:
    """
    Connect to XNAT, bootstrap config, publish one surgery.

    Returns (ok, experiment_id, error_str, result_dict)
    """
    try:
        # Set identity salt for surgeon pseudonymization
        if "XNAT_IDENTITY_SALT" not in os.environ:
            os.environ["XNAT_IDENTITY_SALT"] = "aabbccddeeff" * 5 + "aabb"

        if verbose:
            print("[Step 1] Connecting to XNAT...")
        conn, cfg, login = driver.connect(
            XNAT_URL, project, XNAT_USER, XNAT_PASSWORD, verbose=verbose
        )

        if verbose:
            print("  Connected. Creating fresh DICOM directory...")

        # Create a single surgery with DICOM frames
        with tempfile.TemporaryDirectory() as tmpdir:
            dicom_path = Path(tmpdir) / "dicoms"
            dicom_path.mkdir()

            # Create 1 surgery with 3 DICOM frames using factory
            # Use timestamp + random to make each run unique
            import uuid
            rng_seed = int(time.time() * 1000000) % 100000000
            surgery_uid = f"ANNOT_SURGERY_{rng_seed}"
            # Use diverse seed values to get different pixel content each run
            seeds = {rng_seed, rng_seed + 1000, rng_seed + 2000}  # 3 frames
            surgery_dir = factory.make_surgery(
                uid=surgery_uid,
                seeds=seeds,
                dest_dir=dicom_path,
                rows=64,
                cols=64,
            )

            if verbose:
                print(f"  Publishing surgery: {surgery_uid}")
                print(f"  DICOM dir: {surgery_dir}")

            pub_result = driver.publish_surgery(
                conn,
                cfg,
                login,
                surgery_dir,
                surgery_uid,
                verbose=verbose,
            )

            if not pub_result["ok"]:
                return False, None, pub_result.get("error"), pub_result

            if verbose:
                print("  Surgery published. Querying experiments...")

            # Find the experiment ID that was just created
            # assessments contain experiments (via /assessors endpoint)
            try:
                import requests

                proj = conn.server.select.project(project)
                subjects_list = list(proj.subjects())
                if verbose:
                    print(f"  pyxnat found {len(subjects_list)} subjects")

                # Get the last subject (freshly created)
                if subjects_list:
                    subj = subjects_list[-1]
                    subj_id = subj.id()
                    if verbose:
                        print(f"  Last subject: {subj_id}")

                    # Query the assessors endpoint for this subject
                    assessors_url = f"{XNAT_URL}/data/projects/{project}/subjects/{subj_id}/assessors?format=json"
                    resp = requests.get(
                        assessors_url,
                        auth=(XNAT_USER, XNAT_PASSWORD),
                        timeout=15,
                    )
                    resp.raise_for_status()
                    assessors_data = resp.json()

                    items = assessors_data.get("items", [])
                    if verbose:
                        print(f"  Assessors query returned {len(items)} items")

                    if items:
                        # Get experiments from the first assessor item
                        assessor = items[0]
                        exp_children = [
                            c
                            for c in assessor.get("children", [])
                            if c.get("field") == "experiments/experiment"
                        ]
                        if exp_children and exp_children[0].get("items"):
                            exp_list = exp_children[0]["items"]
                            if exp_list:
                                # Get the last experiment
                                exp = exp_list[-1]
                                exp_id = exp["data_fields"]["ID"]
                                if verbose:
                                    print(f"  Experiment ID: {exp_id}")

                                experiment_ref = f"/projects/{project}/subjects/{subj_id}/experiments/{exp_id}"
                                if verbose:
                                    print(f"  Found experiment: {experiment_ref}")
                                return (
                                    True,
                                    experiment_ref,
                                    None,
                                    pub_result,
                                )

                if verbose:
                    print("  No experiments found")

            except Exception as e:
                if verbose:
                    print(f"  Assessor query exception: {e}")
                    traceback.print_exc()
                return False, None, f"Assessor query failed: {e}", pub_result

    except Exception as e:
        return False, None, f"Step 1 exception: {type(e).__name__}: {e}\n{traceback.format_exc()}", {}

    return False, None, "Could not determine experiment ID", {}


# ---------------------------------------------------------------------------
# Step 2: Build AnnotationSet
# ---------------------------------------------------------------------------

def step_build_annotation_set(experiment_id: str) -> tuple[bool, Optional[AnnotationSet], Optional[str]]:
    """Build a small AnnotationSet with 2 annotators."""
    try:
        annotators = ["worker_A1", "worker_B2"]
        aset = _make_annotation_set_from_surgery(experiment_id, annotators)
        if len(aset.annotations) == len(annotators):
            return True, aset, None
        else:
            return False, None, f"Expected {len(annotators)} annotations, got {len(aset.annotations)}"
    except Exception as e:
        return False, None, f"Step 2 exception: {type(e).__name__}: {e}"


# ---------------------------------------------------------------------------
# Step 3: Upload AnnotationSet
# ---------------------------------------------------------------------------

def step_upload_v1(
    server: Any, experiment_id: str, aset: AnnotationSet
) -> tuple[bool, Optional[str], List[str]]:
    """Upload v1 annotation set to XNAT."""
    try:
        result = upload_annotation_set(
            server,
            experiment_id,
            aset,
            project_name=PROJECT_ENV,
            resource_label="ANNOTATIONS",
        )

        if not result.ok:
            friendly_msg = (
                result.friendly.title if result.friendly else "Unknown error"
            )
            return False, friendly_msg, []

        return True, None, result.files_written

    except Exception as e:
        return False, f"Step 3 exception: {type(e).__name__}: {e}", []


# ---------------------------------------------------------------------------
# Step 4: Download & Compare Round-Trip
# ---------------------------------------------------------------------------

def step_download_and_compare(
    server: Any, experiment_id: str, original_aset: AnnotationSet
) -> tuple[bool, Optional[str], Optional[AnnotationSet]]:
    """Download annotation set and compare round-trip equality."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            dest = Path(tmpdir) / "downloaded"
            result = download_annotation_set(
                server,
                experiment_id,
                dest,
                project_name=PROJECT_ENV,
                resource_label="ANNOTATIONS",
            )

            if not result.ok:
                friendly_msg = (
                    result.friendly.title if result.friendly else "Unknown error"
                )
                return False, friendly_msg, None

            rebuilt = result.annotation_set
            if rebuilt is None:
                return False, "Downloaded annotation_set is None", None

            # Compare blob bytes and manifest fields
            if len(rebuilt.annotations) != len(original_aset.annotations):
                return (
                    False,
                    f"Annotation count mismatch: {len(rebuilt.annotations)} vs {len(original_aset.annotations)}",
                    rebuilt,
                )

            for i, (orig, rebuilt_ann) in enumerate(
                zip(original_aset.annotations, rebuilt.annotations)
            ):
                # Check manifest fields
                if orig.annotator_id != rebuilt_ann.annotator_id:
                    return (
                        False,
                        f"Annotation {i}: annotator_id mismatch",
                        rebuilt,
                    )
                if orig.tool != rebuilt_ann.tool:
                    return False, f"Annotation {i}: tool mismatch", rebuilt
                if orig.annotation_type != rebuilt_ann.annotation_type:
                    return (
                        False,
                        f"Annotation {i}: annotation_type mismatch",
                        rebuilt,
                    )
                if orig.version != rebuilt_ann.version:
                    return False, f"Annotation {i}: version mismatch", rebuilt

                # Check payload byte equality
                if orig.payload is None or rebuilt_ann.payload is None:
                    return (
                        False,
                        f"Annotation {i}: payload is None",
                        rebuilt,
                    )

                if not np.array_equal(orig.payload, rebuilt_ann.payload):
                    return (
                        False,
                        f"Annotation {i}: payload bytes not equal",
                        rebuilt,
                    )

            return True, None, rebuilt

    except Exception as e:
        return (
            False,
            f"Step 4 exception: {type(e).__name__}: {e}\n{traceback.format_exc()}",
            None,
        )


# ---------------------------------------------------------------------------
# Step 5: Upload v2 (Modified)
# ---------------------------------------------------------------------------

def step_upload_v2(
    server: Any, experiment_id: str, original_aset: AnnotationSet
) -> tuple[bool, Optional[str], List[str]]:
    """
    Upload a second version (v2) of the annotation set.
    Modify the first annotation's payload to test versioning.
    """
    try:
        # Build v2: same annotators, bumped versions, modified payloads
        aset_v2 = AnnotationSet(image_ref=experiment_id)

        for i, orig_ann in enumerate(original_aset.annotations):
            # Modify mask by inverting some bits
            new_mask = orig_ann.payload.copy()
            new_mask[0:10, 0:10] = 1 - new_mask[0:10, 0:10]  # Flip a region

            ann_v2 = Annotation(
                annotator_id=orig_ann.annotator_id,
                tool=orig_ann.tool,
                annotation_type=orig_ann.annotation_type,
                created_at=datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
                version=2,  # Bumped version
                derived=False,
                payload=new_mask,
            )
            aset_v2.add(ann_v2)

        result = upload_annotation_set(
            server,
            experiment_id,
            aset_v2,
            project_name=PROJECT_ENV,
            resource_label="ANNOTATIONS",
        )

        if not result.ok:
            friendly_msg = (
                result.friendly.title if result.friendly else "Unknown error"
            )
            return False, friendly_msg, []

        return True, None, result.files_written

    except Exception as e:
        return (
            False,
            f"Step 5 exception: {type(e).__name__}: {e}",
            [],
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="P5 annotation round-trip stress test against live XNAT"
    )
    parser.add_argument(
        "--project",
        default=PROJECT_ENV,
        help=f"XNAT project name (default: {PROJECT_ENV})",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Verbose output"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).parent / "results",
        help="Output directory for results JSON",
    )

    args = parser.parse_args()
    project = args.project
    verbose = args.verbose
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Verdict table: track all steps
    verdict: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project": project,
        "steps": {},
        "m6_observation": None,
        "overall_ok": False,
        "errors": [],
    }

    start_time = time.time()

    # Step 1: Connect & Publish
    if verbose:
        print("\n=== STEP 1: Connect & Publish Surgery ===")
    ok1, exp_id, err1, pub_result = step_publish_surgery(project, verbose=verbose)
    verdict["steps"]["1_publish_surgery"] = {
        "ok": ok1,
        "experiment_id": exp_id,
        "error": err1,
    }
    if not ok1:
        verdict["errors"].append(
            f"STEP 1 FAILED: {err1}"
        )
        if verbose:
            print(f"  FAIL: {err1}")
    else:
        if verbose:
            print(f"  OK: {exp_id}")

    if not ok1:
        print("\n=== VERDICT ===")
        print(json.dumps(verdict, indent=2))
        return 1

    # Step 2: Build AnnotationSet
    if verbose:
        print("\n=== STEP 2: Build AnnotationSet ===")
    ok2, aset, err2 = step_build_annotation_set(exp_id)
    verdict["steps"]["2_build_annotationset"] = {"ok": ok2, "error": err2}
    if not ok2:
        verdict["errors"].append(f"STEP 2 FAILED: {err2}")
        if verbose:
            print(f"  FAIL: {err2}")
    else:
        if verbose:
            print(f"  OK: {len(aset.annotations)} annotations built")

    if not ok2:
        print("\n=== VERDICT ===")
        print(json.dumps(verdict, indent=2))
        return 1

    # Establish server connection for upload/download
    if verbose:
        print("\n=== Connecting to XNAT for annotation ops ===")
    try:
        from src.utilities import XNATLogin, XNATConnection

        login = XNATLogin(
            input_info={"URL": XNAT_URL, "USERNAME": XNAT_USER, "PASSWORD": XNAT_PASSWORD},
            verbose=False,
        )
        conn = XNATConnection(
            login_info=login, stay_connected=True, verbose=False
        )
        gateway = conn.gateway  # Use gateway, not server, for annotations
        if verbose:
            print("  Connected.")
    except Exception as e:
        err = f"Failed to establish XNAT connection: {e}"
        verdict["errors"].append(err)
        print(f"\n{err}")
        print("\n=== VERDICT ===")
        print(json.dumps(verdict, indent=2))
        return 1

    # Step 3: Upload v1
    if verbose:
        print("\n=== STEP 3: Upload v1 AnnotationSet ===")
    ok3, err3, files3 = step_upload_v1(gateway, exp_id, aset)
    verdict["steps"]["3_upload_v1"] = {
        "ok": ok3,
        "files_written": files3,
        "error": err3,
    }
    if not ok3:
        verdict["errors"].append(f"STEP 3 FAILED: {err3}")
        if verbose:
            print(f"  FAIL: {err3}")
    else:
        if verbose:
            print(f"  OK: {len(files3)} files written")

    if not ok3:
        print("\n=== VERDICT ===")
        print(json.dumps(verdict, indent=2))
        return 1

    # Step 4: Download & Compare
    if verbose:
        print("\n=== STEP 4: Download & Compare Round-Trip ===")
    ok4, err4, rebuilt = step_download_and_compare(gateway, exp_id, aset)
    verdict["steps"]["4_download_compare"] = {"ok": ok4, "error": err4}
    if not ok4:
        verdict["errors"].append(f"STEP 4 FAILED: {err4}")
        if verbose:
            print(f"  FAIL: {err4}")
    else:
        if verbose:
            print(f"  OK: Round-trip byte-equal, {len(rebuilt.annotations)} annotations")

    if not ok4:
        print("\n=== VERDICT ===")
        print(json.dumps(verdict, indent=2))
        return 1

    # Step 5: Upload v2
    if verbose:
        print("\n=== STEP 5: Upload v2 (Modified) ===")
    ok5, err5, files5 = step_upload_v2(gateway, exp_id, aset)
    verdict["steps"]["5_upload_v2"] = {
        "ok": ok5,
        "files_written": files5,
        "error": err5,
    }
    if not ok5:
        verdict["errors"].append(f"STEP 5 FAILED: {err5}")
        if verbose:
            print(f"  FAIL: {err5}")
    else:
        if verbose:
            print(f"  OK: {len(files5)} files written (v2)")

    # M6 Observation: Check manifest versioning behavior
    # According to #33 M6 claim: manifest orphans prior versions
    # Download manifest and check if both v1 and v2 are referenced
    if ok5:
        if verbose:
            print("\n=== M6 OBSERVATION: Manifest Versioning ===")
        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                dest = Path(tmpdir) / "manifest_check"
                result = download_annotation_set(
                    gateway,
                    exp_id,
                    dest,
                    project_name=project,
                    resource_label="ANNOTATIONS",
                )
                if result.ok:
                    manifest_path = dest / "manifest.json"
                    if manifest_path.exists():
                        manifest = json.loads(
                            manifest_path.read_bytes().decode("utf-8")
                        )
                        annotations = manifest.get("annotations", [])

                        # Check for v1 and v2 in manifest entries
                        v1_count = sum(
                            1
                            for e in annotations
                            if e.get("version") == 1
                        )
                        v2_count = sum(
                            1
                            for e in annotations
                            if e.get("version") == 2
                        )

                        verdict["m6_observation"] = {
                            "v1_in_manifest": v1_count,
                            "v2_in_manifest": v2_count,
                            "total_annotations": len(annotations),
                            "finding": (
                                f"Manifest contains {v1_count} v1 and {v2_count} v2 annotations. "
                                f"If v2 orphans v1 (v1_count=0), M6 claim verified."
                            ),
                        }
                        if verbose:
                            print(
                                f"  v1 entries: {v1_count}, v2 entries: {v2_count}"
                            )
                            print(
                                f"  Finding: {verdict['m6_observation']['finding']}"
                            )
        except Exception as e:
            verdict["m6_observation"] = {
                "error": str(e),
            }

    # Final verdict
    verdict["overall_ok"] = all(
        v.get("ok", False) for v in verdict["steps"].values()
    )
    verdict["elapsed_seconds"] = time.time() - start_time

    # Write results JSON
    results_file = (
        output_dir
        / f"annotations_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    )
    results_file.write_text(json.dumps(verdict, indent=2), encoding="utf-8")

    if verbose:
        print(f"\n  Results written to: {results_file}")

    # Print verdict table
    print("\n" + "=" * 70)
    print("VERDICT TABLE")
    print("=" * 70)
    for step, result in verdict["steps"].items():
        status = "PASS" if result.get("ok") else "FAIL"
        print(f"{step:30s} [{status:4s}]")
        if not result.get("ok") and result.get("error"):
            print(f"  Error: {result['error']}")

    if verdict["m6_observation"]:
        print("\nM6 OBSERVATION (Manifest Versioning):")
        if "finding" in verdict["m6_observation"]:
            print(f"  {verdict['m6_observation']['finding']}")
        else:
            print(f"  {json.dumps(verdict['m6_observation'], indent=2)}")

    print("=" * 70)
    print(f"Overall: {'PASS' if verdict['overall_ok'] else 'FAIL'}")
    print(f"Elapsed: {verdict['elapsed_seconds']:.2f} seconds")
    print(f"Results: {results_file}")
    print("=" * 70)

    return 0 if verdict["overall_ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
