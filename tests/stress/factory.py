"""
Surgery seed-set factory for stress testing.

Generates realistic fluoroscopy DICOM sets with deterministic UIDs from entropy
sources, pixel-data variance per seed, and overlap-algebra helpers for testing
dedup / duplicate detection scenarios.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Set

import numpy as np


def make_surgery(
    uid: str,
    seeds: set[int],
    dest_dir: Path,
    rows: int = 64,
    cols: int = 64,
) -> Path:
    """
    Create dest_dir/<uid>/ with one .dcm per seed named frame_<seed:04d>.dcm.

    Each file: make_phi_dicom_dataset(rows, cols, seed=seed) +
    InstanceNumber=str(i+1) (i = position in sorted(seeds)),
    StudyDate='20240115', StudyTime='080000',
    ONE shared StudyInstanceUID (deterministic from uid),
    ONE shared SeriesInstanceUID (deterministic from uid,'series'),
    per-instance SOPInstanceUID (deterministic from uid, str(seed)),
    NumberOfStudyRelatedInstances=len(seeds),
    Modality='RF', BodyPartExamined='HIP'.

    Distinct pixel content per seed (numpy RNG seeded by seed, rows/cols match).

    Parameters
    ----------
    uid : str
        Surgery UID (entropy source for deterministic StudyInstanceUID generation).
    seeds : set[int]
        Set of seed integers, one DICOM per seed.
    dest_dir : Path
        Parent directory; surgery subdir created at dest_dir/<uid>/.
    rows, cols : int
        Pixel dimensions (default 64x64).

    Returns
    -------
    Path
        The created surgery subdirectory (dest_dir/<uid>).
    """
    from tests.synthetic_data import make_phi_dicom_dataset
    import pydicom

    dest_dir = Path(dest_dir)
    surgery_dir = dest_dir / uid
    surgery_dir.mkdir(parents=True, exist_ok=True)

    # Deterministic UIDs from entropy sources
    study_uid = pydicom.uid.generate_uid(entropy_srcs=[uid])
    series_uid = pydicom.uid.generate_uid(entropy_srcs=[uid, "series"])

    # Sort seeds for deterministic InstanceNumber assignment
    sorted_seeds = sorted(seeds)

    for i, seed in enumerate(sorted_seeds):
        ds = make_phi_dicom_dataset(rows=rows, cols=cols, seed=seed)

        # Realistic fluoroscopy tags
        ds.Modality = "RF"
        ds.BodyPartExamined = "HIP"

        # Study/Series/Instance metadata
        ds.InstanceNumber = str(i + 1)
        ds.StudyDate = "20240115"
        ds.StudyTime = "080000"
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid
        ds.SOPInstanceUID = pydicom.uid.generate_uid(entropy_srcs=[uid, str(seed)])
        ds.NumberOfStudyRelatedInstances = len(seeds)

        # Regenerate PixelData with seed-specific RNG
        # Keep dtype/bits consistent with synthetic_data conventions (uint16)
        rng = np.random.default_rng(seed)
        arr = rng.integers(0, 4096, size=(rows, cols), dtype=np.uint16)
        ds.Rows = rows
        ds.Columns = cols
        ds.PixelData = arr.tobytes()

        frame_name = f"frame_{seed:04d}.dcm"
        frame_path = surgery_dir / frame_name
        ds.save_as(str(frame_path))

    return surgery_dir


def surgery_pixel_hashes(dir_path: Path) -> dict[str, str]:
    """
    Compute sha256 of PixelData for each .dcm file in dir.

    Parameters
    ----------
    dir_path : Path
        Directory containing .dcm files.

    Returns
    -------
    dict[str, str]
        Mapping {filename: sha256_hex}.
    """
    import pydicom

    hashes = {}
    for dcm_file in sorted(dir_path.glob("*.dcm")):
        ds = pydicom.dcmread(str(dcm_file))
        pixel_bytes = bytes(ds.PixelData)
        sha256 = hashlib.sha256(pixel_bytes).hexdigest()
        hashes[dcm_file.name] = sha256
    return hashes


def assert_distinct_content(dir_path: Path) -> None:
    """
    Precondition check: sha256 of PixelData of every .dcm in dir is pairwise distinct.

    Raises AssertionError if any collisions found, listing them.

    Parameters
    ----------
    dir_path : Path
        Directory containing .dcm files.

    Raises
    ------
    AssertionError
        If any two files have identical PixelData.
    """
    hashes = surgery_pixel_hashes(dir_path)

    # Invert: hash -> [filenames]
    hash_to_files = {}
    for fname, h in hashes.items():
        if h not in hash_to_files:
            hash_to_files[h] = []
        hash_to_files[h].append(fname)

    # Find collisions
    collisions = {h: files for h, files in hash_to_files.items() if len(files) > 1}
    if collisions:
        msg = "PixelData hash collisions found:\n"
        for h, files in collisions.items():
            msg += f"  {h}: {files}\n"
        raise AssertionError(msg)


def overlap_cases(base: set[int]) -> dict[str, set[int]]:
    """
    Seed-set algebra for overlap test scenarios.

    Maps case names to seed sets for testing dedup and duplicate-detection
    scenarios (#32 Q4 design).

    Parameters
    ----------
    base : set[int]
        The baseline set of seeds.

    Returns
    -------
    dict[str, set[int]]
        Mapping:
        - 'exact': identical to base
        - 'subset': strict subset of base (removed some)
        - 'superset': base | extras (added some)
        - 'partial': ~50% of base | new extras (half overlap, half new)
        - 'disjoint': all new seeds (no overlap)
    """
    base_list = sorted(base)
    n = len(base_list)

    # Strict subset: remove last element
    subset = set(base_list[: max(1, n - 1)])

    # Superset: add new seeds outside base range
    superset = base | {max(base) + i + 1 for i in range(3)}

    # Partial: keep first ~50%, add new ones
    partial = set(base_list[: (n + 1) // 2]) | {max(base) + i + 1 for i in range(n // 2)}

    # Disjoint: all-new seeds (offset from max(base))
    disjoint = {max(base) + i + 100 for i in range(n)}

    return {
        "exact": base.copy(),
        "subset": subset,
        "superset": superset,
        "partial": partial,
        "disjoint": disjoint,
    }
