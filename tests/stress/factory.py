"""
Surgery seed-set factory for stress testing.

Generates realistic fluoroscopy DICOM sets with deterministic UIDs from entropy
sources, pixel-data variance per seed, and overlap-algebra helpers for testing
dedup / duplicate detection scenarios.
"""
from __future__ import annotations

import hashlib
from pathlib import Path

import numpy as np


def synth_fluoro_frame(
    seed: int,
    rows: int,
    cols: int,
    dtype=np.uint16,
) -> np.ndarray:
    """
    Generate a deterministic synthetic fluoroscopy frame.

    Composites realistic X-ray anatomy:
    - Dark vignetting background (10-20% intensity)
    - Bright circular collimation field (~85% of frame)
    - 2-3 bright elliptical "bone" shapes (femur-like with cortical edges)
    - 1-2 thin bright guide-wire lines crossing the field
    - Smooth low-frequency illumination gradient + Gaussian blur
    - Poisson-like noise scaled to local intensity

    All elements parameterized deterministically from numpy.random.default_rng(seed).

    Parameters
    ----------
    seed : int
        Random seed for deterministic generation.
    rows, cols : int
        Frame dimensions.
    dtype : numpy dtype
        Output dtype (uint8 or uint16); defaults to uint16.

    Returns
    -------
    np.ndarray
        Shape (rows, cols), dtype as specified.
    """
    rng = np.random.default_rng(seed)

    # Normalize coordinates to [0, 1]
    max_intensity = np.iinfo(dtype).max
    mid_r, mid_c = rows / 2.0, cols / 2.0

    # 1. Dark background with vignette
    background = np.full((rows, cols), max_intensity * 0.15, dtype=np.float64)

    # Vignette: dark edges via distance-based falloff
    y, x = np.ogrid[:rows, :cols]
    dist = np.sqrt((y - mid_r) ** 2 + (x - mid_c) ** 2)
    max_dist = np.sqrt(mid_r**2 + mid_c**2)
    vignette = 1.0 - 0.6 * (dist / max_dist) ** 1.5
    vignette = np.clip(vignette, 0.1, 1.0)
    background = background * vignette

    # 2. Bright circular collimation field (~85% coverage)
    collim_radius = 0.42 * min(mid_r, mid_c)
    collim_mask = dist <= collim_radius
    collim_transition = np.clip((collim_radius - dist) / 10, 0, 1)
    collimation = np.zeros((rows, cols), dtype=np.float64)
    collimation[collim_mask] = max_intensity * 0.70
    # Soft edge via transition
    collimation = collimation * collim_transition

    # 3. Elliptical "bone" shapes (2-3 per seed)
    bones = np.zeros((rows, cols), dtype=np.float64)
    n_bones = rng.integers(2, 4)
    for i in range(n_bones):
        # Random position within collimation field
        bone_center_r = mid_r + rng.uniform(-0.25 * mid_r, 0.25 * mid_r)
        bone_center_c = mid_c + rng.uniform(-0.25 * mid_c, 0.25 * mid_c)

        # Ellipse parameters
        semi_a = rng.uniform(0.08 * min(rows, cols), 0.15 * min(rows, cols))
        semi_b = rng.uniform(0.03 * min(rows, cols), 0.08 * min(rows, cols))
        angle = rng.uniform(0, 180)

        # Draw ellipse
        cos_a = np.cos(np.deg2rad(angle))
        sin_a = np.sin(np.deg2rad(angle))
        dy = y - bone_center_r
        dx = x - bone_center_c
        rotated_y = dy * cos_a + dx * sin_a
        rotated_x = -dy * sin_a + dx * cos_a
        ellipse_mask = (rotated_y**2 / semi_a**2 + rotated_x**2 / semi_b**2) <= 1.0

        # Bright interior
        bone_interior = np.zeros((rows, cols), dtype=np.float64)
        bone_interior[ellipse_mask] = max_intensity * 0.65

        # Thick bright cortical edge
        cortical_ring_width = 2.0
        ellipse_outer = (
            (rotated_y**2 / (semi_a + cortical_ring_width) ** 2 +
             rotated_x**2 / (semi_b + cortical_ring_width) ** 2) <= 1.0
        )
        cortical = np.zeros((rows, cols), dtype=np.float64)
        cortical[ellipse_outer & ~ellipse_mask] = max_intensity * 0.85

        bones += bone_interior + cortical

    bones = np.clip(bones, 0, max_intensity)

    # 4. Guide-wire lines (1-2 thin bright polylines)
    wires = np.zeros((rows, cols), dtype=np.float64)
    n_wires = rng.integers(1, 3)
    for i in range(n_wires):
        # Random start/end positions
        r0 = rng.uniform(0.2 * rows, 0.8 * rows)
        c0 = rng.uniform(0.2 * cols, 0.8 * cols)
        r1 = rng.uniform(0.2 * rows, 0.8 * rows)
        c1 = rng.uniform(0.2 * cols, 0.8 * cols)

        # Draw thin line via distance to line segment
        t = np.maximum(
            0,
            np.minimum(
                1,
                ((y - r0) * (r1 - r0) + (x - c0) * (c1 - c0))
                / (((r1 - r0) ** 2 + (c1 - c0) ** 2) + 1e-6),
            ),
        )
        line_x = c0 + t * (c1 - c0)
        line_y = r0 + t * (r1 - r0)
        line_dist = np.sqrt((x - line_x) ** 2 + (y - line_y) ** 2)
        line_width = 1.5
        wire_mask = line_dist <= line_width
        wire_intensity = np.maximum(0, 1.0 - line_dist / line_width)
        wires[wire_mask] = np.maximum(
            wires[wire_mask], max_intensity * 0.90 * wire_intensity[wire_mask]
        )

    # 5. Composite layers
    frame = background + collimation + bones + wires
    frame = np.clip(frame, 0, max_intensity)

    # 6. Smooth illumination gradient (low-frequency)
    gradient_strength = rng.uniform(0.05, 0.15)
    gradient_angle = rng.uniform(0, 360)
    grad_cos = np.cos(np.deg2rad(gradient_angle))
    grad_sin = np.sin(np.deg2rad(gradient_angle))
    gradient = 1.0 + gradient_strength * (
        (y - mid_r) * grad_cos + (x - mid_c) * grad_sin
    ) / max(mid_r, mid_c)
    gradient = np.clip(gradient, 0.85, 1.15)
    frame = frame * gradient
    frame = np.clip(frame, 0, max_intensity)

    # 7. Gaussian blur (smooth low-frequency detail)
    try:
        from scipy.ndimage import gaussian_filter

        sigma = rng.uniform(1.5, 3.0)
        frame = gaussian_filter(frame, sigma=sigma)
    except ImportError:
        # Fallback: cheap box blur with numpy
        kernel_size = 3
        kernel = np.ones((kernel_size, kernel_size)) / (kernel_size**2)
        frame = np.convolve(
            frame.ravel(), kernel.ravel(), mode="same"
        ).reshape(frame.shape)

    frame = np.clip(frame, 0, max_intensity)

    # 8. Poisson-like noise (scaled to local intensity)
    noise_strength = rng.uniform(0.02, 0.08)
    poisson_noise = rng.poisson(max_intensity * noise_strength, size=(rows, cols))
    poisson_noise = poisson_noise.astype(np.float64)
    frame = frame + poisson_noise
    frame = np.clip(frame, 0, max_intensity)

    # 9. Normalize to dtype range and convert
    frame = (frame / max_intensity * max_intensity).astype(dtype)

    return frame


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

        # Regenerate PixelData with realistic fluoroscopy synthesis
        # Keep dtype/bits consistent with synthetic_data conventions (uint16)
        arr = synth_fluoro_frame(seed, rows=rows, cols=cols, dtype=np.uint16)
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
