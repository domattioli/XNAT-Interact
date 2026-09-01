"""
Malformed DICOM generators for stress testing robustness.

Each function writes a defective DICOM into a directory and returns its Path,
used to test handling of truncation, missing tags, invalid pixel formats,
and large instance counts.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np


def truncated_dicom(dir_path: Path) -> Path:
    """
    Write a valid DICOM file then truncate to first 200 bytes.

    Returns the path to the truncated (broken) file.

    Parameters
    ----------
    dir_path : Path
        Directory to write into.

    Returns
    -------
    Path
        Path to the truncated .dcm file.
    """
    from tests.synthetic_data import make_synthetic_dicom

    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    # Create a valid DICOM first
    valid_path = dir_path / "temp_valid.dcm"
    make_synthetic_dicom(valid_path, rows=16, cols=16, seed=0)

    # Truncate it
    truncated_path = dir_path / "truncated.dcm"
    with open(valid_path, "rb") as f:
        data = f.read(200)
    truncated_path.write_bytes(data)

    valid_path.unlink(missing_ok=True)
    return truncated_path


def no_instance_number(dir_path: Path, seed: int = 1) -> Path:
    """
    Write a DICOM with InstanceNumber deleted (if present).

    Parameters
    ----------
    dir_path : Path
        Directory to write into.
    seed : int
        Seed for pixel data generation.

    Returns
    -------
    Path
        Path to the .dcm file without InstanceNumber.
    """
    from tests.synthetic_data import make_phi_dicom_dataset

    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    ds = make_phi_dicom_dataset(rows=16, cols=16, seed=seed)

    # Delete InstanceNumber if present
    if hasattr(ds, "InstanceNumber"):
        delattr(ds, "InstanceNumber")

    path = dir_path / "no_instance_number.dcm"
    ds.save_as(str(path))
    return path


def three_channel(dir_path: Path, seed: int = 2) -> Path:
    """
    Write a DICOM with RGB (3-channel) PixelData instead of grayscale.

    Sets SamplesPerPixel=3, PhotometricInterpretation='RGB',
    PixelData shaped (rows, cols, 3).

    Parameters
    ----------
    dir_path : Path
        Directory to write into.
    seed : int
        Seed for pixel data generation.

    Returns
    -------
    Path
        Path to the 3-channel .dcm file.
    """
    from tests.synthetic_data import make_phi_dicom_dataset

    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    ds = make_phi_dicom_dataset(rows=16, cols=16, seed=seed)

    # Convert to 3-channel RGB
    rows, cols = 16, 16
    rng = np.random.default_rng(seed)
    arr = rng.integers(0, 256, size=(rows, cols, 3), dtype=np.uint8)

    ds.Rows = rows
    ds.Columns = cols
    ds.SamplesPerPixel = 3
    ds.PhotometricInterpretation = "RGB"
    ds.BitsAllocated = 8
    ds.BitsStored = 8
    ds.HighBit = 7
    ds.PixelData = arr.tobytes()

    path = dir_path / "three_channel.dcm"
    ds.save_as(str(path))
    return path


def dup_private_tag(dir_path: Path, seed: int = 3) -> Path:
    """
    Write a DICOM where a private tag already carries StudyTime and StudyInstanceUID.

    This triggers the H2 add_new collision scenario (tag already present).

    Parameters
    ----------
    dir_path : Path
        Directory to write into.
    seed : int
        Seed for pixel data generation.

    Returns
    -------
    Path
        Path to the .dcm file with duplicate private tags.
    """
    from tests.synthetic_data import make_phi_dicom_dataset
    import pydicom

    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    ds = make_phi_dicom_dataset(rows=16, cols=16, seed=seed)

    # Pre-populate StudyTime and StudyInstanceUID (will exist when add_new is called)
    ds.StudyTime = "080000"
    ds.StudyInstanceUID = pydicom.uid.generate_uid()

    path = dir_path / "dup_private_tag.dcm"
    ds.save_as(str(path))
    return path


def huge_surgery(dir_path: Path, n: int = 1005) -> Path:
    """
    Write n tiny (8x8) DICOMs with InstanceNumber 1..n.

    Tests M1 off-by-one boundary (≥1000 instance count).
    Uses small pixel arrays to keep it fast.

    Parameters
    ----------
    dir_path : Path
        Directory to write into.
    n : int
        Number of DICOMs to generate (default 1005, > 1000 boundary).

    Returns
    -------
    Path
        The parent directory containing all n .dcm files.
    """
    from tests.synthetic_data import make_phi_dicom_dataset
    import pydicom

    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    study_uid = pydicom.uid.generate_uid(entropy_srcs=["huge_surgery"])
    series_uid = pydicom.uid.generate_uid(entropy_srcs=["huge_surgery", "series"])

    for i in range(n):
        ds = make_phi_dicom_dataset(rows=8, cols=8, seed=i % 100)

        ds.InstanceNumber = str(i + 1)
        ds.StudyDate = "20240115"
        ds.StudyTime = "080000"
        ds.StudyInstanceUID = study_uid
        ds.SeriesInstanceUID = series_uid
        ds.SOPInstanceUID = pydicom.uid.generate_uid(
            entropy_srcs=["huge_surgery", str(i)]
        )
        ds.NumberOfStudyRelatedInstances = n

        path = dir_path / f"frame_{i:05d}.dcm"
        ds.save_as(str(path))

    return dir_path


def not_a_dicom(dir_path: Path) -> Path:
    """
    Write a .dcm file containing plain text (not valid DICOM).

    Parameters
    ----------
    dir_path : Path
        Directory to write into.

    Returns
    -------
    Path
        Path to the fake .dcm file.
    """
    dir_path = Path(dir_path)
    dir_path.mkdir(parents=True, exist_ok=True)

    path = dir_path / "not_a_dicom.dcm"
    path.write_bytes(b"hello world this is not dicom")
    return path
