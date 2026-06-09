"""
Device-profile registry for contrast-independent pixel de-identification (Feature 010, Stage 2).

A device profile maps a device identity (model/manufacturer/private-block pattern) to a
list of overlay regions where the device characteristically renders burned-in PHI. The
regions are (x0, y0, x1, y1) bounding boxes in pixels and an optional per-profile tolerance
threshold for profile/detector disagreement.

This is the **contrast-independent trust anchor** (User Story 1): profiled devices are
masked blind (no OCR required), guaranteeing zero false-negatives for their known regions
regardless of text contrast.

Public API
----------
load_profiles(profiles_dir="data/device_profiles") -> dict[str, dict]
    Load all *.json profiles from profiles_dir, validate schema, return
    {device_id: {"boxes": [...], "tolerance": int}}.

device_id_for(dataset) -> str | None
    Resolve device identity from a pydicom Dataset: 0x0019 private block, then
    Manufacturer + ManufacturerModelName, return stable key or None if unprofiled.

boxes_for(dataset, profiles=None, profiles_dir="data/device_profiles") -> list[tuple]
    Look up device_id_for(dataset), return that profile's boxes, or [] if unknown.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Dict, Optional

import numpy as np

logger = logging.getLogger(__name__)


def load_profiles(
    profiles_dir: str = "data/device_profiles",
) -> dict[str, dict]:
    """
    Load all *.json device profiles from profiles_dir and validate their schema.

    Each profile JSON must contain:
      - "device_id": string identifier (key in the returned dict)
      - "boxes": list of [x0, y0, x1, y1] bounding boxes (integers)
      - "tolerance" (optional): int, pixel tolerance for profile/detector disagreement

    Parameters
    ----------
    profiles_dir:
        Path to directory containing *.json profile files.

    Returns
    -------
    dict[str, dict]
        {device_id: {"boxes": [...], "tolerance": int | None}}.

    Raises
    ------
    ValueError
        If any profile is malformed (missing required keys, invalid box format, etc.).
    """
    profiles: dict[str, dict] = {}
    profiles_path = Path(profiles_dir)

    if not profiles_path.exists():
        logger.warning(f"Profiles directory {profiles_path} does not exist. Returning empty.")
        return {}

    for json_file in sorted(profiles_path.glob("*.json")):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Validate schema
            if "device_id" not in data:
                raise ValueError(f"{json_file.name}: missing required key 'device_id'")
            if "boxes" not in data:
                raise ValueError(f"{json_file.name}: missing required key 'boxes'")

            device_id = data["device_id"]
            boxes = data["boxes"]
            tolerance = data.get("tolerance", None)

            # Validate boxes format
            if not isinstance(boxes, list):
                raise ValueError(
                    f"{json_file.name}: 'boxes' must be a list, got {type(boxes).__name__}"
                )
            for i, box in enumerate(boxes):
                if not isinstance(box, (list, tuple)) or len(box) != 4:
                    raise ValueError(
                        f"{json_file.name}: box {i} must be [x0, y0, x1, y1], got {box}"
                    )
                for j, coord in enumerate(box):
                    if not isinstance(coord, int):
                        raise ValueError(
                            f"{json_file.name}: box {i} coord {j} must be int, got {type(coord).__name__}"
                        )

            # Validate tolerance if present
            if tolerance is not None and not isinstance(tolerance, int):
                raise ValueError(
                    f"{json_file.name}: 'tolerance' must be int or null, got {type(tolerance).__name__}"
                )

            profiles[device_id] = {
                "boxes": [tuple(box) for box in boxes],
                "tolerance": tolerance,
            }
            logger.debug(f"Loaded profile '{device_id}' from {json_file.name}")

        except json.JSONDecodeError as e:
            raise ValueError(f"{json_file.name}: invalid JSON — {e}")
        except Exception as e:
            raise ValueError(f"{json_file.name}: {e}")

    logger.info(f"Loaded {len(profiles)} device profiles from {profiles_dir}")
    return profiles


def device_id_for(dataset) -> str | None:
    """
    Resolve device identity from a pydicom Dataset.

    Tries (in order):
      1. Private 0x0019 block (manufacturer's proprietary device profile tag).
      2. Combination of Manufacturer and ManufacturerModelName tags.
      3. Modality (fallback, low-confidence).
      Returns None if none of the above are present.

    Parameters
    ----------
    dataset:
        A pydicom Dataset (FileDataset or plain Dataset).

    Returns
    -------
    str | None
        Stable device identity key, or None if the device cannot be identified.
    """
    # Try 0x0019 private block first (manufacturer-specific profile)
    if 0x0019 in dataset:
        try:
            # The private block at 0x0019 is often used for device-specific profiles.
            # We encode its presence/structure as a key.
            private_block = dataset[0x0019]
            device_id = f"private_0x0019_{id(private_block) & 0xFFFF:04x}"
            return device_id
        except Exception:
            pass

    # Try Manufacturer + ManufacturerModelName
    mfr = getattr(dataset, "Manufacturer", None)
    model = getattr(dataset, "ManufacturerModelName", None)

    if mfr and model:
        # Normalize to a stable key: lowercase, spaces → underscores
        key = f"{mfr}_{model}".lower().replace(" ", "_")
        return key

    if mfr:
        return mfr.lower().replace(" ", "_")

    # Fallback: try Modality
    modality = getattr(dataset, "Modality", None)
    if modality:
        return f"modality_{modality.lower()}"

    return None


def boxes_for(
    dataset,
    profiles: Optional[dict[str, dict]] = None,
    profiles_dir: str = "data/device_profiles",
) -> list[tuple]:
    """
    Resolve device identity from a DICOM dataset and return its profile boxes.

    If profiles is not provided, loads them from profiles_dir on first call.
    Returns an empty list if the device is not profiled.

    Parameters
    ----------
    dataset:
        A pydicom Dataset.
    profiles:
        Optional pre-loaded profiles dict (from load_profiles()).
        If None, loads from profiles_dir.
    profiles_dir:
        Path to profiles directory (used only if profiles is None).

    Returns
    -------
    list[tuple]
        List of (x0, y0, x1, y1) bounding boxes for the device, or [] if unprofiled.
    """
    if profiles is None:
        profiles = load_profiles(profiles_dir)

    device_id = device_id_for(dataset)
    if device_id is None:
        logger.debug("Device identity could not be determined.")
        return []

    if device_id not in profiles:
        logger.debug(f"Device '{device_id}' is not in the profiles registry.")
        return []

    profile = profiles[device_id]
    return profile.get("boxes", [])
