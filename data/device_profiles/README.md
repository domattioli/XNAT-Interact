# Device Profiles for Burned-in PHI De-identification

This directory contains device-profile configurations for contrast-independent pixel de-identification of fluoroscopy and OR C-arm images.

## Schema

Each profile is a JSON file with the following structure:

```json
{
  "device_id": "string_identifier",
  "description": "optional description of the device and its typical PHI region",
  "boxes": [
    [x0, y0, x1, y1],
    [x0, y0, x1, y1]
  ],
  "tolerance": 8
}
```

### Fields

- **`device_id`** (required, string): A stable identifier used as the lookup key. The system derives this from DICOM tags:
  - `Manufacturer + ManufacturerModelName` (e.g., `"siemens_axiom_artis"`)
  - Or a single `Manufacturer` (e.g., `"siemens"`)
  - Keys should be lowercase with underscores instead of spaces.

- **`description`** (optional, string): Human-readable note on the device model and where burned-in text typically appears.

- **`boxes`** (required, list of [x0, y0, x1, y1]): Pixel bounding boxes where the device renders burned-in PHI.
  - Coordinates are integers in pixel space (0-indexed).
  - **(x0, y0)** is the top-left corner (inclusive).
  - **(x1, y1)** is the bottom-right corner (exclusive, following numpy/cv2 convention).
  - Multiple boxes can be specified (e.g., banner + corner regions).

- **`tolerance`** (optional, integer): Pixel tolerance threshold for profile/detector disagreement (see Feature 010 spec, FR-006). If the profile-masked region and the detector-masked region differ by more than this tolerance, the case is routed to quarantine for operator review. Typical value: 8 pixels.

## Workflow: Adding a New Device Profile

1. **Identify the device**: Extract `Manufacturer` and `ManufacturerModelName` from actual DICOM files (use `pydicom.dcmread(file).Manufacturer`, etc.).
2. **Create the key**: Combine them as `"{mfr}_{model}".lower().replace(" ", "_")`.
3. **Determine the burned-in region**: Inspect a sample image or consult the device manual:
   - Often a fixed banner at the top-left or bottom of the frame.
   - May include corner overlays, crosshairs, or technical readouts.
4. **Write the JSON**: Create `{device_id}.json` in this directory with the measured box(es).
5. **Test**: Write a synthetic fixture (or grab a real anonymized sample) with that device identity and verify the profile boxes cover the PHI region.

## Example Devices

- **Siemens AXIOM Artis**: Burned-in text typically in the top-left banner, ~50 pixels high.
- **GE Optima**: Similar top-left banner region.
- **Philips Azurion**: May include corner crosshairs in addition to banners.

## Starter Profiles

This repo ships with **2 starter profiles** (`siemens_axiom_artis.json`, `ge_optima.json`) as worked examples. They are keyed to synthetic test fixtures and cover the standard top-left banner region. Real device profiles are added operationally as new equipment is validated.

## Note on Accuracy

Profiles are **fixed masks**, not perfect detectors. If a device's firmware or overlay configuration changes (e.g., after a software update), the profile may drift. The system detects such drift via profile/detector disagreement (see Feature 010, FR-006) and routes the case to quarantine. The operator can then review the evidence, update the profile, or mark the case safe.
