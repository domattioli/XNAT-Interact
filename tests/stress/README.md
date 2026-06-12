# Stress Test Factory

Stress-test helpers for XNAT-Interact upload robustness.

## Modules

- **factory.py**: `make_surgery(uid, seeds, dest_dir)` generates realistic fluoroscopy DICOM sets with deterministic study/series UIDs. Includes `overlap_cases()` algebra for dedup test scenarios (#32 Q4) and `assert_distinct_content()` precondition checks.
- **intake.py**: `make_intake()` constructs `ORDataIntakeForm` instances from synthetic DICOM directories, mirroring roundtrip-push pattern.
- **malformed.py**: DICOM generators testing robustness: truncated, missing InstanceNumber, 3-channel RGB, duplicate tags, huge surgery (1000+ instances), plain-text fake.

## Overlap Algebra

`overlap_cases(base: set[int]) → dict[str, set[int]]` — test dedup scenarios:
- `'exact'`: identical to base
- `'subset'`: strict subset (removed some)
- `'superset'`: base + extras (added some)
- `'partial'`: ~50% overlap + new seeds
- `'disjoint'`: all-new seeds (no overlap)

## Usage Lanes

- **Volume**: `make_surgery()` with large seed sets, batched intake forms.
- **Concurrency**: multiple surgery dirs, parallel intake/publish.
- **Malformed**: `truncated_dicom()`, `no_instance_number()`, `huge_surgery(1005)`, etc. — test error recovery.

## Environment

Set before importing `src.*`:
```
XNAT_SERVER_URL=http://localhost:8080
XNAT_PROJECT_NAME=<project>
XNAT_USERNAME / XNAT_PASSWORD
```
