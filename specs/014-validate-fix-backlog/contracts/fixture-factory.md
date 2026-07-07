# Contract: Seed-Set Fixture Factory (014)

Extends `tests/synthetic_data.py`. All functions pure/deterministic given a seed; no PHI, no network.

```python
make_byte_duplicates(seed) -> tuple[SyntheticDicom, SyntheticDicom]   # identical bytes, distinct names/UIDs
make_near_duplicates(seed) -> tuple[SyntheticDicom, SyntheticDicom]   # adjacent-frame lookalikes, distinct bytes
make_multi_scan_surgery(seed, n_scans=3) -> SyntheticSurgery
make_big_series(seed, n=1001) -> SyntheticSeries                      # lazy instance generation
make_malicious_archive(seed) -> Path                                  # entries: "../escape.txt", absolute path
make_concurrent_sessions(seed) -> tuple[MetaTablesHandle, MetaTablesHandle]
make_invalid_values(seed) -> SyntheticDicom                           # NaN/None in validity-gated fields
```

Guarantees:
- Byte-duplicate pairs are `bytes(a) == bytes(b)`; near-duplicate pairs are `bytes(a) != bytes(b)` with perceptual distance below the advisory threshold.
- `make_big_series` spans the 999→1000 filename boundary without materializing pixel data.
- `make_malicious_archive` output extracts to zero files outside the target dir when C2 fix is active.
