"""
US3 / RADIOFLUORO_2026: edge cases against the live XNAT container (spec 014).

Seven RF files: a byte-identical same-UID pair, a same-UID different-pixel
pair, a corrupted header, a truncated pixel file and a valid file. Production
refuses the whole folder when one file is unreadable (research SPEC-ISSUE-1),
so the harness probes each file with the production reader, records a
disposition for each, and publishes the readable files. Tests share one
module-scoped state dict and run in file order.
"""
from __future__ import annotations

import dataclasses
import json
import shutil

import pydicom
import pytest

from tests.integration.live_xnat import helpers as H
from tests.integration.live_xnat.cases import build_case

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

CASE = "RADIOFLUORO_2026"


@pytest.fixture(scope="module")
def rfl(live_xnat_server, tmp_path_factory):
    work = tmp_path_factory.mktemp("radiofluoro_2026")
    case = build_case(CASE, work / "src")
    state = {"case": case, "work": work, "dispositions": {}, "counts": {}, "scan_layout": {},
             "absent_tags": {}, "probe": {}, "hashes": {}}
    state["full"] = H.publish_case_session(live_xnat_server, case, "rf", work / "full")
    H.record_case_subject(live_xnat_server, CASE, state["full"].uid)
    return state


def test_whole_folder_refused_naming_corrupted_file(live_xnat_server, rfl):
    full = rfl["full"]
    rfl["full_error"] = full.production_exception
    assert full.production_exception is not None, "production accepted a folder holding a corrupted file"
    assert full.production_exception.startswith("GatewayError"), full.production_exception
    assert "rfl_corrupt_header" in full.production_exception, full.production_exception


def test_probe_each_file_and_publish_readable(live_xnat_server, rfl):
    from src.xnat_experiment_data import ORDataIntakeForm
    from src.xnat_scan_data import SourceDicomDeIdentified

    case = rfl["case"]
    config = live_xnat_server["config_factory"]()
    form = ORDataIntakeForm(config=config, validated_login=live_xnat_server["login"],
                            input_data=case.intake_series("rf"), verbose=False, write_file=False)
    readable = []
    for p in case.files:
        try:
            obj = SourceDicomDeIdentified(dcm_ffn=str(p), config=config, intake_form=form)
            ok = bool(obj.is_valid)
            rfl["probe"][p.name] = "ok" if ok else "is_valid False"
            rfl["hashes"][p.name] = str(obj.image.hash_str).upper()
            if ok:
                readable.append(p)
        except Exception as exc:  # noqa: BLE001
            rfl["probe"][p.name] = f"{type(exc).__name__}: {str(exc).splitlines()[0][:200]}"
    for p in case.files:
        if rfl["probe"][p.name] != "ok":
            rfl["dispositions"][p.name] = "rejected" if p.name == case.raw["corrupted_file"] else "failed_recoverable"

    sub = rfl["work"] / "readable" / "rf"
    sub.mkdir(parents=True)
    for p in readable:
        shutil.copy(p, sub / p.name)
    part = dataclasses.replace(case, rf_dir=sub, rf_files=[sub / p.name for p in readable],
                               files=[sub / p.name for p in readable])
    res = H.publish_case_session(live_xnat_server, part, "rf", rfl["work"] / "readable")
    H.record_case_subject(live_xnat_server, CASE, res.uid)
    rfl["rf"] = res
    for p in readable:
        rfl["dispositions"][p.name] = "published" if p.name in res.source_to_new else "rejected"
    server = H.list_scan_files(live_xnat_server, res) if res.scan_uri else []
    rfl["counts"].update(processed=len(server),
                         rejected=sum(v == "rejected" for v in rfl["dispositions"].values()),
                         recovered=0)
    err = "; ".join(x for x in (rfl.get("full_error"), res.production_exception, res.publish_exception) if x)
    H.set_phase(rfl, "ingestion", "partial" if res.production_exception is None else "failure",
                files=[r["Name"] for r in server], links=[res.files_uri] if res.files_uri else [], error=err or None)
    assert rfl["probe"][case.raw["corrupted_file"]] != "ok"
    assert res.production_exception is None, res.production_exception
    assert res.publish_exception is None, res.publish_exception
    assert len(server) == len(res.published_files)
    assert sorted(rfl["dispositions"]) == sorted(p.name for p in case.files), "a file has no disposition"
    assert rfl["dispositions"][case.raw["corrupted_file"]] == "rejected"


def _downloads(live, rfl):
    if "dl" not in rfl:
        rfl["dl"] = H.download_scan_files(live, rfl["rf"], rfl["work"] / "download")
    return rfl["dl"]


def test_no_copy_of_truncated_file(live_xnat_server, rfl):
    case, res = rfl["case"], rfl["rf"]
    truncated = case.raw["truncated_file"]
    dl = _downloads(live_xnat_server, rfl)
    new_fn = res.source_to_new.get(truncated)
    assert new_fn is None or new_fn not in dl, f"truncated file published as {new_fn} (probe: {rfl['probe'][truncated]})"
    src_uid = pydicom.dcmread(str(case.rf_dir / "rfl_valid.dcm"), stop_before_pixels=True).StudyInstanceUID
    assert src_uid  # fixture sanity


def test_duplicate_pair_hash_stored_once(live_xnat_server, rfl):
    case = rfl["case"]
    a, b = case.raw["duplicate_pair"]
    assert rfl["hashes"][a] == rfl["hashes"][b], "fixture duplicate pair hashes differ"
    hashes, _ = H.server_identity_rows(live_xnat_server)
    n = int((hashes["NAME"].astype(str).str.upper() == rfl["hashes"][a]).sum())
    rfl["counts"]["collisions_resolved"] = 1 if n == 1 else 0
    assert n == 1, f"duplicate hash appears {n} times in IMAGE_HASHES"


def test_image_dedup_confirms_duplicate(live_xnat_server, rfl):
    """T036 second half: production image_dedup over the production adapter."""
    from src.services.dedup import image_dedup
    from src.xnat_experiment_data import _ConfigTablesRegistryAdapter

    a, _ = rfl["case"].raw["duplicate_pair"]
    adapter = _ConfigTablesRegistryAdapter(live_xnat_server["config_factory"]())
    sop = pydicom.dcmread(str(rfl["case"].rf_dir / a), stop_before_pixels=True).SOPInstanceUID
    # SPEC-ISSUE-18: the adapter has no image_exists(); image_dedup calls it.
    result = image_dedup(rfl["hashes"][a], sop, adapter)
    assert result.is_duplicate


def test_collision_pair_kept_distinct(live_xnat_server, rfl):
    case, res = rfl["case"], rfl["rf"]
    a, b = case.raw["collision_pair"]
    assert rfl["hashes"][a] != rfl["hashes"][b]
    hashes, _ = H.server_identity_rows(live_xnat_server)
    stored = set(hashes["NAME"].astype(str).str.upper())
    dl = _downloads(live_xnat_server, rfl)
    missing = [n for n in (a, b) if res.source_to_new.get(n) not in dl]
    rfl["counts"]["collisions_recorded"] = int(rfl["hashes"][a] in stored and rfl["hashes"][b] in stored and not missing)
    assert rfl["hashes"][a] in stored and rfl["hashes"][b] in stored
    assert not missing, f"collision files not downloadable: {missing}"
    assert rfl["counts"]["collisions_recorded"] == 1


def test_no_shared_hash_rows_in_project(live_xnat_server, rfl):
    hashes, _ = H.server_identity_rows(live_xnat_server)
    names = hashes["NAME"].astype(str).str.upper()
    dupes = sorted(set(names[names.duplicated()]))
    assert not dupes, f"{len(dupes)} hashes stored more than once"


def test_headers_checksums_and_summary(live_xnat_server, rfl, outcome_writer):
    from src.utilities import UIDandMetaInfo

    res = rfl["rf"]
    redacted = UIDandMetaInfo().redacted_string
    dl = _downloads(live_xnat_server, rfl)
    bad = [f"{n}:{e.keyword}" for n, p in dl.items()
           for e in pydicom.dcmread(str(p), stop_before_pixels=True).iterall()
           if e.VR == "PN" and e.value not in (None, "") and str(e.value) != redacted]
    H.set_phase(rfl, "deidentification", "success" if not bad else "failure", files=list(dl),
                error="; ".join(bad[:5]) or None)
    H.set_phase(rfl, "annotation", "partial", error="not exercised: US3 has no annotation step")
    H.set_phase(rfl, "segmentation", "partial", error="not exercised: US3 has no segmentation step")
    H.set_phase(rfl, "staple", "partial", error="not exercised: US3 has no staple step")
    passed, failed = 0, []
    for name, path in dl.items():
        local = res.published_files.get(name)
        if local is not None and H.sha256(path) == H.sha256(local):
            passed += 1
        else:
            failed.append(name)
    rfl["counts"]["checksum_pass"], rfl["counts"]["checksum_fail"] = passed, len(failed)
    rfl["scan_layout"].update(H.scan_layout(live_xnat_server, res))
    H.set_phase(rfl, "integrity_validation", "success" if passed and not failed else "failure",
                error=f"mismatch: {failed[:5]}" if failed else None)
    path = H.write_case_summary(rfl, outcome_writer, CASE)
    H.validate_summary(path)
    assert not bad, bad
    assert passed > 0, "zero files compared"
    assert not failed, failed
