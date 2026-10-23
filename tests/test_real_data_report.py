"""
tests/test_real_data_report.py -- scripts/real_data_report.py over synthetic twins.

Offline. Synthetic data only (tests/synthetic_twins.py). The report script is
what the operator runs on real PHI cases, so these tests pin the leak guards:
terminal output, the verdicts payload, --out placement and source folders.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

import pydicom
import pytest

from tests import synthetic_twins as tw

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "real_data_report.py"
TWIN_PROFILES = str(REPO / "tests" / "fixtures" / "device_profiles_twins")
OP_DATE, OP_TIME = "2031-04-05", "07:08"


def _load_script():
    import importlib.util

    spec = importlib.util.spec_from_file_location("real_data_report", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["real_data_report"] = mod
    spec.loader.exec_module(mod)
    return mod


rdr = _load_script()


def _tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        h.update(str(p.relative_to(root)).encode())
        if p.is_file():
            h.update(p.read_bytes())
    return h.hexdigest()


def _env():
    env = dict(os.environ)
    env["TMPDIR"] = os.environ.get("TMPDIR", "/private/tmp/claude/t")
    return env


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    root = tmp_path_factory.mktemp("rdr_cases")
    manifests = tw.make_all_twins(root)
    manifests["099"] = tw.make_ocr_only_case(root)
    intake = {Path(m["folder"]).name: dict(operation_date=OP_DATE, start_time=OP_TIME) for m in manifests.values()}
    before = _tree_hash(root)
    out = tmp_path_factory.mktemp("rdr_out") / "report.html"
    rdr.build_report(root, intake, out, profiles_dir=TWIN_PROFILES)
    return dict(root=root, manifests=manifests, intake=intake, out=out, html=out.read_text(encoding="utf-8"),
                before=before, after=_tree_hash(root))


def _embedded(html: str, ident: str):
    m = re.search(rf'<script id="{ident}" type="application/json">(.*?)</script>', html, re.S)
    assert m, ident
    return json.loads(m.group(1))


def _planted(manifests) -> set:
    out = set()
    for m in manifests.values():
        out |= {p["value"] for p in m["header_plants"]} if "header_plants" in m else set()
        out |= {p["text"] for p in m["pixel_plants"]}
        out |= set(m.get("invented_dates", []))
        out |= {f["path"] for f in m["files"]}
        out.add(Path(m["folder"]).name)
    out |= {OP_DATE, OP_DATE.replace("-", ""), OP_TIME, OP_TIME.replace(":", "") + "00"}
    return {s for s in out if s}


def test_report_builds_with_phi_banner(built):
    assert built["out"].is_file()
    assert "Contains PHI - treat like the source files." in built["html"]
    data = _embedded(built["html"], "report-data")
    assert {c["name"] for c in data["cases"]} == {"twin_015", "twin_086", "twin_089", "twin_099"}
    assert {c["verdict"] for c in data["cases"]} <= {"CLEAN", "REDACT", "QUARANTINE"}
    assert data["missing_intake"] == 0


def test_unreadable_file_is_listed_with_its_message(built):
    data = _embedded(built["html"], "report-data")
    case = next(c for c in data["cases"] if c["name"] == "twin_015")
    assert case["load_errors"] and "acquisition date" in case["load_errors"][0]["message"]
    assert any(f["status"] == "not_loaded" for f in case["files"])


def test_html_has_no_external_urls(built):
    html = built["html"]
    assert "://" not in html
    assert not re.search(r"https?:|<link\b|<script[^>]+\bsrc=|url\(|@import", html)


def test_toggle_data_present_for_every_file(built):
    data = _embedded(built["html"], "report-data")
    oks = [f for c in data["cases"] for f in c["files"] if f["status"] == "ok"]
    assert len(oks) >= 7
    for c in data["cases"]:
        for f in c["files"]:
            if f["status"] != "ok":
                continue
            assert f["frames_orig"] and f["header"], f["rel"]
            assert len(f["frames_orig"]) == min(f["n_frames"], rdr.MAX_FRAMES)
            if c["verdict"] == "REDACT":
                assert f["frames_masked"] and len(f["frames_masked"]) == len(f["frames_orig"])
                assert f["boxes"]
            statuses = {r[0]: r[4] for r in f["header"]}
            assert statuses["0010,0010"] in ("changed", "removed")  # PatientName


def test_masked_image_equals_what_upload_would_send(built, tmp_path):
    from src.xnat_experiment_data import _redact_dicom_zip

    data = _embedded(built["html"], "report-data")
    compared = 0
    for c in data["cases"]:
        if c["verdict"] != "REDACT":
            continue
        for f in c["files"]:
            if f["status"] != "ok":
                continue
            src = built["root"] / c["name"] / f["rel"]
            zpath = tmp_path / f"{c['name']}_{compared}.zip"
            with zipfile.ZipFile(zpath, "w") as zf:
                zf.write(src, "x.dcm")
            _redact_dicom_zip(zpath, [tuple(b) for b in c["applied_boxes"]])
            with zipfile.ZipFile(zpath) as zf:
                ref = pydicom.dcmread(io.BytesIO(zf.read("x.dcm")))
            expect = rdr.thumbnails(pydicom.dcmread(str(src)), ref)
            assert f["frames_masked"] == expect["frames_masked"], (c["name"], f["rel"])
            assert f["frames_orig"] != f["frames_masked"]
            compared += 1
    assert compared >= 5


def test_stdout_has_only_path_and_exit_code(built, tmp_path):
    out = tmp_path / "o" / "report.html"
    intake = tmp_path / "intake.json"
    intake.write_text(json.dumps(built["intake"]))
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--cases", str(built["root"]), "--intake", str(intake), "--out", str(out),
         "--profiles-dir", TWIN_PROFILES], capture_output=True, text=True, env=_env(), cwd=str(tmp_path))
    assert proc.returncode == 0
    lines = proc.stdout.strip().splitlines()
    assert lines == [f"report: {out}", "exit code: 0"]
    for s in _planted(built["manifests"]):
        assert s not in proc.stdout and s not in proc.stderr, s
    assert proc.stderr == ""


def test_stdout_on_failure_names_only_the_class(built, tmp_path):
    intake = tmp_path / "intake.json"
    intake.write_text("{not json")
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--cases", str(built["root"]), "--intake", str(intake),
         "--out", str(tmp_path / "r.html")], capture_output=True, text=True, env=_env(), cwd=str(tmp_path))
    assert proc.returncode == 2
    assert re.fullmatch(r"failed: \w+\nexit code: 2\n", proc.stdout)


def _mirror_payload(html: str) -> dict:
    """Python mirror of the page's buildVerdictsPayload, driven by the embedded allowed-keys list."""
    allowed = _embedded(html, "allowed-keys")
    data = _embedded(html, "report-data")
    entries = []
    for c in data["cases"]:
        for f in c["files"]:
            e = dict(f, file_id=f["id"])  # everything the page holds for a file, so extra keys must be filtered out
            e["ticks"] = {"text_missed": True, "over_masked": False, "header_problem": True, "note": "x"}
            entries.append(e)
    files = []
    for e in entries:
        out = {k: e[k] for k in allowed["file"] if k in e}
        out["ticks"] = {k: e["ticks"][k] for k in allowed["ticks"] if k in e["ticks"]}
        files.append(out)
    return {allowed["top"][0]: files}


def test_verdicts_payload_has_no_values_names_or_dates(built):
    html = built["html"]
    payload = _mirror_payload(html)
    allowed = _embedded(html, "allowed-keys")
    assert set(payload) == set(allowed["top"])
    data = _embedded(html, "report-data")
    salt = data["salt"]
    rels = {(c["name"], f["rel"]) for c in data["cases"] for f in c["files"]}
    assert payload["files"]
    for item in payload["files"]:
        assert set(item) <= set(allowed["file"]) and set(item["ticks"]) <= set(allowed["ticks"])
    assert {i["file_id"] for i in payload["files"]} == {rdr.file_id(salt, n, r) for (n, r) in rels}
    text = json.dumps(payload)
    for s in _planted(built["manifests"]):
        assert s not in text, s
    for (n, r) in rels:
        assert r not in text and n not in text


def test_allowed_keys_are_the_documented_set(built):
    allowed = _embedded(built["html"], "allowed-keys")
    assert set(allowed["file"]) == {"file_id", "ticks", "verdict", "reason", "tiers"}
    assert set(allowed["ticks"]) == {"text_missed", "over_masked", "header_problem"}


def test_download_handler_builds_entries_from_allowed_keys_only(built):
    allowed = _embedded(built["html"], "allowed-keys")
    m = re.search(r"entries\.push\(\{(.*?)\}\);", built["html"], re.S)
    assert m
    keys = re.findall(r"(\w+):", m.group(1))
    assert keys and set(keys) <= set(allowed["file"])


@pytest.mark.skipif(shutil.which("node") is None, reason="node not installed")
def test_js_builder_filters_extra_keys(built):
    html = built["html"]
    m = re.search(r"function pick\(.*?window\.buildVerdictsPayload=buildVerdictsPayload;", html, re.S)
    js = m.group(0).replace("window.buildVerdictsPayload=buildVerdictsPayload;", "") + """
var allowed=%s;
var out=buildVerdictsPayload([{file_id:'a',verdict:'CLEAN',reason:'r',tiers:['t'],name:'LEAK',rel:'LEAK',
  ticks:{text_missed:true,note:'LEAK'}}],allowed);
console.log(JSON.stringify(out));""" % json.dumps(_embedded(html, "allowed-keys"))
    proc = subprocess.run(["node", "-e", js], capture_output=True, text=True)
    assert proc.returncode == 0, proc.stderr
    assert "LEAK" not in proc.stdout
    assert json.loads(proc.stdout)["files"][0]["ticks"] == {"text_missed": True}


def test_out_inside_repo_is_refused(built, tmp_path):
    target = REPO / "zz_report_refusal_probe" / "report.html"
    proc = subprocess.run(
        [sys.executable, str(SCRIPT), "--cases", str(built["root"]), "--out", str(target)],
        capture_output=True, text=True, env=_env(), cwd=str(tmp_path))
    created = target.exists()
    if created:  # clean up only what a broken guard would have written
        target.unlink()
        target.parent.rmdir()
    assert proc.returncode != 0
    assert "refused" in proc.stdout
    assert not created
    with pytest.raises(PermissionError):
        rdr.build_report(built["root"], None, REPO / "zz_probe" / "r.html")
    assert not (REPO / "zz_probe").exists()


def test_out_inside_cases_folder_is_refused(built):
    with pytest.raises(PermissionError):
        rdr.build_report(built["root"], None, built["root"] / "twin_015" / "r.html")


def test_source_folders_unchanged(built):
    assert built["before"] == built["after"]


def test_missing_intake_shows_banner(tmp_path):
    cases = tmp_path / "cases"
    tw.make_twin_case(cases, "086")
    out = tmp_path / "o" / "r.html"
    rdr.build_report(cases, None, out, profiles_dir=TWIN_PROFILES)
    html = out.read_text(encoding="utf-8")
    assert _embedded(html, "report-data")["missing_intake"] == 1
    assert "date check is meaningless" in html
