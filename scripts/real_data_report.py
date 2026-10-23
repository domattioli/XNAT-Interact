#!/usr/bin/env python3
"""
Build a self-contained HTML review report over folders of real imaging cases.

Usage:
    python scripts/real_data_report.py --cases <folder of case folders>
        [--intake intake.json] [--out ~/Downloads/xnat_real_report/report.html]
        [--profiles-dir DIR]

The operator runs this on real PHI cases. The pipeline is the production
upload pipeline stopped before upload: SourceRFSession intake (header de-id,
dates -> operation date, UID/time rewrite), automated_pixel_review (the
pixel gate), then the same masking function upload uses (_redact_dicom_zip)
on a zip written the way SourceRFSession.write() writes it. No XNAT, no
network. Source folders are only read.

The terminal shows only the report path and an exit code. The report itself
contains PHI (original pixels and header values) and is treated like the
source files. The verdicts file the report can download holds only a salted
file hash, tick boxes, verdict, reason and tiers.

--intake JSON: {"<case folder name>": {"operation_date": "YYYY-MM-DD", "start_time": "HH:MM"}}
Without it a fixed placeholder date is used and the report says the date check
is meaningless.

Exit status: 0 report written, 2 refused or failed.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import hashlib
import io
import json
import logging
import os
import secrets
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

PLACEHOLDER_DATE = "1970-01-01"
PLACEHOLDER_TIME = "00:00"
MAX_THUMB = 512
MAX_FRAMES = 24
DEFAULT_OUT = "~/Downloads/xnat_real_report/report.html"

# Keys the verdicts.json payload may contain. Emitted into the page as JSON so
# a test can check the page's own list against what the builder produces.
ALLOWED_KEYS = {
    "top": ["files"],
    "file": ["file_id", "ticks", "verdict", "reason", "tiers"],
    "ticks": ["text_missed", "over_masked", "header_problem"],
}

_DECISION_LABEL = {"CONFIRMED": "CLEAN", "REDACT": "REDACT", "QUARANTINE": "QUARANTINE"}


# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------

def file_id(salt: str, case_name: str, rel_path: str) -> str:
    """Salted, stable hash of a file's relative path (the only file identity in verdicts.json)."""
    return hashlib.sha256((salt + case_name + "/" + rel_path).encode("utf-8")).hexdigest()[:16]


def _inside(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _offline_config():
    """ConfigTables with no server behind it: empty IMAGE_HASHES and SUBJECTS tables."""
    import pandas as pd
    from src.utilities import ConfigTables

    ct = ConfigTables.__new__(ConfigTables)
    ct._login_info = SimpleNamespace(is_valid=True, validated_username="offline", validated_password="")
    ct._xnat_connection = SimpleNamespace(is_open=False, is_verified=False, server=None, gateway=None,
                                          xnat_project_name="OFFLINE", login_info=ct._login_info)
    ct._local_variables = SimpleNamespace(
        data_librarian=["offline"], config_fn="database_config.json", config_ffn=os.devnull,
        backup_fn="database_config-backup.json", xnat_config_folder_name="config",
        xnat_backups_folder_name="backups", xnat_project_name="OFFLINE", required_login_keys=[],
        default_meta_table_columns=["NAME", "UID", "DATE", "CREATED_BY"], tmp_data_dir=Path(tempfile.gettempdir()),
        cataloged_resources_ffn=os.devnull, template_img_dir=tempfile.gettempdir(), template_img=None,
        acceptable_img_dtypes=[], required_img_size_for_hashing=(224, 224), mturk_batch_col_names=[],
        redacted_string="REDACTED", required_batch_upload_columns={},
    )
    ct._uid = "OFFLINE"
    ct._tables = {"IMAGE_HASHES": pd.DataFrame(columns=["NAME"]), "SUBJECTS": pd.DataFrame(columns=["NAME"])}
    return ct


def _intake_stub(uid: str, folder: Path, saved_dir: Path, entry: dict) -> SimpleNamespace:
    saved_dir.mkdir(parents=True, exist_ok=True)
    saved = saved_dir / "intake.json"
    saved.write_text("{}")
    return SimpleNamespace(uid=uid, operation_date=entry["operation_date"], epic_start_time=entry["start_time"],
                           relevant_folder=folder, saved_ffn=saved, saved_ffn_str=str(saved))


def _case_uid(salt: str, case_name: str) -> str:
    return "2.25." + str(int(hashlib.sha256((salt + "uid" + case_name).encode()).hexdigest()[:24], 16))


# ----------------------------------------------------------------------------
# Header table and pixels
# ----------------------------------------------------------------------------

_BINARY_VR = {"OB", "OW", "OD", "OF", "OL", "OV", "UN"}


def _flatten(ds, prefix: str = "") -> Dict[str, tuple]:
    out: Dict[str, tuple] = {}
    for elem in ds:
        if elem.tag == 0x7FE00010:
            continue
        key = f"{prefix}{elem.tag.group:04X},{elem.tag.element:04X}"
        if elem.VR == "SQ":
            out[key] = (elem.name, f"<sequence of {len(elem.value)}>")
            for i, item in enumerate(elem.value):
                out.update(_flatten(item, f"{key}[{i}]/"))
            continue
        if elem.VR in _BINARY_VR:
            try:
                val = f"<binary {len(elem.value)} bytes>"
            except Exception:  # noqa: BLE001
                val = "<binary>"
        else:
            val = str(elem.value)
            if len(val) > 200:
                val = val[:200] + "..."
        out[key] = (elem.name, val)
    return out


def header_rows(orig, proc) -> List[list]:
    a, b = _flatten(orig), _flatten(proc)
    rows = []
    for key in sorted(set(a) | set(b)):
        if key in a and key in b:
            status = "same" if a[key][1] == b[key][1] else "changed"
        elif key in a:
            status = "removed"
        else:
            status = "added"
        rows.append([key, (a.get(key) or b.get(key))[0], a[key][1] if key in a else "", b[key][1] if key in b else "", status])
    return rows


def _to_gray(frame):
    import numpy as np

    a = np.asarray(frame, dtype=np.float64)
    if a.ndim == 3:
        a = a[..., :3].mean(axis=-1)
    return a


def _window(ds, gray_frames):
    import numpy as np

    try:
        wc = float(ds.WindowCenter[0] if hasattr(ds.WindowCenter, "__len__") and not isinstance(ds.WindowCenter, str) else ds.WindowCenter)
        ww = float(ds.WindowWidth[0] if hasattr(ds.WindowWidth, "__len__") and not isinstance(ds.WindowWidth, str) else ds.WindowWidth)
        if ww > 0:
            return wc - ww / 2.0, wc + ww / 2.0
    except Exception:  # noqa: BLE001
        pass
    allpx = np.concatenate([g.ravel()[:: max(1, g.size // 20000)] for g in gray_frames])
    lo, hi = np.percentile(allpx, [1, 99])
    if hi <= lo:
        lo, hi = float(allpx.min()), float(allpx.max()) + 1.0
    return float(lo), float(hi)


def _png_b64(gray, lo: float, hi: float) -> str:
    """Window/level to 8-bit and downscale. Display only: never feeds masking."""
    import numpy as np
    from PIL import Image

    u8 = np.clip((gray - lo) / max(hi - lo, 1e-9) * 255.0, 0, 255).astype(np.uint8)
    img = Image.fromarray(u8)
    img.thumbnail((MAX_THUMB, MAX_THUMB))
    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=True)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def _frames_of_ds(ds) -> list:
    arr = ds.pixel_array
    n = int(getattr(ds, "NumberOfFrames", 1) or 1)
    frames = list(arr) if n > 1 else [arr]
    return [_to_gray(f) for f in frames]


def _sample_idx(n: int) -> List[int]:
    if n <= MAX_FRAMES:
        return list(range(n))
    step = (n - 1) / (MAX_FRAMES - 1)
    return sorted({int(round(i * step)) for i in range(MAX_FRAMES)})


def thumbnails(orig_ds, masked_ds) -> dict:
    """Base64 PNG lists for original and masked frames, windowed alike from the original."""
    of = _frames_of_ds(orig_ds)
    idx = _sample_idx(len(of))
    lo, hi = _window(orig_ds, [of[i] for i in idx])
    orig = [_png_b64(of[i], lo, hi) for i in idx]
    masked = None
    if masked_ds is not None:
        mf = _frames_of_ds(masked_ds)
        masked = [_png_b64(mf[i], lo, hi) for i in idx if i < len(mf)]
    return dict(frames_orig=orig, frames_masked=masked, n_frames=len(of), sampled=len(idx) < len(of),
                rows=int(of[0].shape[0]), cols=int(of[0].shape[1]))


# ----------------------------------------------------------------------------
# Case processing (production code, stopped before upload)
# ----------------------------------------------------------------------------

def _ensure_identity_salt(report_salt: str) -> None:
    from src.services.identity import load_identity_salt

    try:
        load_identity_salt()
    except Exception:  # noqa: BLE001 - no librarian salt on this machine: use a per-run one for CaseDateHash
        os.environ["XNAT_IDENTITY_SALT"] = hashlib.sha256(report_salt.encode()).hexdigest()


def _candidates(case_dir: Path) -> List[Path]:
    return sorted(f for f in case_dir.rglob("*") if f.is_file() and f.suffix == ".dcm")


def _friendly_text(exc: BaseException) -> str:
    fr = getattr(exc, "friendly", None)
    if fr is not None:
        return f"{fr.title}: {fr.message}"
    return f"{type(exc).__name__}: " + str(exc).split("\n")[0]


def _load_session(case_dir: Path, entry: dict, uid: str, work: Path, config, profile_load_errors: list):
    """SourceRFSession over *case_dir*; on a load failure, over a copy that leaves out unreadable files."""
    from src.services.xnat_gateway import GatewayError
    from src.xnat_experiment_data import SourceRFSession
    from src.xnat_scan_data import SourceDicomDeIdentified

    try:
        intake = _intake_stub(uid, case_dir, work / "direct", entry)
        return SourceRFSession(intake, config), case_dir
    except GatewayError:
        pass
    # Find the unreadable files one by one, then rebuild the session without them.
    probe = _intake_stub(uid, case_dir, work / "probe", entry)
    view = work / "view"
    view.mkdir(parents=True, exist_ok=True)
    good = 0
    for ffn in _candidates(case_dir):
        rel = ffn.relative_to(case_dir)
        try:
            SourceDicomDeIdentified(dcm_ffn=ffn, config=config, intake_form=probe)
        except Exception as exc:  # noqa: BLE001
            profile_load_errors.append(dict(file=rel.as_posix(), message=_friendly_text(exc)))
            continue
        dest = view / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ffn, dest)
        good += 1
    if not good:
        return None, view
    intake = _intake_stub(uid, view, work / "rebuilt", entry)
    return SourceRFSession(intake, config), view


def _write_upload_zip(sess, dest_dir: Path) -> Path:
    """Write the session the way SourceRFSession.write() does: dcmwrite each Dataset, then zip the folder."""
    from pydicom import dcmwrite

    staging = dest_dir / "stage"
    staging.mkdir(parents=True, exist_ok=True)
    for idx, row in sess.df.iterrows():
        if row["IS_VALID"] is True or row["IS_VALID"] == True:  # noqa: E712
            dcmwrite(str(staging / str(row["NEW_FN"])), row["OBJECT"].metadata)
    return Path(shutil.make_archive(str(dest_dir / "dicom_files"), "zip", str(staging)))


def process_case(case_dir: Path, entry: dict, salt: str, profiles_dir: str, work: Path, config) -> dict:
    import pydicom
    from app.logic.upload import automated_pixel_review
    from src.xnat_experiment_data import (ReviewDecision, _operation_date_as_dicom_da, _redact_dicom_zip,
                                          _UPLOAD_DATE_KEYWORDS)

    name = case_dir.name
    uid = _case_uid(salt, name)
    case = dict(name=name, verdict="NO_IMAGES", reason="no DICOM images found", tiers=[], load_errors=[], files=[],
                skipped=0, date_ok=None)
    case["skipped"] = sum(1 for f in case_dir.rglob("*") if f.is_file() and f.suffix != ".dcm")

    # 1. Pixel gate on the source folder, exactly as prepare_and_upload calls it.
    decision, boxes, tiers, reason = None, [], [], ""
    sink: list = []
    try:
        auto = automated_pixel_review(case_dir, profiles_dir=profiles_dir, assessment_sink=sink)
        if auto is None:
            case["verdict"], case["reason"] = "NO_IMAGES", "no DICOM images found"
        else:
            decision, boxes = auto
            assessment = sink[0] if sink else None
            tiers = list(getattr(assessment, "tiers_fired", []) or [])
            reason = str(getattr(assessment, "reason", "") or "")
            case["verdict"] = _DECISION_LABEL.get(decision.name, "QUARANTINE")
            case["reason"], case["tiers"] = reason, tiers
            regions = [(int(x0), int(y0), int(x1) - int(x0), int(y1) - int(y0))
                       for x0, y0, x1, y1 in (getattr(assessment, "regions", []) or [])]
            case["gate_regions"] = regions
    except Exception as exc:  # noqa: BLE001 - fail closed, like upload
        case["verdict"], case["reason"] = "QUARANTINE", f"pixel gate failed ({type(exc).__name__})"
        decision, boxes = ReviewDecision.QUARANTINE, []
        case["gate_regions"] = []

    # 2. Header de-id, dates, UIDs, times: the production intake path.
    load_errors: list = []
    sess = None
    try:
        sess, view = _load_session(case_dir, entry, uid, work, config, load_errors)
    except Exception as exc:  # noqa: BLE001
        load_errors.append(dict(file="(whole case)", message=f"{type(exc).__name__}: " + _friendly_text(exc)))
        view = case_dir
    case["load_errors"] = load_errors

    # 3. Masking: the zip upload would send, through the function upload calls.
    masked_by_new_fn: Dict[str, Any] = {}
    if sess is not None and sess.is_valid and decision is not None:
        zpath = _write_upload_zip(sess, work)
        applied = decision == ReviewDecision.REDACT and bool(boxes)
        if decision == ReviewDecision.REDACT and not boxes:
            case["reason"] += " (upload would stop: no redaction areas)"
        if applied:
            _redact_dicom_zip(zpath, boxes)
        if decision != ReviewDecision.QUARANTINE and not (decision == ReviewDecision.REDACT and not boxes):
            with zipfile.ZipFile(zpath) as zf:
                for n in zf.namelist():
                    masked_by_new_fn[n] = pydicom.dcmread(io.BytesIO(zf.read(n)))
    case["applied_boxes"] = [list(b) for b in boxes] if decision == ReviewDecision.REDACT else []

    # 4. Per-file entries.
    seen_rel = set()
    if sess is not None:
        all_ffns = sess._all_dicom_ffns()
        date_checks = []
        for idx, row in sess.df.iterrows():
            ffn = Path(all_ffns[int(idx)])
            rel = ffn.relative_to(view).as_posix()
            seen_rel.add(rel)
            f = dict(id=file_id(salt, name, rel), name=ffn.name, rel=rel, status="ok", boxes=case["applied_boxes"],
                     gate_regions=case.get("gate_regions", []))
            obj = row["OBJECT"] if row["IS_VALID"] is True or row["IS_VALID"] == True else None  # noqa: E712
            if obj is None:
                f["status"] = "not_loaded"
                f["note"] = "file not accepted for upload by production rules"
                case["files"].append(f)
                continue
            try:
                orig = pydicom.dcmread(str(case_dir / rel))
                proc = obj.metadata
                f["header"] = header_rows(orig, proc)
                op_da = _operation_date_as_dicom_da(entry["operation_date"])
                date_checks.append(all(str(getattr(proc, k, "")) == op_da for k in _UPLOAD_DATE_KEYWORDS if k in proc))
            except Exception as exc:  # noqa: BLE001
                f["header"], f["note"] = [], f"header view unavailable ({type(exc).__name__})"
                orig = None
            try:
                if orig is None:
                    raise ValueError()
                f.update(thumbnails(orig, masked_by_new_fn.get(str(row["NEW_FN"]))))
            except Exception as exc:  # noqa: BLE001
                f.update(frames_orig=[], frames_masked=None, n_frames=0, sampled=False, rows=0, cols=0,
                         preview_error=f"pixel preview unavailable ({type(exc).__name__})")
            case["files"].append(f)
        case["date_ok"] = all(date_checks) if date_checks else None
    for le in load_errors:
        rel = le["file"]
        case["files"].append(dict(id=file_id(salt, name, rel), name=Path(rel).name, rel=rel, status="not_loaded",
                                  note=le["message"], boxes=[], gate_regions=[]))
    for f in case["files"]:
        f["verdict"] = case["verdict"] if f["status"] == "ok" else "NOT_LOADED"
        f["reason"] = case["reason"] if f["status"] == "ok" else "load error"
        f["tiers"] = list(case["tiers"]) if f["status"] == "ok" else []
    return case


# ----------------------------------------------------------------------------
# HTML
# ----------------------------------------------------------------------------

def _json_for_script(obj) -> str:
    """JSON safe inside a <script> element, with no literal '://' or '<' in the page."""
    return (json.dumps(obj, separators=(",", ":"))
            .replace("<", "\\u003c").replace(">", "\\u003e").replace("&", "\\u0026").replace("/", "\\u002f"))


_CSS = """
:root{--bg:#fff;--fg:#1c1c1e;--mut:#6b6b70;--card:#f4f4f6;--bd:#d0d0d6;--bad:#b3261e;--ok:#1b7a3a;--warn:#9a6700;--chg:#fff3c4;--rm:#fde0de;--add:#dff3e4}
@media (prefers-color-scheme:dark){:root{--bg:#161618;--fg:#ececef;--mut:#9a9aa2;--card:#222226;--bd:#3a3a40;--bad:#ff8b84;--ok:#6fd08c;--warn:#e6b84d;--chg:#4a4220;--rm:#4a2422;--add:#1f3a2a}}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.4 system-ui,sans-serif}
.phi{background:var(--bad);color:#fff;padding:10px 16px;font-weight:700;position:sticky;top:0;z-index:5}
.warn{background:var(--warn);color:#fff;padding:8px 16px;font-weight:600}
main{padding:12px 16px;max-width:1300px;margin:0 auto}
.bar{display:flex;gap:12px;align-items:center;flex-wrap:wrap;margin:8px 0 14px}
button{font:inherit;padding:5px 12px;border:1px solid var(--bd);background:var(--card);color:var(--fg);border-radius:6px;cursor:pointer}
button.on{background:var(--ok);color:#fff;border-color:var(--ok)}button.off{background:var(--bad);color:#fff;border-color:var(--bad)}
.case{background:var(--card);border:1px solid var(--bd);border-radius:10px;padding:12px;margin:14px 0}
.pill{display:inline-block;padding:2px 10px;border-radius:99px;color:#fff;font-weight:700}
.CLEAN{background:var(--ok)}.REDACT{background:var(--warn)}.QUARANTINE,.NOT_LOADED,.NO_IMAGES{background:var(--bad)}
.file{border-top:1px solid var(--bd);margin-top:10px;padding-top:10px}
.pix{position:relative;display:inline-block;line-height:0;border:1px solid var(--bd);background:#000}
.pix img{max-width:100%;height:auto;image-rendering:pixelated}
.pix .bx{position:absolute;border:2px solid #ff2d55;box-sizing:border-box;pointer-events:none}
.pix .gr{position:absolute;border:2px dashed #ffd60a;box-sizing:border-box;pointer-events:none}
.row{display:flex;gap:16px;flex-wrap:wrap;align-items:flex-start}
table{border-collapse:collapse;font:12px/1.3 ui-monospace,monospace;max-width:720px}
td,th{border:1px solid var(--bd);padding:2px 6px;text-align:left;vertical-align:top;word-break:break-word}
tr.changed td{background:var(--chg)}tr.removed td{background:var(--rm)}tr.added td{background:var(--add)}
s{opacity:.7}.hdr{max-height:360px;overflow:auto}
textarea{width:100%;min-height:36px;font:inherit}
.mut{color:var(--mut)}
"""

_JS = r"""
(function(){
var DATA=JSON.parse(document.getElementById('report-data').textContent);
var ALLOWED=JSON.parse(document.getElementById('allowed-keys').textContent);
var STATE={all:true,files:{}};
function pick(o,keys){var r={};for(var i=0;i<keys.length;i++){if(Object.prototype.hasOwnProperty.call(o,keys[i])){r[keys[i]]=o[keys[i]];}}return r;}
function buildVerdictsPayload(entries,allowed){
  var files=[];
  for(var i=0;i<entries.length;i++){
    var e=pick(entries[i],allowed.file);
    e.ticks=pick(entries[i].ticks||{},allowed.ticks);
    files.push(e);
  }
  var out={};out[allowed.top[0]]=files;return out;
}
window.buildVerdictsPayload=buildVerdictsPayload;
function el(t,c,x){var e=document.createElement(t);if(c)e.className=c;if(x!==undefined)e.textContent=x;return e;}
function fileState(id){if(!STATE.files[id]){STATE.files[id]={on:STATE.all,ticks:{text_missed:false,over_masked:false,header_problem:false},frame:0,refresh:null};}return STATE.files[id];}
var root=document.getElementById('cases');
if(DATA.missing_intake>0){var w=el('div','warn','No intake dates supplied for '+DATA.missing_intake+' case(s): a placeholder operation date was used, so the date check is meaningless.');document.getElementById('banners').appendChild(w);}
var gbtn=document.getElementById('global');
function setGlobal(v){STATE.all=v;gbtn.textContent='All edits: '+(v?'ON':'OFF');gbtn.className=v?'on':'off';for(var k in STATE.files){STATE.files[k].on=v;if(STATE.files[k].refresh)STATE.files[k].refresh();}}
gbtn.onclick=function(){setGlobal(!STATE.all);};
DATA.cases.forEach(function(c){
  var card=el('div','case');
  var h=el('div','');var p=el('span','pill '+c.verdict,c.verdict);h.appendChild(p);
  h.appendChild(el('strong','',' '+c.name));
  card.appendChild(h);
  card.appendChild(el('div','','Reason: '+(c.reason||'-')));
  card.appendChild(el('div','','Tiers fired: '+(c.tiers.length?c.tiers.join(', '):'none')));
  card.appendChild(el('div','mut','Header dates equal operation date: '+(c.date_ok===null?'n/a':(c.date_ok?'yes':'NO'))+(DATA.missing_intake>0&&c.placeholder?' (placeholder date, meaningless)':'')+'; other files skipped: '+c.skipped));
  c.load_errors.forEach(function(le){card.appendChild(el('div','warn','Load error: '+le.file+': '+le.message));});
  c.files.forEach(function(f){
    var st=fileState(f.id);
    var box=el('div','file');
    box.appendChild(el('div','','File: '+f.rel+' ['+f.verdict+']'));
    if(f.status!=='ok'){box.appendChild(el('div','mut',f.note||''));card.appendChild(box);return;}
    var tb=el('button','on','EDITS ON');box.appendChild(tb);
    var row=el('div','row');
    var pixwrap=el('div','');
    var pix=el('div','pix');var img=el('img');var ov=el('div','');
    pix.appendChild(img);pix.appendChild(ov);pixwrap.appendChild(pix);
    var cap=el('div','mut','');pixwrap.appendChild(cap);
    var slider=null;
    if(f.frames_orig.length>1){slider=document.createElement('input');slider.type='range';slider.min=0;slider.max=f.frames_orig.length-1;slider.value=0;slider.oninput=function(){st.frame=+slider.value;refresh();};pixwrap.appendChild(slider);}
    row.appendChild(pixwrap);
    var hw=el('div','hdr');var tbl=el('table');hw.appendChild(tbl);row.appendChild(hw);
    box.appendChild(row);
    function refresh(){
      tb.textContent=st.on?'EDITS ON':'EDITS OFF';tb.className=st.on?'on':'off';
      if(f.preview_error){cap.textContent=f.preview_error;}
      else{
        var m=st.on&&f.frames_masked?f.frames_masked:f.frames_orig;
        img.src='data:image/png;base64,'+m[Math.min(st.frame,m.length-1)];
        cap.textContent='frame '+(st.frame+1)+'/'+f.frames_orig.length+(f.sampled?' (sampled from '+f.n_frames+')':'')+(st.on?' - masked as upload would send':' - original');
      }
      ov.innerHTML='';
      if(st.on&&f.rows){f.boxes.forEach(function(b){var d=el('div','bx');d.style.left=(100*b[0]/f.cols)+'%';d.style.top=(100*b[1]/f.rows)+'%';d.style.width=(100*b[2]/f.cols)+'%';d.style.height=(100*b[3]/f.rows)+'%';ov.appendChild(d);});}
      tbl.innerHTML='';
      var hr=el('tr');['Tag','Name',st.on?'Original':'Value',st.on?'Processed':''].forEach(function(t){if(t)hr.appendChild(el('th','',t));});tbl.appendChild(hr);
      f.header.forEach(function(r){
        var tr=el('tr',st.on?r[4]:'');tr.appendChild(el('td','',r[0]));tr.appendChild(el('td','',r[1]));
        var o=el('td','');
        if(st.on&&(r[4]==='removed'||r[4]==='changed')){o.appendChild(el('s','',r[2]));}else{o.textContent=r[2];}
        tr.appendChild(o);
        if(st.on){tr.appendChild(el('td','',r[3]));}
        tbl.appendChild(tr);
      });
    }
    st.refresh=refresh;
    tb.onclick=function(){st.on=!st.on;refresh();};
    var tk=el('div','');
    [['text_missed','Text missed'],['over_masked','Over-masked'],['header_problem','Header problem']].forEach(function(t){
      var lab=el('label','');var cb=document.createElement('input');cb.type='checkbox';cb.onchange=function(){st.ticks[t[0]]=cb.checked;};lab.appendChild(cb);lab.appendChild(document.createTextNode(' '+t[1]+'  '));tk.appendChild(lab);});
    box.appendChild(tk);
    var note=el('textarea');note.placeholder='Note (stays in this page; never downloaded)';box.appendChild(note);
    card.appendChild(box);refresh();
  });
  root.appendChild(card);
});
setGlobal(true);
document.getElementById('dl').onclick=function(){
  var entries=[];
  DATA.cases.forEach(function(c){c.files.forEach(function(f){
    var st=fileState(f.id);
    entries.push({file_id:f.id,verdict:f.verdict,reason:f.reason,tiers:f.tiers,ticks:st.ticks});
  });});
  var payload=buildVerdictsPayload(entries,ALLOWED);
  var blob=new Blob([JSON.stringify(payload,null,1)],{type:'application/json'});
  var a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download='verdicts.json';document.body.appendChild(a);a.click();a.remove();
};
})();
"""


def render_html(cases: List[dict], missing_intake: int, salt: str) -> str:
    for c in cases:
        c["placeholder"] = bool(c.get("placeholder"))
    data = dict(salt=salt, missing_intake=missing_intake, cases=cases)
    return (
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>Real data review report</title><style>" + _CSS + "</style></head><body>"
        "<div class=\"phi\">Contains PHI - treat like the source files.</div>"
        "<div id=\"banners\"></div><main>"
        "<div class=\"bar\"><button id=\"global\" class=\"on\">All edits: ON</button>"
        "<button id=\"dl\">Download verdicts</button>"
        "<span class=\"mut\">verdicts.json holds only a salted file hash, ticks, verdict, reason and tiers.</span></div>"
        "<div id=\"cases\"></div></main>"
        "<script id=\"report-data\" type=\"application/json\">" + _json_for_script(data) + "</script>"
        "<script id=\"allowed-keys\" type=\"application/json\">" + _json_for_script(ALLOWED_KEYS) + "</script>"
        "<script>" + _JS + "</script></body></html>\n"
    )


# ----------------------------------------------------------------------------
# Public entry points
# ----------------------------------------------------------------------------

def refuse_reason(out_path: Path, cases_dir: Path) -> Optional[str]:
    """None when *out_path* is safe to write, else a short soft message."""
    if _inside(out_path, REPO):
        return "refused: --out is inside the repository; choose a folder outside it"
    if _inside(out_path, cases_dir):
        return "refused: --out is inside the cases folder; choose another folder"
    return None


def build_report(cases_dir, intake: Optional[dict], out_path, *, profiles_dir: Optional[str] = None,
                 salt: Optional[str] = None) -> Path:
    """Process every case folder under *cases_dir* and write one HTML file to *out_path*."""
    cases_dir, out_path = Path(cases_dir), Path(out_path)
    msg = refuse_reason(out_path, cases_dir)
    if msg:
        raise PermissionError(msg)
    salt = salt or secrets.token_hex(16)
    profiles_dir = profiles_dir or str(REPO / "data" / "device_profiles")
    _ensure_identity_salt(salt)
    intake = intake or {}
    config = _offline_config()
    cases, missing = [], 0
    with tempfile.TemporaryDirectory(prefix="real_report_") as tmp:
        for i, folder in enumerate(sorted(p for p in cases_dir.iterdir() if p.is_dir())):
            entry = intake.get(folder.name)
            placeholder = entry is None
            if placeholder:
                missing += 1
                entry = dict(operation_date=PLACEHOLDER_DATE, start_time=PLACEHOLDER_TIME)
            else:
                entry = dict(operation_date=str(entry["operation_date"]), start_time=str(entry["start_time"]))
            case = process_case(folder, entry, salt, profiles_dir, Path(tmp) / f"c{i}", config)
            case["placeholder"] = placeholder
            cases.append(case)
    html = render_html(cases, missing, salt)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html, encoding="utf-8")
    return out_path


@contextlib.contextmanager
def _silenced():
    """Nothing a library prints or logs may reach the terminal (stdout and stderr, Python and OS level)."""
    sys.stdout.flush()
    sys.stderr.flush()
    saved = (os.dup(1), os.dup(2), sys.stdout, sys.stderr)
    devnull = open(os.devnull, "w")
    logging.disable(logging.CRITICAL)
    try:
        os.dup2(devnull.fileno(), 1)
        os.dup2(devnull.fileno(), 2)
        sys.stdout = sys.stderr = devnull
        yield
    finally:
        sys.stdout, sys.stderr = saved[2], saved[3]
        os.dup2(saved[0], 1)
        os.dup2(saved[1], 2)
        os.close(saved[0])
        os.close(saved[1])
        devnull.close()
        logging.disable(logging.NOTSET)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--cases", required=True)
    parser.add_argument("--intake", default=None)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--profiles-dir", default=None)

    def finish(line: str, code: int) -> int:
        sys.stdout.write(f"{line}\nexit code: {code}\n")
        sys.stdout.flush()
        return code

    # Exceptions ignored in __del__ (also at interpreter exit) print a traceback to stderr: drop them.
    saved_hook = sys.unraisablehook
    sys.unraisablehook = lambda _unraisable: None
    try:
        return _main(parser, finish, argv)
    finally:
        with _silenced():
            import gc

            gc.collect()
        sys.unraisablehook = saved_hook


def _main(parser, finish, argv) -> int:
    try:
        with _silenced():
            try:
                args = parser.parse_args(argv)
                args_ok = True
            except SystemExit:
                args_ok = False
        if not args_ok:
            return finish("refused: bad arguments (need --cases)", 2)
        cases_dir = Path(args.cases).expanduser()
        out_path = Path(args.out).expanduser()
        msg = refuse_reason(out_path, cases_dir)
        if msg:
            return finish(msg, 2)
        if not cases_dir.is_dir():
            return finish("refused: --cases is not a folder", 2)
        intake = None
        if args.intake:
            with open(Path(args.intake).expanduser(), encoding="utf-8") as fh:
                intake = json.load(fh)
        with _silenced():
            path = build_report(cases_dir, intake, out_path, profiles_dir=args.profiles_dir)
        return finish(f"report: {path}", 0)
    except BaseException as exc:  # noqa: BLE001 - never print a message, only the class
        return finish(f"failed: {type(exc).__name__}", 2)


if __name__ == "__main__":
    _code = main()
    sys.unraisablehook = lambda _unraisable: None  # interpreter-exit cleanup must stay silent too
    sys.exit(_code)
