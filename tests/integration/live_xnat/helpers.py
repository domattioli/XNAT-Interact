"""
Helpers for the live-XNAT suite (spec 014): publish a case through the
production pipeline, read results back over the admin session, and retry a
verification read once after a 401/403.

Every write to XNAT for source data goes through production code
(`ORDataIntakeForm`, `SourceRFSession` / `SourceESVSession`,
`write_publish_catalog_subroutine`). Verification reads use `conn.server._exec`
or `conn.gateway` with the session the conftest opened. No credentials here.
"""
from __future__ import annotations

import hashlib
import io
import json
import re
import shutil
import tempfile
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest


# --------------------------------------------------------------------------- #
# Re-authentication (T013)
# --------------------------------------------------------------------------- #
def _auth_status(exc: BaseException) -> Optional[int]:
    resp = getattr(exc, "response", None)
    code = getattr(resp, "status_code", None) or getattr(exc, "status_code", None)
    if code in (401, 403):
        return int(code)
    text = str(exc)
    for c in ("401", "403"):
        if re.search(rf"\b{c}\b", text) and ("Unauthorized" in text or "Forbidden" in text or "status" in text.lower()):
            return int(c)
    return None


def with_reauth(session_box: Dict[str, Any], fn: Callable[[Any], Any]) -> Any:
    """
    Run ``fn(connection)``. On a 401/403 open one fresh session through
    ``session_box["new_session"]``, store it in the box, and retry once. A
    second auth failure fails the test with a named reason (spec edge case).
    """
    try:
        return fn(session_box["connection"])
    except Exception as exc:  # noqa: BLE001
        if _auth_status(exc) is None:
            raise
    login, conn = session_box["new_session"]()
    session_box["login"], session_box["connection"] = login, conn
    try:
        return fn(conn)
    except Exception as exc:  # noqa: BLE001
        if _auth_status(exc) is not None:
            pytest.fail(f"admin session expired: {session_box['server_url']}")
        raise


def data_uri(qs: str) -> str:
    """REST URI for a gateway query string (gateway strings omit ``/data``)."""
    uri = qs if qs.startswith("/") else "/" + qs
    for one, many in (("project", "projects"), ("subject", "subjects"), ("experiment", "experiments"),
                      ("scan", "scans"), ("resource", "resources"), ("file", "files")):
        uri = re.sub(rf"/{one}/", f"/{many}/", uri)
    return uri if uri.startswith("/data/") else "/data" + uri


def get_json(session_box: Dict[str, Any], uri: str) -> dict:
    uri = data_uri(uri)
    raw = with_reauth(session_box, lambda c: c.server._exec(uri, "GET"))
    return json.loads(raw)


def get_bytes(session_box: Dict[str, Any], uri: str) -> bytes:
    uri = data_uri(uri)
    def _get(c):
        resp = c.server.get(uri)  # pyxnat Interface.get returns a requests.Response
        if resp.status_code in (401, 403):
            resp.raise_for_status()
        if resp.status_code != 200:
            raise RuntimeError(f"GET {uri} returned HTTP {resp.status_code}")
        return resp.content

    return with_reauth(session_box, _get)


# --------------------------------------------------------------------------- #
# Publishing (T014)
# --------------------------------------------------------------------------- #
@dataclass
class PublishResult:
    """Outcome of one production publish (data-model.md)."""

    session_kind: str
    uid: Optional[str] = None
    subject_label: Optional[str] = None
    experiment_label: Optional[str] = None
    scan_uri: Optional[str] = None
    files_uri: Optional[str] = None
    published_files: Dict[str, Path] = field(default_factory=dict)  # NEW_FN to local de-identified copy
    source_to_new: Dict[str, str] = field(default_factory=dict)  # source file name to NEW_FN
    rejections: Dict[str, str] = field(default_factory=dict)
    production_exception: Optional[str] = None
    publish_exception: Optional[str] = None
    pixel_decision: Optional[str] = None
    pixel_decision_error: Optional[str] = None
    prompts_answered: List[str] = field(default_factory=list)
    workdir: Optional[Path] = None


def _confirmer_for(case, kind: str, result: PublishResult):
    """
    Build the production automated confirmer from the case's first DICOM frame
    (T014). Production returns QUARANTINE for every unprofiled synthetic device,
    which blocks the upload (src/xnat_experiment_data.py:542). The harness
    records that decision and then confirms, as tests/stress/driver.py does, so
    the rest of the pipeline is exercised (research SPEC-ISSUE-10).
    """
    from src.xnat_experiment_data import ReviewDecision, make_automated_pixel_confirmer

    dicoms = [p for p in case.rf_files if p.suffix == ".dcm"] if kind == "rf" else []
    if dicoms:
        try:
            import pydicom

            ds = pydicom.dcmread(str(dicoms[0]))
            auto = make_automated_pixel_confirmer([ds.pixel_array], ds)
            decision, _ = auto(f"{case.name} {kind}")
            result.pixel_decision = getattr(decision, "value", str(decision))
        except Exception as exc:  # noqa: BLE001
            result.pixel_decision_error = f"{type(exc).__name__}: {exc}"
    else:
        result.pixel_decision = "not_applicable"

    def _confirm(_context: str):
        return ReviewDecision.CONFIRMED, []

    return _confirm


def publish_case_session(live: Dict[str, Any], case, kind: str, workdir: Path) -> PublishResult:
    """Publish ``case``'s ``kind`` folder (rf or esv) through production code."""
    from src.xnat_experiment_data import (
        ORDataIntakeForm,
        SourceESVSession,
        SourceRFSession,
    )

    result = PublishResult(session_kind=kind, workdir=Path(workdir))
    login, conn = live["login"], live["connection"]
    config = live["config_factory"]()

    # SPEC-ISSUE-16: SourceESVSession asks on stdin whether to proceed when a
    # folder holds video but no still images (src/xnat_experiment_data.py:1272).
    # The harness answers "1" (proceed) as an operator would, and records it.
    def _operator_answer(prompt=""):
        result.prompts_answered.append(str(prompt).strip())
        return "1"

    import builtins
    from unittest import mock

    with mock.patch.object(builtins, "input", _operator_answer):
        return _publish(case, kind, workdir, result, login, conn, config)


def _publish(case, kind, workdir, result, login, conn, config) -> PublishResult:
    from src.xnat_experiment_data import (
        ORDataIntakeForm,
        SourceESVSession,
        SourceRFSession,
    )

    try:
        form = ORDataIntakeForm(
            config=config, validated_login=login, input_data=case.intake_series(kind),
            verbose=False, write_file=True,
        )
        result.uid = form.uid
        cls = SourceRFSession if kind == "rf" else SourceESVSession
        session = cls(intake_form=form, config=config)
    except Exception as exc:  # noqa: BLE001
        result.production_exception = f"{type(exc).__name__}: {exc}"
        result.rejections.update(_rejections_from_text(case, str(exc)))
        return result

    result.subject_label = form.uid
    if getattr(session, "df", None) is not None and "NEW_FN" in session.df:
        for _, row in session.df.iterrows():
            fn = str(row.get("FN"))
            ext = row.get("EXT")
            name = fn + (str(ext) if isinstance(ext, str) and not fn.endswith(ext) else "")
            if row.get("IS_VALID"):
                result.source_to_new[name] = str(row["NEW_FN"])
            else:
                result.rejections[name] = "IS_VALID false in production dataframe"
    if not getattr(session, "is_valid", False):
        result.production_exception = "production session is_valid is False"
        return result

    # Test-side spies: keep the zip (delete_zip=False) and record publish errors
    # that write_publish_catalog_subroutine swallows (src/xnat_experiment_data.py:885).
    captured: Dict[str, Any] = {}
    orig_write, orig_publish = session.write, session.publish_to_xnat

    def _spy_write(*a, **kw):
        zipped, cfg = orig_write(*a, **kw)
        captured["zips"] = list(zipped)
        return zipped, cfg

    def _spy_publish(*a, **kw):
        try:
            return orig_publish(*a, **kw)
        except Exception as exc:  # noqa: BLE001
            captured["publish_exc"] = f"{type(exc).__name__}: {exc}"
            raise

    session.write, session.publish_to_xnat = _spy_write, _spy_publish
    confirmer = _confirmer_for(case, kind, result)
    try:
        config = session.write_publish_catalog_subroutine(
            config, conn, login, verbose=False, delete_zip=False,
            pixel_review_confirmer=confirmer,
        )
        # The subroutine already calls config.push_to_xnat() and then deletes the
        # local config file (src/xnat_experiment_data.py:893-910); a second push
        # here would fail on the missing file.
    except Exception as exc:  # noqa: BLE001
        result.production_exception = f"{type(exc).__name__}: {exc}"
    result.publish_exception = captured.get("publish_exc")

    subj_qs, exp_qs, scan_qs, files_qs, res_label = session._generate_queries(conn)
    result.experiment_label = exp_qs.rstrip("/").split("/")[-1]
    result.scan_uri = scan_qs
    result.files_uri = files_qs

    out = Path(workdir) / f"{case.name}_{kind}_published"
    out.mkdir(parents=True, exist_ok=True)
    for z in captured.get("zips", []):
        if Path(z).exists():
            with zipfile.ZipFile(z) as zf:
                zf.extractall(out)
    for p in sorted(out.rglob("*")):
        if p.is_file():
            result.published_files[p.name] = p
    return result


def _rejections_from_text(case, text: str) -> Dict[str, str]:
    found = {}
    for p in case.files:
        if p.stem in text:
            found[p.name] = text.split("\n")[0][:300]
    return found


# --------------------------------------------------------------------------- #
# Read-back and derived resources (T015)
# --------------------------------------------------------------------------- #
def sha256(data) -> str:
    if isinstance(data, (str, Path)):
        data = Path(data).read_bytes()
    return hashlib.sha256(data).hexdigest()


def list_scan_files(live: Dict[str, Any], result: PublishResult) -> List[dict]:
    uri = f"{result.scan_uri.rstrip('/')}/resources/SRC/files?format=json"
    return get_json(live, uri)["ResultSet"]["Result"]


def download_scan_files(live: Dict[str, Any], result: PublishResult, dest: Path) -> Dict[str, Path]:
    """Download every file of the published scan resource over the admin session."""
    dest = Path(dest)
    dest.mkdir(parents=True, exist_ok=True)
    files = {}
    for row in list_scan_files(live, result):
        name = row["Name"]
        # The listing's URI (project/subject/experiment-id path) returns 404 for
        # rfSessionData on this server; the label path from the session works.
        uri = f"{result.scan_uri.rstrip('/')}/resources/SRC/files/{name}"
        target = dest / name
        target.write_bytes(get_bytes(live, uri))
        files[name] = target
    return files


def put_derived_resource(live: Dict[str, Any], result: PublishResult, label: str, files: Dict[str, bytes]) -> List[str]:
    """Store derived files under resource ``label`` on the case scan; return their URIs."""
    gateway = live["connection"].gateway
    uris = []
    with tempfile.TemporaryDirectory() as td:
        for name, data in files.items():
            ffn = Path(td) / name
            ffn.write_bytes(data)
            gateway.put_file(result.scan_uri, label, name, str(ffn),
                             content="DERIVED", format=name.rsplit(".", 1)[-1].upper(),
                             tags="LIVE_IT", overwrite=True)
            uris.append(f"{result.scan_uri.rstrip('/')}/resources/{label}/files/{name}")
    return uris


def get_derived_resource(live: Dict[str, Any], result: PublishResult, label: str) -> Dict[str, bytes]:
    uri = f"{result.scan_uri.rstrip('/')}/resources/{label}/files?format=json"
    rows = get_json(live, uri)["ResultSet"]["Result"]
    base = f"{result.scan_uri.rstrip('/')}/resources/{label}/files/"
    return {r["Name"]: get_bytes(live, base + r["Name"]) for r in rows}


def server_identity_rows(live: Dict[str, Any]):
    """IMAGE_HASHES as stored on the server, read through a fresh session (FR-010)."""
    # new_session() switches the shared session (XNATConnection is a singleton,
    # SPEC-ISSUE-19), so later reads use the fresh session too.
    live["new_session"]()
    cfg = live["config_factory"]()
    return cfg.tables["IMAGE_HASHES"].copy(), cfg.tables["SUBJECTS"].copy()


def record_case_subject(live: Dict[str, Any], case_name: str, uid: Optional[str]) -> None:
    """Add ``uid`` to the server-side case ledger used by the FR-002 reset."""
    if not uid:
        return
    from tests.integration.live_xnat.conftest import _read_case_ledger, write_case_ledger

    conn = live["connection"]
    ledger = _read_case_ledger(conn, live["project_name"])
    ledger.setdefault(case_name, [])
    if uid not in ledger[case_name]:
        ledger[case_name].append(uid)
    write_case_ledger(conn, live["project_name"], ledger)


# --------------------------------------------------------------------------- #
# Phase records and summary
# --------------------------------------------------------------------------- #
def now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def set_phase(state: Dict[str, Any], phase: str, status: str, files=None, links=None, error=None) -> None:
    """Record (or overwrite) the result of one of the six FR-019 phases."""
    state.setdefault("phases", {})[phase] = {
        "status": status,
        "files": sorted(str(f) for f in (files or [])),
        "links": sorted(str(x) for x in (links or [])),
        "error": error,
    }


def write_case_summary(state: Dict[str, Any], writer, case_name: str) -> Path:
    """Write the six-phase outcome summary; a phase never reached is a failure."""
    from tests.integration.live_xnat.outcome_summary import PHASES, PipelinePhaseRecord

    writer.clear()
    # Production generates a new case uid every run; replace it with a stable
    # placeholder so two runs can be compared (SC-008).
    uids = {v.uid: f"<uid:{k}>" for k, v in state.items() if isinstance(v, PublishResult) and v.uid}

    def norm(x):
        if isinstance(x, str):
            for u, ph in uids.items():
                x = x.replace(u, ph)
            return re.sub(r"Xnat4Tests_[ES]\d+", "<xnat-id>", x)
        if isinstance(x, list):
            return [norm(i) for i in x]
        if isinstance(x, dict):
            return {norm(k): norm(v) for k, v in x.items()}
        return x

    phases = norm(state.get("phases", {}))
    for name in PHASES:
        rec = phases.get(name, {"status": "failure", "files": [], "links": [], "error": "phase not reached"})
        writer.add_phase_record(PipelinePhaseRecord(
            case_name=case_name, phase=name, status=rec["status"], timestamp=now_iso(),
            files_affected=rec["files"], xnat_resource_links=rec["links"], error_detail=rec["error"],
        ))
    c = state.get("counts", {})
    return writer.write_outcome(
        case_name=case_name,
        files_processed=c.get("processed", 0),
        files_recovered=c.get("recovered", 0),
        files_rejected=c.get("rejected", 0),
        checksum_pass_count=c.get("checksum_pass", 0),
        checksum_fail_count=c.get("checksum_fail", 0),
        uid_collisions_resolved=c.get("collisions_resolved", 0),
        uid_collisions_recorded=c.get("collisions_recorded", 0),
        absent_tags=norm(state.get("absent_tags", {})),
        dispositions=norm(state.get("dispositions", {})),
        scan_layout=norm(state.get("scan_layout", {})),
    )


def validate_summary(path: Path) -> None:
    """Validate a written summary against contracts/outcome-summary.schema.json."""
    import jsonschema

    schema_path = (Path(__file__).resolve().parents[3] / "specs" / "014-real-xnat-integration-testing"
                   / "contracts" / "outcome-summary.schema.json")
    schema = json.loads(schema_path.read_text())
    jsonschema.validate(json.loads(Path(path).read_text()), schema)


def scan_layout(live: Dict[str, Any], result: PublishResult) -> Dict[str, int]:
    """Scan id to file count for the experiment holding ``result``."""
    exp_uri = data_uri(result.scan_uri).rstrip("/").rsplit("/scans/", 1)[0]
    layout = {}
    for row in get_json(live, f"{exp_uri}/scans?format=json")["ResultSet"]["Result"]:
        sid = row["ID"]
        files = get_json(live, f"{exp_uri}/scans/{sid}/files?format=json")["ResultSet"]["Result"]
        layout[f"{result.experiment_label}/{sid}"] = len(files)
    return layout


def make_masks(frames, kind: str):
    """Synthetic segmenter output on downloaded frames: (n_frames, rows, cols) uint8."""
    import numpy as np

    arr = np.asarray(frames, dtype=float)
    mean = arr.mean()
    if kind == "rule_based":
        return (arr > mean).astype("uint8")
    if kind == "ml_style":
        return (arr > mean * 1.05).astype("uint8")
    return (arr > mean * 0.95).astype("uint8")  # hybrid


SEGMENTERS = ("rule_based", "ml_style", "hybrid")


def npz_bytes(**arrays) -> bytes:
    import numpy as np

    buf = io.BytesIO()
    np.savez_compressed(buf, **arrays)
    return buf.getvalue()


def npz_load(data: bytes) -> Dict[str, Any]:
    import numpy as np

    with np.load(io.BytesIO(data)) as z:
        return {k: z[k] for k in z.files}
