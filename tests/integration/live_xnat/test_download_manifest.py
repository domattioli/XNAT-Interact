"""
Spec 015 live tests: the download record against a real XNAT (FR-015, FR-016, FR-018a).

Publishes a synthetic case through production (spec 014 harness), downloads
it through the production ``download_selection``, and checks the manifest
against what the server holds.  Requires the spec 014 local test bed.
"""
from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest

from tests.integration.live_xnat import helpers as H

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

CASE = "KNEE_2025"


@pytest.fixture(scope="module")
def published(knee_rf_published):
    # Shared with the spec 014 KNEE module: one publish per run avoids the
    # spec 009 duplicate guard rejecting a second copy of the same case.
    result = knee_rf_published["rf"]
    assert result.scan_uri, f"publish failed: {result.production_exception or result.publish_exception}"
    return {"result": result, "work": knee_rf_published["work"]}


def _row(result):
    return [{
        "subject": result.subject_label,
        "experiment": result.experiment_label,
        "scan_id": result.scan_uri.rstrip("/").split("/")[-1],
    }]


def _download(live, published, dest: Path):
    from app.logic.download import download_selection
    from app.logic.download_manifest import ManifestIdentity

    gw = live["gateway_session"]()
    identity = ManifestIdentity(username=live["username"], server_url=live["server_url"])
    outcome = download_selection(gw.server, live["project_name"], _row(published["result"]), dest,
                                 identity=identity, uploader=gw)
    assert outcome.ok, outcome.friendly
    return json.loads(Path(outcome.manifest_path).read_text(encoding="utf-8"))


def _strip(files):
    return sorted(json.dumps(f, sort_keys=True) for f in files)


def test_every_entry_matches_the_server(live_xnat_server, published, downloads_cleanup):  # T025
    m = _download(live_xnat_server, published, published["work"] / "dl1")
    downloads_cleanup.append(m["server_copy"]["filename"])
    assert m["complete"] and m["files"]
    for e in m["files"]:
        uri = f"{e['scan_query_string']}/resources/{e['resource_label']}/files/{e['filename']}"
        assert H.sha256(H.get_bytes(live_xnat_server, uri)) == e["sha256"], e["relative_path"]
    assert m["server_copy"]["status"] == "uploaded"


def test_two_downloads_have_identical_entries(live_xnat_server, published, downloads_cleanup):  # T026
    a = _download(live_xnat_server, published, published["work"] / "dl2a")
    b = _download(live_xnat_server, published, published["work"] / "dl2b")
    downloads_cleanup.extend([a["server_copy"]["filename"], b["server_copy"]["filename"]])
    assert _strip(a["files"]) == _strip(b["files"])
    assert a["run_id"] != b["run_id"]
    assert a["server_copy"]["filename"] != b["server_copy"]["filename"]
    from src.services import xnat_conventions as conventions
    listed = live_xnat_server["gateway_session"]().list_files(
        conventions.project_qs(live_xnat_server["project_name"]), conventions.DOWNLOADS_RESOURCE)
    assert a["server_copy"]["filename"] in listed and b["server_copy"]["filename"] in listed


def test_concurrent_downloads_both_land(live_xnat_server, published, downloads_cleanup):  # T027
    barrier = threading.Barrier(2)
    results, errors = {}, {}

    def _go(key):
        try:
            barrier.wait(timeout=30)
            results[key] = _download(live_xnat_server, published, published["work"] / f"dl3{key}")
        except Exception as exc:  # noqa: BLE001
            errors[key] = f"{type(exc).__name__}: {exc}"

    threads = [threading.Thread(target=_go, args=(k,)) for k in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=300)
    assert not errors, errors
    names = [results[k]["server_copy"]["filename"] for k in ("a", "b")]
    downloads_cleanup.extend(names)
    assert names[0] != names[1]
    assert all(results[k]["server_copy"]["status"] == "uploaded" for k in ("a", "b"))
    from src.services import xnat_conventions as conventions
    listed = live_xnat_server["gateway_session"]().list_files(
        conventions.project_qs(live_xnat_server["project_name"]), conventions.DOWNLOADS_RESOURCE)
    assert set(names) <= set(listed)
