"""
US3 / concurrency (spec 014, FR-013, SC-007): one session downloads CONC_A
while a second, independent session replaces CONC_B. Both sessions are
production ``PyxnatGateway`` objects; ``XNATConnection`` is a process-wide
singleton (SPEC-ISSUE-19), so it cannot provide two live sessions at once.
"""
from __future__ import annotations

import os
import threading

import pytest

from tests.integration.live_xnat import helpers as H

pytestmark = [pytest.mark.requires_server, pytest.mark.slow]

RESOURCE = "LIVE_IT_CONC"


def test_download_while_other_session_uploads(live_xnat_server, tmp_path):
    from src.services import xnat_conventions as conventions

    proj_qs = conventions.project_qs(live_xnat_server["project_name"])
    seed = live_xnat_server["gateway_session"]()
    a_bytes, b_old, b_new = os.urandom(2_000_000), os.urandom(4096), os.urandom(8192)
    for name, data in (("conc_a.bin", a_bytes), ("conc_b.bin", b_old)):
        (tmp_path / name).write_bytes(data)
        seed.put_file(proj_qs, RESOURCE, name, str(tmp_path / name), content="TEST", format="BIN",
                      tags="LIVE_IT", overwrite=True)
    a_sha = H.sha256(a_bytes)
    (tmp_path / "conc_b_new.bin").write_bytes(b_new)

    reader, writer = live_xnat_server["gateway_session"](), live_xnat_server["gateway_session"]()
    barrier = threading.Barrier(2)
    errors = {}

    def _read():
        try:
            barrier.wait(timeout=30)
            reader.get_file_copy(proj_qs, RESOURCE, "conc_a.bin", str(tmp_path / "a_down.bin"))
        except Exception as exc:  # noqa: BLE001
            errors["read"] = f"{type(exc).__name__}: {exc}"

    def _write():
        try:
            barrier.wait(timeout=30)
            writer.put_file(proj_qs, RESOURCE, "conc_b.bin", str(tmp_path / "conc_b_new.bin"),
                            content="TEST", format="BIN", tags="LIVE_IT", overwrite=True)
        except Exception as exc:  # noqa: BLE001
            errors["write"] = f"{type(exc).__name__}: {exc}"

    threads = [threading.Thread(target=_read), threading.Thread(target=_write)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=120)
    assert not errors, errors
    assert H.sha256(tmp_path / "a_down.bin") == a_sha
    check = live_xnat_server["gateway_session"]()
    check.get_file_copy(proj_qs, RESOURCE, "conc_b.bin", str(tmp_path / "b_check.bin"))
    assert H.sha256(tmp_path / "b_check.bin") == H.sha256(b_new)
