"""Spec 016 US4: the publish-analysis command line."""
import json
from types import SimpleNamespace

import pytest

from src.services.analysis_intake import cli
from tests.fakes.analysis_folders import (
    EXPERIMENT, PROJECT, SUBJECT, USERNAME, FakeConfigTables, clean_angles_folder, dirty_extra_file,
)
from tests.fakes.fake_xnat import FakeXNAT


def _no_login(*args, **kwargs):
    raise AssertionError("login must not be asked for")


def _run(argv):
    lines = []
    code = cli.main(argv, connect=_no_login, out=lines.append)
    return code, "\n".join(lines)


def test_init_writes_template(tmp_path):
    code, text = _run(["--init", "knee_flexion_angle", str(tmp_path / "res")])
    assert code == 0 and "analysis.json" in text
    assert json.loads((tmp_path / "res" / "analysis.json").read_text())["type_name"] == "knee_flexion_angle"


def test_init_twice_refused(tmp_path):
    _run(["--init", "knee_flexion_angle", str(tmp_path)])
    code, text = _run(["--init", "knee_flexion_angle", str(tmp_path)])
    assert code == 1 and "already" in text


def test_list_types():
    code, text = _run(["--list-types"])
    assert code == 0
    assert "knee_flexion_angle" in text and "segmentation_consensus" in text


def test_dry_run_clean_and_dirty(tmp_path, monkeypatch):
    from src.services.analysis_intake import gates
    monkeypatch.setattr(gates, "default_classifier", lambda: (lambda text: (False, [])))
    clean = clean_angles_folder(tmp_path / "c")
    code, text = _run([str(clean.output), "--manifest", str(clean.manifest), "--dry-run"])
    assert code == 0 and "Nothing was uploaded" in text
    dirty = dirty_extra_file(tmp_path / "d")
    code, text = _run([str(dirty.output), "--manifest", str(dirty.manifest), "--dry-run"])
    assert code == 1 and "scratch.txt" in text and "Traceback" not in text


def test_dry_run_without_classifier_fails_closed(tmp_path, monkeypatch):
    from src.services.analysis_intake import gates

    def unavailable():
        raise gates.ClassifierUnavailable("ImportError")
    monkeypatch.setattr(gates, "default_classifier", unavailable)
    clean = clean_angles_folder(tmp_path)
    code, text = _run([str(clean.output), "--manifest", str(clean.manifest), "--dry-run"])
    assert code == 1 and "PHI" in text


def test_no_password_or_server_option():
    options = {s for a in cli.build_parser()._actions for s in a.option_strings}
    assert not any(word in o for o in options for word in ("pass", "server", "url", "host", "token"))


def test_unknown_option_is_plain_message():
    code, text = _run(["--password", "x"])
    assert code == 1 and "Traceback" not in text


def test_real_publish_uses_connect(tmp_path, monkeypatch):
    from src.services.analysis_intake import gates
    monkeypatch.setattr(gates, "default_classifier", lambda: (lambda text: (False, [])))
    fx = FakeXNAT(project_name=PROJECT)
    fx.seed_rf_experiment(SUBJECT, EXPERIMENT)
    ct = FakeConfigTables()
    closed = []
    conn = SimpleNamespace(gateway=fx, close=lambda: closed.append(True))
    case = clean_angles_folder(tmp_path)
    lines = []
    code = cli.main([str(case.output), "--manifest", str(case.manifest)],
                    connect=lambda username: (USERNAME, conn, ct), out=lines.append)
    assert code == 0, lines
    assert "knee_flexion_angle__v1" in "\n".join(lines)
    assert closed == [True]


def test_main_py_dispatches_subcommand(monkeypatch):
    main = pytest.importorskip("main")
    seen = []
    monkeypatch.setattr("sys.argv", ["main.py", "publish-analysis", "--list-types"])
    monkeypatch.setattr(cli, "main", lambda argv, **kw: seen.append(argv) or 0)
    with pytest.raises(SystemExit) as exc:
        main.main()
    assert exc.value.code == 0 and seen == [["--list-types"]]
