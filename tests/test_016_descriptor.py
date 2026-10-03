"""Spec 016 US4: descriptor template, loading and checks."""
import json

import pytest

from src.services.analysis_intake import IntakeRefusal, load_descriptor, load_types, write_descriptor_template
from src.services.analysis_intake.descriptor import check_descriptor
from tests.fakes.analysis_folders import write_analysis_json


def test_template_content(tmp_path):
    path = write_descriptor_template("knee_flexion_angle", tmp_path)
    data = json.loads(path.read_text())
    assert data["type_name"] == "knee_flexion_angle"
    assert data["type_version"] == 1
    assert set(data["run"]) == {"case_uid", "code_ref", "parameters", "notes", "supersedes"}


def test_template_refuses_overwrite(tmp_path):
    write_descriptor_template("knee_flexion_angle", tmp_path)
    with pytest.raises(IntakeRefusal) as exc:
        write_descriptor_template("knee_flexion_angle", tmp_path)
    assert exc.value.friendly.recourse


def test_template_unknown_type(tmp_path):
    with pytest.raises(IntakeRefusal):
        write_descriptor_template("no_such_type", tmp_path)


def test_yaml_refused_without_parser(tmp_path, monkeypatch):
    import builtins
    real = builtins.__import__

    def no_yaml(name, *args, **kwargs):
        if name == "yaml":
            raise ImportError("no yaml reader")
        return real(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_yaml)
    (tmp_path / "analysis.yaml").write_text("type_name: knee_flexion_angle\n")
    with pytest.raises(IntakeRefusal) as exc:
        load_descriptor(tmp_path)
    assert exc.value.friendly.recourse


def test_yaml_accepted_with_injected_loader(tmp_path):
    (tmp_path / "analysis.yaml").write_text("anything")
    data = {"descriptor_version": "1", "type_name": "knee_flexion_angle", "type_version": 1,
            "run": {"case_uid": "CASEA", "code_ref": "git:abc"}}
    assert load_descriptor(tmp_path, yaml_loader=lambda text: data) == data


def test_missing_case_uid_refused_naming_field(tmp_path):
    write_analysis_json(tmp_path, case_uid="")
    with pytest.raises(IntakeRefusal) as exc:
        check_descriptor(load_descriptor(tmp_path), load_types())
    assert "case_uid" in exc.value.friendly.message


def test_unknown_run_key_refused(tmp_path):
    path = write_analysis_json(tmp_path)
    data = json.loads(path.read_text())
    data["run"]["patient"] = "x"
    path.write_text(json.dumps(data))
    with pytest.raises(IntakeRefusal):
        check_descriptor(load_descriptor(tmp_path), load_types())


def test_descriptor_schema_cross_check(tmp_path):
    jsonschema = pytest.importorskip("jsonschema")
    from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR
    schema = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "analysis-descriptor.schema.json").read_text())
    write_analysis_json(tmp_path)
    jsonschema.validate(load_descriptor(tmp_path), schema)
