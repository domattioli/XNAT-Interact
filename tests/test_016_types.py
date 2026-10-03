"""Spec 016 US3: analysis types are reviewed files that load and validate."""
import json
import shutil
from pathlib import Path

import pytest

from src.services.analysis_intake import IntakeRefusal, load_types, publish_analysis
from src.services.analysis_intake.types import ANALYSIS_TYPES_DIR
from src.services import xnat_conventions as conventions
from src.annotations.io_xnat import MANIFEST_FILENAME, _blob_filename
from src.services.analysis_intake.gates import spec_for


def test_registered_types_load():
    types = load_types()
    assert {"knee_flexion_angle", "annotations", "segmentation_consensus"} <= set(types)


def test_type_files_match_meta_schema():
    jsonschema = pytest.importorskip("jsonschema")
    meta = json.loads((ANALYSIS_TYPES_DIR / "schemas" / "analysis-type.schema.json").read_text())
    for path in ANALYSIS_TYPES_DIR.glob("*.json"):
        jsonschema.validate(json.loads(path.read_text()), meta)


def test_contract_schemas_copied_byte_for_byte():
    contracts = Path(__file__).resolve().parents[1] / "specs" / "016-analysis-intake" / "contracts"
    if not contracts.is_dir():
        pytest.skip("spec folder moved to DomI")
    for name in ("analysis-type.schema.json", "analysis-descriptor.schema.json"):
        assert (contracts / name).read_bytes() == (ANALYSIS_TYPES_DIR / "schemas" / name).read_bytes()


def _copy_types(tmp_path):
    folder = tmp_path / "types"
    shutil.copytree(ANALYSIS_TYPES_DIR, folder)
    return folder


def test_broken_type_refused_naming_field(tmp_path):
    folder = _copy_types(tmp_path)
    path = folder / "knee_flexion_angle.json"
    data = json.loads(path.read_text())
    data["placement"] = "somewhere"
    path.write_text(json.dumps(data))
    with pytest.raises(IntakeRefusal) as exc:
        load_types(folder)
    assert "placement" in exc.value.friendly.message
    assert exc.value.friendly.recourse


def test_type_file_name_must_match_type_name(tmp_path):
    folder = _copy_types(tmp_path)
    shutil.move(folder / "annotations.json", folder / "notes.json")
    with pytest.raises(IntakeRefusal):
        load_types(folder)


def test_annotations_patterns_match_upload_names():
    atype = load_types()["annotations"]
    assert spec_for(atype, MANIFEST_FILENAME) is not None
    assert spec_for(atype, _blob_filename("anno_x", "mask", 1, "rle")) is not None
    assert spec_for(atype, _blob_filename("anno_x", "score", 2, "json_scalar")) is not None
    assert atype.resource_label == conventions.ResourceLabel.ANNOTATIONS


def test_consensus_label_matches_production():
    atype = load_types()["segmentation_consensus"]
    assert atype.resource_label == conventions.ResourceLabel.SEGMENTATION_CONSENSUS
    assert atype.base_label("1.2.3") == conventions.consensus_label("1.2.3")


def test_publish_refuses_annotations(tmp_path):
    atype = load_types()["annotations"]
    desc = {"type_name": "annotations", "type_version": 1, "run": {"case_uid": "CASEA"}}
    with pytest.raises(IntakeRefusal) as exc:
        publish_analysis(desc, tmp_path, gateway=None, atype=atype, files=[])
    assert exc.value.friendly.recourse
