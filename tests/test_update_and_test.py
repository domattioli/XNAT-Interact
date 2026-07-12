"""
Offline unit tests for update_and_test.py's requirements parsing + import-name
resolution (issue #10).  Pure logic — no network, no server, no heavy deps.
"""
import importlib.util
from pathlib import Path

import pytest

# Load update_and_test.py from repo root without a package install.
_ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("update_and_test", _ROOT / "update_and_test.py")
uat = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(uat)


class TestParseRequirementLine:
    def test_full_line_comment_skipped(self):
        assert uat._parse_requirement_line("# streamlit: the UI framework") is None

    def test_blank_line_skipped(self):
        assert uat._parse_requirement_line("   \n") is None

    def test_inline_comment_stripped(self):
        assert uat._parse_requirement_line("cffi  # CFFI backend; pin explicitly") == "cffi"

    def test_option_line_skipped(self):
        assert uat._parse_requirement_line("-r requirements-dev.txt") is None
        assert uat._parse_requirement_line("-e .") is None

    def test_plain_requirement_kept(self):
        assert uat._parse_requirement_line("numpy") == "numpy"

    def test_versioned_requirement_kept(self):
        assert uat._parse_requirement_line("SQLAlchemy>=2.0  # ORM") == "SQLAlchemy>=2.0"


class TestDistName:
    @pytest.mark.parametrize("spec,expected", [
        ("SQLAlchemy>=2.0", "SQLAlchemy"),
        ("opencv-python", "opencv-python"),
        ("psycopg[binary]", "psycopg"),
        ("numpy==1.26.4", "numpy"),
        ("package==1.2; python_version<'3.10'", "package"),
        ("pkg~=1.0", "pkg"),
    ])
    def test_dist_name(self, spec, expected):
        assert uat._dist_name(spec) == expected


class TestResolveImportName:
    def test_metadata_map_wins(self):
        dist_map = {"opencv-python": ["cv2"]}
        assert uat.resolve_import_name("opencv-python", dist_map) == "cv2"

    def test_prefers_module_matching_normalized_name(self):
        dist_map = {"sqlalchemy": ["sqlalchemy", "sqlalchemy_ext"]}
        assert uat.resolve_import_name("SQLAlchemy", dist_map) == "sqlalchemy"

    def test_fallback_alias_when_not_in_map(self):
        assert uat.resolve_import_name("python-dateutil", {}) == "dateutil"
        assert uat.resolve_import_name("fonttools", {}) == "fontTools"
        assert uat.resolve_import_name("charset-normalizer", {}) == "charset_normalizer"

    def test_fallback_normalizes_hyphen_when_unknown(self):
        assert uat.resolve_import_name("some-new-pkg", {}) == "some_new_pkg"

    def test_sqlalchemy_case_regression(self):
        # issue #10: old hardcoded map imported 'SQLAlchemy' verbatim ->
        # ModuleNotFoundError though installed. Resolver yields real module name.
        dist_map = {"sqlalchemy": ["sqlalchemy"]}
        assert uat.resolve_import_name("SQLAlchemy", dist_map) == "sqlalchemy"
