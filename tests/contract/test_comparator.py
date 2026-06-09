"""
tests/contract/test_comparator.py — Unit tests for XnatStateComparator.

Covers:
  - identical states match
  - ID-only diff is ignored after normalization
  - file bytes diff fails
  - missing-resource fails
"""
from __future__ import annotations

import pytest

from tests.contract.comparator import XnatStateComparator, ComparisonResult


# ---------------------------------------------------------------------------
# Helpers: build minimal state dicts
# ---------------------------------------------------------------------------

def _base_state() -> dict:
    return {
        "project": "TEST",
        "subjects": {
            "SUBJ_001": {
                "experiments": {
                    "EXP_001": {
                        "scans": {
                            "0": {
                                "resources": {
                                    "SRC": {
                                        "files": {
                                            "frame_000.dcm": "abc123",
                                        }
                                    }
                                }
                            }
                        },
                        "assessors": {},
                    }
                }
            }
        },
    }


# ---------------------------------------------------------------------------
# T1 — identical states match
# ---------------------------------------------------------------------------

class TestIdenticalStatesMatch:

    def test_identical_states_equal(self):
        cmp = XnatStateComparator()
        state = _base_state()
        result = cmp.compare(state, state)
        assert result.equal
        result.assert_equal()  # must not raise

    def test_compare_returns_comparison_result(self):
        cmp = XnatStateComparator()
        state = _base_state()
        result = cmp.compare(state, state)
        assert isinstance(result, ComparisonResult)


# ---------------------------------------------------------------------------
# T2 — ID-only diff is ignored after normalization
# ---------------------------------------------------------------------------

class TestIdOnlyDiffIgnored:

    def test_id_field_stripped(self):
        cmp = XnatStateComparator()
        state_a = {
            "project": "TEST",
            "ID": "server-assigned-001",
            "subjects": {},
        }
        state_b = {
            "project": "TEST",
            "ID": "server-assigned-999",
            "subjects": {},
        }
        result = cmp.compare(state_a, state_b)
        assert result.equal, f"Expected equal after ID strip, got diffs: {result.diffs}"

    def test_insert_date_stripped(self):
        cmp = XnatStateComparator()
        state_a = {"project": "TEST", "insert_date": "2024-01-01", "subjects": {}}
        state_b = {"project": "TEST", "insert_date": "2026-06-01", "subjects": {}}
        result = cmp.compare(state_a, state_b)
        assert result.equal

    def test_uri_stripped(self):
        cmp = XnatStateComparator()
        state_a = {"project": "TEST", "URI": "/data/projects/A", "subjects": {}}
        state_b = {"project": "TEST", "URI": "/data/projects/B", "subjects": {}}
        result = cmp.compare(state_a, state_b)
        assert result.equal

    def test_xnat_datatype_id_stripped(self):
        cmp = XnatStateComparator()
        state_a = {"project": "TEST", "xnat_rfSessionData/id": "XNAT_S001", "subjects": {}}
        state_b = {"project": "TEST", "xnat_rfSessionData/id": "XNAT_S999", "subjects": {}}
        result = cmp.compare(state_a, state_b)
        assert result.equal

    def test_timestamp_suffix_stripped(self):
        cmp = XnatStateComparator()
        state_a = {"project": "TEST", "create_date": "2024-01-01T00:00:00", "subjects": {}}
        state_b = {"project": "TEST", "create_date": "2026-06-06T12:00:00", "subjects": {}}
        result = cmp.compare(state_a, state_b)
        assert result.equal

    def test_non_id_field_not_stripped(self):
        """A meaningful field like 'label' must NOT be stripped."""
        cmp = XnatStateComparator()
        norm = cmp.normalize({"label": "my-label", "ID": "should-strip"})
        assert "label" in norm
        assert "ID" not in norm


# ---------------------------------------------------------------------------
# T3 — file bytes diff fails
# ---------------------------------------------------------------------------

class TestFileBytesHashDiff:

    def test_different_file_hash_not_equal(self):
        cmp = XnatStateComparator()
        state_a = _base_state()
        state_b = _base_state()
        # mutate one file hash
        state_b["subjects"]["SUBJ_001"]["experiments"]["EXP_001"]["scans"]["0"]["resources"]["SRC"]["files"]["frame_000.dcm"] = "deadbeef"
        result = cmp.compare(state_a, state_b)
        assert not result.equal

    def test_different_file_hash_assert_equal_raises(self):
        cmp = XnatStateComparator()
        state_a = _base_state()
        state_b = _base_state()
        state_b["subjects"]["SUBJ_001"]["experiments"]["EXP_001"]["scans"]["0"]["resources"]["SRC"]["files"]["frame_000.dcm"] = "deadbeef"
        result = cmp.compare(state_a, state_b)
        with pytest.raises(AssertionError, match="parity mismatch"):
            result.assert_equal()


# ---------------------------------------------------------------------------
# T4 — missing resource fails
# ---------------------------------------------------------------------------

class TestMissingResourceFails:

    def test_extra_resource_in_fake_not_equal(self):
        cmp = XnatStateComparator()
        state_a = _base_state()
        state_b = _base_state()
        # real state is missing the SRC resource entirely
        del state_b["subjects"]["SUBJ_001"]["experiments"]["EXP_001"]["scans"]["0"]["resources"]["SRC"]
        result = cmp.compare(state_a, state_b)
        assert not result.equal

    def test_missing_resource_assert_equal_raises(self):
        cmp = XnatStateComparator()
        state_a = _base_state()
        state_b = _base_state()
        del state_b["subjects"]["SUBJ_001"]["experiments"]["EXP_001"]["scans"]["0"]["resources"]["SRC"]
        result = cmp.compare(state_a, state_b)
        with pytest.raises(AssertionError):
            result.assert_equal()

    def test_extra_file_in_real_not_equal(self):
        cmp = XnatStateComparator()
        state_a = _base_state()
        state_b = _base_state()
        # real has an extra file
        state_b["subjects"]["SUBJ_001"]["experiments"]["EXP_001"]["scans"]["0"]["resources"]["SRC"]["files"]["extra.dcm"] = "99ff00"
        result = cmp.compare(state_a, state_b)
        assert not result.equal
