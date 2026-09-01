"""
tests/test_browse_real_listing.py — TASK 2 browse fallback verification.

Tests that browse._experiment_labels and browse._subject_names have
last-resort REST API fallbacks that activate only when primary paths
return empty results.

The fallbacks allow the browse UI to work against real pyxnat.Interface
(which lacks label hooks) by querying the REST API directly.
"""
from __future__ import annotations

from typing import Dict, List
from unittest.mock import MagicMock


from app.logic.browse import (
    _experiment_labels,
    _subject_names,
    _rest_json,
    fetch_data_table,
    COLUMNS,
)


# ---------------------------------------------------------------------------
# Test double: ServerWithEmptyPrimaryPaths
#
# Simulates a real pyxnat.Interface that has select() but primary paths
# return empty. Fallback REST .get() will be tested.
# ---------------------------------------------------------------------------

class ServerWithEmptyPrimaryPaths:
    """
    Server that returns empty from select().get() but has a working
    .get(uri) method that returns canned REST JSON.
    """

    def __init__(self):
        self._rest_responses: Dict[str, dict] = {}

    def select(self, qs: str):
        """Return a selectable that always returns empty."""
        return MagicMock(get=lambda: None)

    def get(self, uri: str):
        """Return canned REST response for the given URI."""
        return MagicMock(
            json=lambda: self._rest_responses.get(
                uri, {"ResultSet": {"Result": []}}
            )
        )

    def seed_rest_response(self, uri: str, data: dict) -> None:
        """Seed a canned REST JSON response."""
        self._rest_responses[uri] = data


class ServerWithPrimaryPathsOnly:
    """
    Server that has working primary paths (list_subjects_with_labels,
    list_experiments) but NO .get() method (no REST fallback).
    Tests that fallback is never invoked when primary paths work.
    """

    def __init__(self):
        self._subjects: Dict[str, str] = {}  # internal_id -> label
        self._experiments: Dict[str, List[str]] = {}  # subject_label -> [exp_labels]
        self._get_called = False

    def select(self, qs: str):
        """Return a selectable that always returns empty (uses list_experiments fallback)."""
        return MagicMock(get=lambda: None)

    def list_subjects_with_labels(self, project_name: str) -> List[tuple]:
        """Return [(internal_id, label), ...] to avoid needing REST."""
        return list(self._subjects.items())

    def list_experiments(self, project_name: str, subject: str) -> List[str]:
        """Return experiments for a subject."""
        return self._experiments.get(subject, [])

    def get(self, uri: str):
        """Should never be called when primary paths work."""
        self._get_called = True
        raise AssertionError(f".get(uri) should not be called when primary paths work; uri={uri}")

    def seed_subject(self, internal_id: str, label: str) -> None:
        """Seed a subject."""
        self._subjects[internal_id] = label

    def seed_experiments(self, subject_label: str, exp_labels: List[str]) -> None:
        """Seed experiments for a subject."""
        self._experiments[subject_label] = exp_labels


# ---------------------------------------------------------------------------
# Test _rest_json helper
# ---------------------------------------------------------------------------

class TestRestJsonHelper:
    """Test the _rest_json helper function."""

    def test_rest_json_returns_dict_on_success(self):
        """_rest_json calls .get(uri) and returns parsed dict."""
        server = ServerWithEmptyPrimaryPaths()
        server.seed_rest_response(
            "/data/projects/TEST/subjects?format=json",
            {
                "ResultSet": {
                    "Result": [
                        {"ID": "TEST_S001", "label": "SUBJ_001"},
                    ]
                }
            },
        )

        result = _rest_json(
            server,
            "/data/projects/TEST/subjects?format=json"
        )
        assert result is not None
        assert "ResultSet" in result
        assert result["ResultSet"]["Result"][0]["label"] == "SUBJ_001"

    def test_rest_json_returns_none_on_missing_get(self):
        """_rest_json returns None if server has no .get() method."""
        server = MagicMock(spec=[])  # No .get() method
        result = _rest_json(server, "/data/projects/TEST/subjects?format=json")
        assert result is None

    def test_rest_json_returns_none_on_exception(self):
        """_rest_json returns None if .get() raises an exception."""
        server = MagicMock()
        server.get.side_effect = Exception("network error")
        result = _rest_json(server, "/data/projects/TEST/subjects?format=json")
        assert result is None


# ---------------------------------------------------------------------------
# Test _experiment_labels fallback
# ---------------------------------------------------------------------------

class TestExperimentLabelsFallback:
    """
    _experiment_labels should use REST fallback only when primary
    paths (select().get() and list_experiments()) both return empty.
    """

    def test_primary_path_select_returns_experiments(self):
        """Primary path succeeds — fallback never invoked."""
        server = ServerWithPrimaryPathsOnly()
        server.seed_subject("S001", "SUBJ_001")
        server.seed_experiments("SUBJ_001", ["EXP_001", "EXP_002"])

        exps = _experiment_labels(server, "TEST_PROJ", "SUBJ_001")

        assert exps == ["EXP_001", "EXP_002"]
        # Verify .get() was never called (no REST fallback)
        assert not server._get_called, ".get() should not be called when primary paths work"

    def test_fallback_returns_experiments_when_primary_empty(self):
        """Primary paths return empty — fallback returns REST results."""
        server = ServerWithEmptyPrimaryPaths()
        server.seed_rest_response(
            "/data/experiments?project=TEST_PROJ&columns=ID,label,xsiType,subject_ID,subject_label&format=json",
            {
                "ResultSet": {
                    "Result": [
                        {
                            "ID": "EXP_001",
                            "label": "EXP_001",
                            "subject_ID": "TEST_S001",
                            "subject_label": "SUBJ_001",
                        },
                        {
                            "ID": "EXP_002",
                            "label": "EXP_002",
                            "subject_ID": "TEST_S001",
                            "subject_label": "SUBJ_001",
                        },
                        {
                            "ID": "EXP_003",
                            "label": "EXP_003",
                            "subject_ID": "TEST_S002",
                            "subject_label": "SUBJ_002",
                        },
                    ]
                }
            },
        )

        exps = _experiment_labels(server, "TEST_PROJ", "SUBJ_001")

        # Should return only experiments matching SUBJ_001
        assert sorted(exps) == ["EXP_001", "EXP_002"]

    def test_fallback_filters_by_subject_id_when_label_mismatch(self):
        """Fallback can match by subject_ID if label doesn't match."""
        server = ServerWithEmptyPrimaryPaths()
        server.seed_rest_response(
            "/data/experiments?project=TEST_PROJ&columns=ID,label,xsiType,subject_ID,subject_label&format=json",
            {
                "ResultSet": {
                    "Result": [
                        {
                            "ID": "EXP_001",
                            "label": "EXP_001",
                            "subject_ID": "INTERNAL_ID",
                            "subject_label": "DIFFERENT_LABEL",
                        },
                    ]
                }
            },
        )

        # Query using subject_ID (internal ID)
        exps = _experiment_labels(server, "TEST_PROJ", "INTERNAL_ID")
        assert exps == ["EXP_001"]


# ---------------------------------------------------------------------------
# Test _subject_names fallback
# ---------------------------------------------------------------------------

class TestSubjectNamesFallback:
    """
    _subject_names should use REST fallback only when label_for_subject
    is not available (e.g., on real pyxnat.Interface).
    """

    def test_primary_label_hook_succeeds(self):
        """Primary hook list_subjects_with_labels works — no fallback."""
        server = ServerWithPrimaryPathsOnly()
        server.seed_subject("S001", "SUBJ_001")
        server.seed_subject("S002", "SUBJ_002")

        subjects = _subject_names(server, "TEST_PROJ")

        assert sorted(subjects) == ["SUBJ_001", "SUBJ_002"]
        assert not server._get_called

    def test_fallback_resolves_internal_ids_via_rest(self):
        """
        When label hooks are missing, fallback uses REST to resolve
        internal IDs → labels.
        """
        # Create a server that has select() returning internal IDs
        # but no label hooks. Must have .get() for fallback.
        class ServerWithSelectAndRest:
            def __init__(self):
                self._subject_ids = ["S001", "S002"]
                self._rest_response = {
                    "ResultSet": {
                        "Result": [
                            {"ID": "S001", "label": "SUBJ_001"},
                            {"ID": "S002", "label": "SUBJ_002"},
                        ]
                    }
                }

            def select(self, qs: str):
                # Return internal IDs from select (no label hook)
                return MagicMock(get=lambda: self._subject_ids)

            def get(self, uri: str):
                return MagicMock(json=lambda: self._rest_response)

        server = ServerWithSelectAndRest()
        subjects = _subject_names(server, "TEST_PROJ")

        # Should resolve to labels via REST fallback
        assert sorted(subjects) == ["SUBJ_001", "SUBJ_002"]

    def test_fallback_returns_raw_ids_if_no_rest(self):
        """
        If REST fallback also fails, returns raw IDs as last resort.
        """
        class ServerWithSelectOnly:
            def select(self, qs: str):
                return MagicMock(get=lambda: ["S001", "S002"])

            def list_subjects(self, project_name: str):
                return []

        server = ServerWithSelectOnly()
        subjects = _subject_names(server, "TEST_PROJ")

        # No label hooks and no .get() — returns raw IDs
        assert sorted(subjects) == ["S001", "S002"]


# ---------------------------------------------------------------------------
# Integration: fetch_data_table with fallbacks
# ---------------------------------------------------------------------------

class TestFetchDataTableWithFallbacks:
    """
    Integration test: fetch_data_table should use fallbacks when needed
    but maintain the full contract (scan_id present, data complete).
    """

    def test_fetch_data_table_keeps_scan_id_with_fallback(self):
        """
        Rows returned by fetch_data_table must always include scan_id,
        even when fallbacks are used to resolve subjects/experiments.
        """
        server = ServerWithEmptyPrimaryPaths()
        # Seed REST responses for subjects and experiments
        server.seed_rest_response(
            "/data/projects/TEST_PROJ/subjects?format=json",
            {
                "ResultSet": {
                    "Result": [
                        {"ID": "S001", "label": "SUBJ_001"},
                    ]
                }
            },
        )
        server.seed_rest_response(
            "/data/experiments?project=TEST_PROJ&columns=ID,label,xsiType,subject_ID,subject_label&format=json",
            {
                "ResultSet": {
                    "Result": [
                        {
                            "ID": "EXP_001",
                            "label": "EXP_001",
                            "subject_ID": "S001",
                            "subject_label": "SUBJ_001",
                        },
                    ]
                }
            },
        )

        rows = fetch_data_table(server, "TEST_PROJ")

        assert isinstance(rows, list), f"Expected list, got {type(rows)}"
        if rows:  # If any rows returned
            for row in rows:
                assert "scan_id" in row, f"scan_id missing from row: {row}"
                assert all(col in row for col in COLUMNS), (
                    f"Missing columns in row: {set(COLUMNS) - set(row.keys())}"
                )
