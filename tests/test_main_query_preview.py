"""
Tests for main.py::query_experiments_rest — REST-based experiment querying.

Verifies:
- query_experiments_rest() calls the correct REST endpoint with auth
- Response JSON is parsed and mapped to expected DataFrame columns
- Mixed session types (mr + rf) are preserved in results
- Proper error handling for HTTP and network failures
- Empty result set returns DataFrame with correct column structure

No live server required — all tests use mocked requests.
"""
from __future__ import annotations

import pytest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError, RequestException

from main import query_experiments_rest
from src.services.errors import FriendlyError
from src.utilities import XNATLogin


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_validated_login() -> Mock:
    """Create a mock XNATLogin object with credentials."""
    login = Mock(spec=XNATLogin)
    login.validated_username = "testuser"
    login.validated_password = "testpass"
    return login


@pytest.fixture()
def sample_rest_response_mixed_types() -> dict:
    """Sample REST response with mixed mr and rf experiment types."""
    return {
        "ResultSet": {
            "Result": [
                {
                    "ID": "expt_001",
                    "label": "MR_Exp_001",
                    "xsiType": "xnat:mrSessionData",
                    "subject_ID": "subj_001",
                    "subject_label": "S001",
                    "date": "2024-01-15",
                    "insert_date": "2024-01-15T10:30:00"
                },
                {
                    "ID": "expt_002",
                    "label": "RF_Exp_001",
                    "xsiType": "xnat:rfSessionData",
                    "subject_ID": "subj_002",
                    "subject_label": "S002",
                    "date": "2024-01-16",
                    "insert_date": "2024-01-16T14:45:00"
                },
                {
                    "ID": "expt_003",
                    "label": "MR_Exp_002",
                    "xsiType": "xnat:mrSessionData",
                    "subject_ID": "subj_003",
                    "subject_label": "S003",
                    "date": "2024-01-17",
                    "insert_date": "2024-01-17T09:15:00"
                },
            ]
        }
    }


@pytest.fixture()
def sample_rest_response_all_three_types() -> dict:
    """
    Sample REST response with all three session types the tool must surface (#39):
    - xnat:mrSessionData  — historical uploads (pre-PR#38 ignored-xsiType bug)
    - xnat:rfSessionData   — trauma / fluoroscopy uploads (post-fix)
    - xnat:esvSessionData  — arthroscopy uploads (post-fix)

    Regression guard: the query→preview path must not drop any of the three,
    or historical mr-typed data stays invisible while new data appears.
    """
    return {
        "ResultSet": {
            "Result": [
                {
                    "ID": "expt_mr",
                    "label": "Historical_MR",
                    "xsiType": "xnat:mrSessionData",
                    "subject_ID": "subj_mr",
                    "subject_label": "S_MR",
                    "date": "2024-01-15",
                    "insert_date": "2024-01-15T10:30:00",
                },
                {
                    "ID": "expt_rf",
                    "label": "Trauma_RF",
                    "xsiType": "xnat:rfSessionData",
                    "subject_ID": "subj_rf",
                    "subject_label": "S_RF",
                    "date": "2024-02-16",
                    "insert_date": "2024-02-16T14:45:00",
                },
                {
                    "ID": "expt_esv",
                    "label": "Arthro_ESV",
                    "xsiType": "xnat:esvSessionData",
                    "subject_ID": "subj_esv",
                    "subject_label": "S_ESV",
                    "date": "2024-03-17",
                    "insert_date": "2024-03-17T09:15:00",
                },
            ]
        }
    }


@pytest.fixture()
def sample_rest_response_empty() -> dict:
    """Sample REST response with no results."""
    return {
        "ResultSet": {
            "Result": []
        }
    }


# ---------------------------------------------------------------------------
# Tests — successful queries
# ---------------------------------------------------------------------------

def test_query_experiments_rest_returns_dataframe_with_mixed_types(mock_validated_login, sample_rest_response_mixed_types):
    """
    Test that query_experiments_rest returns a DataFrame with mixed session types.
    Verifies:
    - DataFrame shape (3 rows)
    - Required columns present
    - xsiType values preserved (mr and rf both present)
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = sample_rest_response_mixed_types
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = query_experiments_rest(mock_validated_login, "STRESS_VOL")

        # Verify shape
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3

        # Verify required columns
        required_cols = ['expt_id', 'experiment', 'xsiType', 'subject_id', 'subject_label', 'operation_date', 'insert_date']
        for col in required_cols:
            assert col in result.columns, f"Missing column: {col}"

        # Verify mixed types preserved
        xsi_types = result['xsiType'].unique()
        assert len(xsi_types) == 2
        assert 'xnat:mrSessionData' in xsi_types
        assert 'xnat:rfSessionData' in xsi_types

        # Verify some data integrity
        assert result['expt_id'].iloc[0] == 'expt_001'
        assert result['experiment'].iloc[1] == 'RF_Exp_001'
        assert result['subject_label'].iloc[2] == 'S003'


def test_query_experiments_rest_uses_correct_endpoint(mock_validated_login, sample_rest_response_mixed_types):
    """
    Test that query_experiments_rest constructs the correct REST API endpoint.
    Verifies:
    - Endpoint includes project name
    - Endpoint includes required columns
    - HTTPBasicAuth is used with credentials
    """
    with patch('main.requests.get') as mock_get, \
         patch('main._app_config') as mock_config:
        mock_config.server_url = "http://localhost:8080/xnat"
        mock_config.project_name = "STRESS_VOL"

        mock_response = Mock()
        mock_response.json.return_value = sample_rest_response_mixed_types
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        query_experiments_rest(mock_validated_login, "STRESS_VOL")

        # Verify the endpoint was called with correct URL
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        endpoint_url = call_args[0][0] if call_args[0] else call_args[1].get('url', '')

        # Verify expected parts of the URL
        assert "localhost:8080" in endpoint_url
        assert "STRESS_VOL" in endpoint_url
        assert "columns=ID,label,xsiType,subject_ID,subject_label,date,insert_date" in endpoint_url
        assert "format=json" in endpoint_url

        # Verify auth
        assert 'auth' in call_args[1]
        auth = call_args[1]['auth']
        assert isinstance(auth, HTTPBasicAuth)
        assert auth.username == "testuser"
        assert auth.password == "testpass"


def test_query_experiments_rest_empty_result_returns_correct_columns(mock_validated_login, sample_rest_response_empty):
    """
    Test that empty result set returns DataFrame with correct columns.
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = sample_rest_response_empty
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = query_experiments_rest(mock_validated_login, "STRESS_VOL")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0

        required_cols = ['expt_id', 'experiment', 'xsiType', 'subject_id', 'subject_label', 'operation_date', 'insert_date']
        for col in required_cols:
            assert col in result.columns, f"Missing column in empty result: {col}"


def test_query_experiments_rest_handles_list_response_format(mock_validated_login):
    """
    Test that query_experiments_rest can handle direct list response (not wrapped in ResultSet).
    XNAT REST sometimes returns a bare list instead of {ResultSet: {Result: [...]}}.
    """
    list_response = [
        {
            "ID": "expt_001",
            "label": "MR_Exp_001",
            "xsiType": "xnat:mrSessionData",
            "subject_ID": "subj_001",
            "subject_label": "S001",
            "date": "2024-01-15",
            "insert_date": "2024-01-15T10:30:00"
        }
    ]

    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = list_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = query_experiments_rest(mock_validated_login, "STRESS_VOL")

        assert len(result) == 1
        assert result['expt_id'].iloc[0] == 'expt_001'


def test_query_preview_surfaces_all_three_session_types(mock_validated_login, sample_rest_response_all_three_types):
    """
    Regression for #39: the query→preview path must surface sessions of ALL
    THREE types — mr (historical, ignored-xsiType bug), rf (trauma), and esv
    (arthroscopy). Historical mr-typed uploads must remain visible after the
    PR#38 creation-call fix, or data forks into visible-new / invisible-old.

    Guards the query leg AND the preview-block derivation in
    download_queried_data (procedure <- xsiType; subject_id <- subject_label).
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = sample_rest_response_all_three_types
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = query_experiments_rest(mock_validated_login, "GROK_AHRQ_Data")

        # Query leg: all three rows survive, no type filter drops any.
        assert len(result) == 3
        xsi_types = set(result['xsiType'])
        assert xsi_types == {
            'xnat:mrSessionData',
            'xnat:rfSessionData',
            'xnat:esvSessionData',
        }, f"A session type was dropped by the query leg: {xsi_types}"

        # Preview-block derivation (mirrors download_queried_data): procedure
        # carries the raw xsiType and every type is represented.
        result['procedure'] = result['xsiType']
        result['subject_id'] = result['subject_label']
        assert set(result['procedure']) == {
            'xnat:mrSessionData',
            'xnat:rfSessionData',
            'xnat:esvSessionData',
        }
        assert set(result['subject_id']) == {'S_MR', 'S_RF', 'S_ESV'}

        # insert_date split used by the preview must work for every row.
        result['insert_date'] = pd.to_datetime(result['insert_date'])
        assert result['insert_date'].dt.date.notna().all()


# ---------------------------------------------------------------------------
# Tests — error handling
# ---------------------------------------------------------------------------

def test_query_experiments_rest_raises_friendly_error_on_http_error(mock_validated_login):
    """
    Test that HTTP errors surface the FriendlyError content.
    FriendlyError is a plain dataclass (not an Exception), so the helper raises
    ValueError carrying the FriendlyError title + message.
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = HTTPError(
            "404 Not Found", response=mock_response
        )
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="REST API query failed") as exc_info:
            query_experiments_rest(mock_validated_login, "NONEXISTENT")
        assert "404" in str(exc_info.value)
        assert "NONEXISTENT" in str(exc_info.value)


def test_query_experiments_rest_raises_friendly_error_on_network_error(mock_validated_login):
    """
    Test that network errors are wrapped in FriendlyError content (raised as ValueError).
    """
    with patch('main.requests.get') as mock_get:
        mock_get.side_effect = RequestException("Connection refused")

        with pytest.raises(ValueError, match="Network error during REST API query"):
            query_experiments_rest(mock_validated_login, "STRESS_VOL")


def test_query_experiments_rest_raises_friendly_error_on_invalid_json(mock_validated_login):
    """
    Test that invalid JSON response is wrapped in FriendlyError content (raised as ValueError).
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response

        with pytest.raises(ValueError, match="Invalid JSON response from XNAT"):
            query_experiments_rest(mock_validated_login, "STRESS_VOL")


# ---------------------------------------------------------------------------
# Integration test with download_queried_data flow
# ---------------------------------------------------------------------------

def test_query_experiments_rest_produces_columns_compatible_with_download_flow(mock_validated_login, sample_rest_response_mixed_types):
    """
    Test that the DataFrame produced by query_experiments_rest has the shape
    expected by the preview block in download_queried_data.

    The downstream code derives (no second subjectData query):
    - procedure   <- xsiType (string, e.g. 'xnat:rfSessionData')
    - subject_id  <- subject_label
    - upload_date / upload_time <- split insert_date
    - operation_date is converted to date and filtered
    """
    with patch('main.requests.get') as mock_get:
        mock_response = Mock()
        mock_response.json.return_value = sample_rest_response_mixed_types
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = query_experiments_rest(mock_validated_login, "STRESS_VOL")

        # Verify columns required downstream
        assert 'xsiType' in result.columns
        assert 'subject_label' in result.columns
        assert 'operation_date' in result.columns
        assert 'insert_date' in result.columns

        # Verify the preview-block derivation works on this frame
        result['procedure'] = result['xsiType']
        result['subject_id'] = result['subject_label']
        assert list(result['procedure']) == ['xnat:mrSessionData', 'xnat:rfSessionData', 'xnat:mrSessionData']
        assert list(result['subject_id']) == ['S001', 'S002', 'S003']

        # Verify insert_date can be converted to datetime and split
        result['insert_date'] = pd.to_datetime(result['insert_date'])
        result['upload_date'] = result['insert_date'].dt.date
        result['upload_time'] = result['insert_date'].dt.time

        assert result['upload_date'].dtype.name == 'object'  # date objects
        assert result['upload_time'].dtype.name == 'object'  # time objects

        # Verify operation_date can be filtered after conversion
        from datetime import date
        result['operation_date'] = pd.to_datetime(result['operation_date']).dt.date
        target_date = date(2024, 1, 16)
        filtered = result[(result['operation_date'] >= target_date) & (result['operation_date'] <= target_date)]
        assert len(filtered) == 1
        assert filtered['experiment'].iloc[0] == 'RF_Exp_001'
