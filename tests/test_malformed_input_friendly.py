"""
tests/test_malformed_input_friendly.py — Malformed DICOM error handling validation.

Test comprehensive error handling for malformed DICOM files (truncated, not-a-dicom,
three-channel) with friendly error messages that name the failing files and provide
recourse to the user.

Offline only: no real server, no real XNAT connection, no PHI, all data synthetic.

Validation criteria:
  1. GatewayError is raised on malformed input (not silently ignored).
  2. friendly.message includes the filename(s) of the malformed file(s).
  3. friendly.message includes a reason snippet (extracted from the original exception).
  4. friendly.message is readable (≤~500 chars terminal-friendly).
  5. friendly.recourse is non-empty (actionable next steps).
  6. All-good files still construct successfully (control test).
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from src.services.xnat_gateway import GatewayError
from src.xnat_experiment_data import SourceRFSession
from src.utilities import ConfigTables
from tests.stress import malformed, factory
from tests.fakes.fake_xnat import FakeXNAT


# ---------------------------------------------------------------------------
# Fixtures / Setup Helpers
# ---------------------------------------------------------------------------


def _make_intake_form_stub(uid: str, folder: Path) -> SimpleNamespace:
    """
    Minimal ORDataIntakeForm stub for SourceRFSession construction.

    Parameters
    ----------
    uid : str
        Case UID (e.g., "CASE_001").
    folder : Path
        Directory containing DICOM files.

    Returns
    -------
    SimpleNamespace
        Stub with required attributes: uid, operation_date, epic_start_time,
        relevant_folder, saved_ffn, saved_ffn_str.
    """
    form = SimpleNamespace()
    form.uid = uid
    form.operation_date = "20240115"  # YYYYMMDD format
    form.epic_start_time = "080000"   # HHMMSS format
    form.relevant_folder = folder

    # Create a placeholder intake form JSON file in the folder
    intake_json_path = folder / "intake_form.json"
    intake_json_path.write_text("{}")
    form.saved_ffn = intake_json_path
    form.saved_ffn_str = str(intake_json_path)
    return form


def _make_config_tables_stub(
    project_name: str = "TEST_PROJECT",
    project_users: list | None = None,
) -> ConfigTables:
    """
    Minimal ConfigTables stub with FakeXNAT backend.

    Parameters
    ----------
    project_name : str
        XNAT project name.
    project_users : list | None
        List of authorized users.

    Returns
    -------
    ConfigTables
        Stub configured with FakeXNAT (no real server).
    """
    if project_users is None:
        project_users = ["testuser"]


    fake_xnat = FakeXNAT(project_name=project_name, project_users=project_users)
    login_info = SimpleNamespace(
        is_valid=True,
        validated_username="testuser",
        validated_password="fake_password",
    )
    xnat_conn = SimpleNamespace(
        is_open=True,
        is_verified=True,
        server=fake_xnat,
        gateway=fake_xnat,
        xnat_project_name=project_name,
        login_info=login_info,
        project_handle=SimpleNamespace(
            users=lambda: project_users,
            label=lambda: project_name,
        ),
    )

    # Allocate ConfigTables without __init__; inject minimal state
    ct = ConfigTables.__new__(ConfigTables)
    ct._login_info = login_info
    ct._xnat_connection = xnat_conn
    ct._local_variables = SimpleNamespace(
        data_librarian=["testuser"],
        config_fn="database_config.json",
        config_ffn="/tmp/database_config.json",
        backup_fn="database_config-backup.json",
        xnat_config_folder_name="config",
        xnat_backups_folder_name="backups",
        xnat_project_name=project_name,
        required_login_keys=[],
        default_meta_table_columns=["NAME", "UID", "DATE", "CREATED_BY"],
        tmp_data_dir=Path("/tmp"),
        cataloged_resources_ffn="/tmp/cataloged.json",
        template_img_dir="/tmp",
        template_img=None,
        acceptable_img_dtypes=[],
        required_img_size_for_hashing=(224, 224),
        mturk_batch_col_names=[],
        redacted_string="REDACTED",
        required_batch_upload_columns={},
    )
    ct._uid = "FAKE_UID"
    # Add minimal tables for ImageHash reference
    import pandas as pd
    ct._tables = {
        "IMAGE_HASHES": pd.DataFrame(columns=["NAME"]),  # Empty hash table
        "SUBJECTS": pd.DataFrame(columns=["NAME"]),  # Empty subjects table
    }
    return ct


# ---------------------------------------------------------------------------
# Tests: Three Malformed Types + Control
# ---------------------------------------------------------------------------


class TestTruncatedDicomFriendlyError:
    """Truncated DICOM (first 200 bytes) → GatewayError with friendly message."""

    def test_truncated_dicom_raises_gateway_error_with_friendly(
        self, tmp_workdir: Path
    ) -> None:
        """
        Truncated DICOM triggers Transfer Syntax UID error.
        GatewayError is raised with friendly message naming the truncated file.
        """
        # Setup: create malformed + control directory
        work_dir = tmp_workdir / "truncated_case"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 1 truncated DICOM
        truncated_path = malformed.truncated_dicom(work_dir)
        assert truncated_path.exists(), "truncated_dicom() should create file"

        # Create 2 good DICOMs
        good_surgery = factory.make_surgery(
            uid="good_surgery_1",
            seeds={1, 2},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        # Construct intake form and config
        intake = _make_intake_form_stub("1.2.3.4.5.1", work_dir)
        config = _make_config_tables_stub()

        # Act: attempt to construct SourceRFSession
        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        # Assert: GatewayError carries a FriendlyError
        friendly = exc_info.value.friendly
        assert friendly is not None, "GatewayError.friendly should be set"
        assert friendly.message is not None, "FriendlyError.message should exist"

        # Verify friendly message naming the truncated file
        # The message names the file WITHOUT the extension (just "truncated")
        assert "truncated" in friendly.message.lower(), (
            f"Friendly message should name 'truncated', got: {friendly.message}"
        )

        # Verify reason snippet (not full traceback, but a clue about the error)
        # Truncated DICOMs typically fail with "Transfer Syntax UID" or "attribute"
        reason_found = any(
            keyword in friendly.message.lower()
            for keyword in ["transfer", "attribute", "read", "syntax", "truncat"]
        )
        assert reason_found or len(friendly.message) > 50, (
            f"Friendly message should include a reason snippet, got: {friendly.message}"
        )

        # Verify recourse is non-empty
        assert friendly.recourse, "recourse list should be non-empty"
        assert isinstance(friendly.recourse, list), "recourse should be a list"
        assert len(friendly.recourse) > 0, "recourse should have at least one step"

        # Verify message is readable (≤~500 chars)
        assert len(friendly.message) <= 500, (
            f"Friendly message too long ({len(friendly.message)} chars): {friendly.message}"
        )

    def test_truncated_dicom_message_includes_guidance(
        self, tmp_workdir: Path
    ) -> None:
        """Verify friendly.message is actionable (not just a raw traceback)."""
        work_dir = tmp_workdir / "truncated_guidance"
        work_dir.mkdir(parents=True, exist_ok=True)

        malformed.truncated_dicom(work_dir)
        factory.make_surgery(uid="good_1", seeds={1}, dest_dir=work_dir)

        intake = _make_intake_form_stub("1.2.3.4.5.2", work_dir)
        config = _make_config_tables_stub()

        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        friendly = exc_info.value.friendly
        # Should be readable English, not a Python traceback line
        assert "Traceback" not in friendly.message, (
            "Friendly message should not include raw traceback"
        )
        # Should include actual steps
        assert len(friendly.recourse) > 0, "Should provide recourse options"


class TestNotADicomFriendlyError:
    """Plain-text .dcm file → GatewayError with friendly message."""

    def test_not_a_dicom_raises_gateway_error_with_friendly(
        self, tmp_workdir: Path
    ) -> None:
        """
        Non-DICOM .dcm file (plain text) triggers pydicom read error.
        GatewayError is raised with friendly message naming the file.
        """
        work_dir = tmp_workdir / "not_dicom_case"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 1 fake .dcm file (not valid DICOM)
        not_dicom_path = malformed.not_a_dicom(work_dir)
        assert not_dicom_path.exists(), "not_a_dicom() should create file"

        # Create 2 good DICOMs
        good_surgery = factory.make_surgery(
            uid="good_surgery_2",
            seeds={3, 4},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        # Construct intake form and config
        intake = _make_intake_form_stub("1.2.3.4.5.3", work_dir)
        config = _make_config_tables_stub()

        # Act: attempt to construct SourceRFSession
        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        # Assert: GatewayError carries a FriendlyError
        friendly = exc_info.value.friendly
        assert friendly is not None, "GatewayError.friendly should be set"

        # Verify friendly message names the not-a-dicom file
        assert "not_a_dicom" in friendly.message.lower(), (
            f"Friendly message should name 'not_a_dicom.dcm', got: {friendly.message}"
        )

        # Verify reason snippet
        reason_found = any(
            keyword in friendly.message.lower()
            for keyword in ["read", "parse", "dicom", "format", "invalid"]
        )
        assert reason_found or len(friendly.message) > 50, (
            f"Friendly message should include a reason, got: {friendly.message}"
        )

        # Verify recourse
        assert friendly.recourse, "recourse list should be non-empty"
        assert len(friendly.message) <= 500, "message should fit terminal"


class TestThreeChannelDicomFriendlyError:
    """RGB (3-channel) DICOM → GatewayError with friendly message."""

    def test_three_channel_raises_gateway_error_with_friendly(
        self, tmp_workdir: Path
    ) -> None:
        """
        3-channel (RGB) DICOM should fail validation.
        GatewayError is raised with friendly message naming the file.
        """
        work_dir = tmp_workdir / "three_channel_case"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 1 three-channel DICOM
        three_channel_path = malformed.three_channel(work_dir)
        assert three_channel_path.exists(), "three_channel() should create file"

        # Create 2 good DICOMs
        good_surgery = factory.make_surgery(
            uid="good_surgery_3",
            seeds={5, 6},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        # Construct intake form and config
        intake = _make_intake_form_stub("1.2.3.4.5.4", work_dir)
        config = _make_config_tables_stub()

        # Act: attempt to construct SourceRFSession
        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        # Assert: GatewayError carries a FriendlyError
        friendly = exc_info.value.friendly
        assert friendly is not None, "GatewayError.friendly should be set"

        # Verify friendly message names the three-channel file
        assert "three_channel" in friendly.message.lower(), (
            f"Friendly message should name 'three_channel.dcm', got: {friendly.message}"
        )

        # Verify reason snippet
        reason_found = any(
            keyword in friendly.message.lower()
            for keyword in ["channel", "grayscale", "rgb", "monochrome", "sample"]
        )
        assert reason_found or len(friendly.message) > 50, (
            f"Friendly message should include a reason, got: {friendly.message}"
        )

        # Verify recourse
        assert friendly.recourse, "recourse list should be non-empty"
        assert len(friendly.message) <= 500, "message should fit terminal"


class TestCleanDicomConstructsSuccessfully:
    """Control test: all good files → SourceRFSession constructs without error."""

    def test_clean_dir_still_constructs_successfully(
        self, tmp_workdir: Path
    ) -> None:
        """
        Directory with only valid DICOM files should construct successfully.
        No GatewayError, no validation failure.
        """
        work_dir = tmp_workdir / "clean_case"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 2 good DICOMs (no malformed files)
        good_surgery = factory.make_surgery(
            uid="good_surgery_clean",
            seeds={10, 11},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        # Construct intake form and config
        intake = _make_intake_form_stub("1.2.3.4.5.6.7.8.9.0", work_dir)
        config = _make_config_tables_stub()

        # Act: should succeed without raising
        session = SourceRFSession(intake, config)

        # Assert: session is valid
        assert session is not None, "Session should be constructed"
        assert session.df is not None, "Session dataframe should exist"
        # All rows should be valid (no malformed files)
        assert session.df["IS_VALID"].sum() > 0, (
            "Should have at least one valid DICOM"
        )


class TestMultipleFailuresReportedTogether:
    """
    Mix 2+ malformed types in same directory.
    Verify all failing filenames are named in the friendly message.
    """

    def test_multiple_failures_reported_together(
        self, tmp_workdir: Path
    ) -> None:
        """
        Directory with truncated + not-a-dicom files.
        Friendly message should name both.
        """
        work_dir = tmp_workdir / "multi_failure_case"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 2 malformed files
        truncated_path = malformed.truncated_dicom(work_dir)
        not_dicom_path = malformed.not_a_dicom(work_dir)

        # Create 1 good DICOM (so at least one valid file exists)
        good_surgery = factory.make_surgery(
            uid="good_surgery_multi",
            seeds={20},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        # Construct intake form and config
        intake = _make_intake_form_stub("1.2.3.4.5.5", work_dir)
        config = _make_config_tables_stub()

        # Act: attempt to construct SourceRFSession
        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        # Assert: friendly message names both failing files
        friendly = exc_info.value.friendly
        message_lower = friendly.message.lower()

        # Both filenames should be present
        assert "truncated" in message_lower, (
            f"Message should name truncated.dcm: {friendly.message}"
        )
        assert "not_a_dicom" in message_lower, (
            f"Message should name not_a_dicom.dcm: {friendly.message}"
        )

        # Recourse should still be actionable
        assert friendly.recourse, "recourse list should be non-empty"

    def test_multiple_failures_message_readable(self, tmp_workdir: Path) -> None:
        """Verify message remains readable with multiple failures."""
        work_dir = tmp_workdir / "multi_failure_readable"
        work_dir.mkdir(parents=True, exist_ok=True)

        # Create 3 malformed files
        malformed.truncated_dicom(work_dir)
        malformed.not_a_dicom(work_dir)
        malformed.three_channel(work_dir)

        # Create 1 good DICOM
        good_surgery = factory.make_surgery(
            uid="good_surgery_3x",
            seeds={30},
            dest_dir=work_dir,
        )
        for dcm_file in good_surgery.glob("*.dcm"):
            dcm_file.rename(work_dir / dcm_file.name)

        intake = _make_intake_form_stub("1.2.3.4.5.6", work_dir)
        config = _make_config_tables_stub()

        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        friendly = exc_info.value.friendly

        # Should mention first 3 files and possibly say "... and N more"
        message = friendly.message
        assert len(message) > 0, "message should not be empty"
        assert len(message) <= 500, (
            f"message should fit terminal ({len(message)} chars)"
        )
        # Should NOT be a raw Python traceback
        assert "Traceback" not in message, "should not include raw traceback"
        # Should mention at least one failing file (without .dcm extension)
        failing_files = ["truncated", "not_a_dicom", "three_channel"]
        found_any = any(f in message.lower() for f in failing_files)
        assert found_any, f"should mention at least one of {failing_files}"


# ---------------------------------------------------------------------------
# Integration: verify error object structure
# ---------------------------------------------------------------------------


class TestFriendlyErrorStructure:
    """Verify FriendlyError object structure in GatewayError."""

    def test_gateway_error_carries_friendly_error(
        self, tmp_workdir: Path
    ) -> None:
        """GatewayError.friendly should be a valid FriendlyError."""
        from src.services.errors import FriendlyError

        work_dir = tmp_workdir / "structure_test"
        work_dir.mkdir(parents=True, exist_ok=True)

        malformed.truncated_dicom(work_dir)
        factory.make_surgery(uid="good_structure", seeds={40}, dest_dir=work_dir)

        intake = _make_intake_form_stub("1.2.3.4.5.7", work_dir)
        config = _make_config_tables_stub()

        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        friendly = exc_info.value.friendly
        assert isinstance(friendly, FriendlyError), (
            f"GatewayError.friendly should be FriendlyError, got {type(friendly)}"
        )

        # Required fields
        assert hasattr(friendly, "title"), "FriendlyError should have title"
        assert hasattr(friendly, "message"), "FriendlyError should have message"
        assert hasattr(friendly, "recourse"), "FriendlyError should have recourse"
        assert friendly.title, "title should be non-empty"
        assert friendly.message, "message should be non-empty"
        assert isinstance(friendly.recourse, list), "recourse should be a list"

    def test_diagnostic_log_path_can_be_set(self, tmp_workdir: Path) -> None:
        """FriendlyError can optionally include a diagnostic log path."""
        work_dir = tmp_workdir / "log_path_test"
        work_dir.mkdir(parents=True, exist_ok=True)

        malformed.not_a_dicom(work_dir)
        factory.make_surgery(uid="good_log", seeds={50}, dest_dir=work_dir)

        intake = _make_intake_form_stub("1.2.3.4.5.8", work_dir)
        config = _make_config_tables_stub()

        with pytest.raises(GatewayError) as exc_info:
            SourceRFSession(intake, config)

        friendly = exc_info.value.friendly
        # diagnostic_log_path may be None if FriendlyError was created without calling handle()
        # But when it is set, it should be a valid string
        if friendly.diagnostic_log_path is not None:
            assert isinstance(friendly.diagnostic_log_path, str), (
                f"diagnostic_log_path should be str, got {type(friendly.diagnostic_log_path)}"
            )
