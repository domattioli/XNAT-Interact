"""
tests/test_guided_upload_wizard.py — Offline tests for guided upload wizard.

NO streamlit import. NO network. NO real XNAT.

Tests the step_blockers validation logic that gates step advance.
This validates the REDCap-style point-of-entry model that structurally
prevents the common mistakes (advancing past missing required fields,
skipping privacy check, etc.).
"""
from __future__ import annotations


# Only import from app/logic and our wizard (which must not import streamlit directly)
from app.guided.wizard_upload import step_blockers


class TestStepBlockers:
    """Test the step-by-step blocker logic."""

    def test_step_0_empty_filer_hawkid_blocks(self) -> None:
        """Step 0: missing filer_hawkid is a blocker."""
        form = {
            "filer_hawkid": "",
            "operation_date": "2024-01-01",
        }
        blockers = step_blockers(0, form, False)
        assert len(blockers) > 0
        assert any("HawkID" in b for b in blockers)

    def test_step_0_empty_operation_date_blocks(self) -> None:
        """Step 0: missing operation_date is a blocker."""
        form = {
            "filer_hawkid": "jsmith",
            "operation_date": "",
        }
        blockers = step_blockers(0, form, False)
        assert len(blockers) > 0
        assert any("Operation Date" in b for b in blockers)

    def test_step_0_full_succeeds(self) -> None:
        """Step 0: with both fields filled, no blockers."""
        form = {
            "filer_hawkid": "jsmith",
            "operation_date": "2024-01-01",
        }
        blockers = step_blockers(0, form, False)
        assert len(blockers) == 0

    def test_step_1_missing_institution_blocks(self) -> None:
        """Step 1: missing institution_name is a blocker."""
        form = {
            "institution_name": "",
            "procedure_name": "ACL Repair",
            "performing_surgeon": "mdoe",
            "epic_start_time": "09:30:00",
        }
        blockers = step_blockers(1, form, False)
        assert len(blockers) > 0
        assert any("Institution" in b for b in blockers)

    def test_step_1_missing_procedure_blocks(self) -> None:
        """Step 1: missing procedure_name is a blocker."""
        form = {
            "institution_name": "UIHC",
            "procedure_name": "",
            "performing_surgeon": "mdoe",
            "epic_start_time": "09:30:00",
        }
        blockers = step_blockers(1, form, False)
        assert len(blockers) > 0
        assert any("Procedure" in b for b in blockers)

    def test_step_1_missing_surgeon_blocks(self) -> None:
        """Step 1: missing performing_surgeon is a blocker."""
        form = {
            "institution_name": "UIHC",
            "procedure_name": "ACL Repair",
            "performing_surgeon": "",
            "epic_start_time": "09:30:00",
        }
        blockers = step_blockers(1, form, False)
        assert len(blockers) > 0
        assert any("Surgeon" in b for b in blockers)

    def test_step_1_missing_epic_start_time_blocks(self) -> None:
        """Step 1: missing epic_start_time is a blocker."""
        form = {
            "institution_name": "UIHC",
            "procedure_name": "ACL Repair",
            "performing_surgeon": "mdoe",
            "epic_start_time": "",
        }
        blockers = step_blockers(1, form, False)
        assert len(blockers) > 0
        assert any("Epic Start Time" in b for b in blockers)

    def test_step_1_all_filled_succeeds(self) -> None:
        """Step 1: with all required fields, no blockers."""
        form = {
            "institution_name": "UIHC",
            "procedure_name": "ACL Repair",
            "performing_surgeon": "mdoe",
            "epic_start_time": "09:30:00",
        }
        blockers = step_blockers(1, form, False)
        assert len(blockers) == 0

    def test_step_2_empty_image_dir_blocks(self) -> None:
        """Step 2: missing image_dir is a blocker."""
        form = {
            "image_dir": "",
        }
        blockers = step_blockers(2, form, False)
        assert len(blockers) > 0
        assert any("Image" in b for b in blockers)

    def test_step_2_with_image_dir_succeeds(self) -> None:
        """Step 2: with image_dir, no blockers."""
        form = {
            "image_dir": "/path/to/images",
        }
        blockers = step_blockers(2, form, False)
        assert len(blockers) == 0

    def test_step_3_privacy_not_affirmed_blocks(self) -> None:
        """Step 3: privacy_affirmed=False is a blocker (key gate)."""
        form = {}
        blockers = step_blockers(3, form, privacy_affirmed=False)
        assert len(blockers) > 0
        assert any("PHI" in b for b in blockers)

    def test_step_3_privacy_affirmed_succeeds(self) -> None:
        """Step 3: privacy_affirmed=True removes the blocker."""
        form = {}
        blockers = step_blockers(3, form, privacy_affirmed=True)
        assert len(blockers) == 0

    def test_step_4_review_no_blockers(self) -> None:
        """Step 4: review is read-only, no blockers."""
        form = {}
        blockers = step_blockers(4, form, False)
        assert len(blockers) == 0

    def test_step_5_confirm_no_blockers(self) -> None:
        """Step 5: confirm step has no blockers."""
        form = {}
        blockers = step_blockers(5, form, False)
        assert len(blockers) == 0

    def test_fully_valid_form_all_steps_clear(self) -> None:
        """A completely valid form passes all steps."""
        form = {
            "filer_hawkid": "jsmith",
            "operation_date": "2024-01-01",
            "institution_name": "UIHC",
            "procedure_name": "ACL Repair",
            "performing_surgeon": "mdoe",
            "epic_start_time": "09:30:00",
            "image_dir": "/path/to/images",
        }
        for step in range(6):
            # All steps should be passable with full form + privacy affirmed
            blockers = step_blockers(
                step,
                form,
                privacy_affirmed=True,
                image_count=10,
            )
            assert len(blockers) == 0, f"Step {step} should not block with full form"
