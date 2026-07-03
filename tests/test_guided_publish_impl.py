"""
tests/test_guided_publish_impl.py — Offline tests for app.guided.publish_impl.

Tests make_publish_fn with FakeXNAT server (fake path).
No real XNAT, no streamlit.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from app.guided.demo import build_demo_server
from app.guided.publish_impl import make_publish_fn
from app.logic.download import list_downloadable
from app.logic.upload import prepare_and_upload, UploadOutcome
from src.xnat_experiment_data import ReviewDecision


class TestMakePublishFnFake:
    """make_publish_fn returns fake path for FakeXNAT."""

    def test_make_publish_fn_returns_callable(self):
        server = build_demo_server()
        fn = make_publish_fn(server)
        assert callable(fn)

    def test_fake_publish_registers_new_case(self):
        """Fake publish creates subject+experiment in the FakeXNAT archive."""
        server = build_demo_server()
        initial_count = len(server._experiments)

        fn = make_publish_fn(server)
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a placeholder DICOM file
            dcm_path = Path(tmpdir) / "test.dcm"
            dcm_path.write_bytes(b"FAKE_DICOM")

            fn(
                server_connection=server,
                form_values={
                    "filer_hawkid": "testuser",
                    "operation_date": "2024-03-15",
                    "procedure_name": "ANKLE_ORIF",
                    "institution_name": "MERCY_HOSPITAL",
                    "performing_surgeon": "dr_jones",
                    "epic_start_time": "10:00",
                },
                image_dir=tmpdir,
                pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, None),
            )

        # New experiment should be registered
        assert len(server._experiments) > initial_count

    def test_prepare_and_upload_with_fake_publish_fn(self):
        """prepare_and_upload with make_publish_fn returns ok=True."""
        server = build_demo_server()
        publish_fn = make_publish_fn(server)

        with tempfile.TemporaryDirectory() as tmpdir:
            dcm_path = Path(tmpdir) / "frame.dcm"
            dcm_path.write_bytes(b"FAKE_DICOM_CONTENT")

            outcome: UploadOutcome = prepare_and_upload(
                form_values={
                    "filer_hawkid": "testuser",
                    "operation_date": "2024-03-15",
                    "procedure_name": "HIP_ARTHROSCOPY",
                    "institution_name": "UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS",
                    "performing_surgeon": "dr_smith",
                    "epic_start_time": "09:00",
                    "image_dir": tmpdir,
                },
                image_dir=tmpdir,
                server_connection=server,
                review_decision=ReviewDecision.CONFIRMED,
                publish_fn=publish_fn,
            )

        assert outcome.ok is True
        assert outcome.friendly is None

    def test_list_downloadable_shows_new_case_after_upload(self):
        """After fake publish, list_downloadable includes the new case."""
        server = build_demo_server()
        publish_fn = make_publish_fn(server)
        project_name = server.project_name

        initial_downloadable = list_downloadable(server, project_name)
        initial_count = len(initial_downloadable) if isinstance(initial_downloadable, list) else 0

        with tempfile.TemporaryDirectory() as tmpdir:
            dcm_path = Path(tmpdir) / "frame.dcm"
            dcm_path.write_bytes(b"FAKE_DICOM_CONTENT_UNIQUE")

            publish_fn(
                server_connection=server,
                form_values={
                    "filer_hawkid": "tester2",
                    "operation_date": "2024-04-01",
                    "procedure_name": "ANKLE_ORIF",
                    "institution_name": "MERCY_HOSPITAL",
                    "performing_surgeon": "dr_okafor",
                    "epic_start_time": "11:00",
                },
                image_dir=tmpdir,
                pixel_review_confirmer=lambda ctx: (ReviewDecision.CONFIRMED, None),
            )

        after_downloadable = list_downloadable(server, project_name)
        after_count = len(after_downloadable) if isinstance(after_downloadable, list) else 0
        assert after_count > initial_count


class TestDownloadAfterUpload:
    """Download pipeline works after fake publish."""

    def test_download_selection_returns_files(self):
        """download_selection returns files for a seeded demo case."""
        from app.logic.download import download_selection
        server = build_demo_server()
        project_name = server.project_name

        downloadable = list_downloadable(server, project_name)
        assert isinstance(downloadable, list)
        assert len(downloadable) > 0

        # Download first case
        row = downloadable[0]
        with tempfile.TemporaryDirectory() as dest:
            outcome = download_selection(server, project_name, [row], dest)
            assert outcome.ok is True
            assert len(outcome.files_written) > 0
            for fp in outcome.files_written:
                assert fp.exists()
                assert fp.stat().st_size > 0
