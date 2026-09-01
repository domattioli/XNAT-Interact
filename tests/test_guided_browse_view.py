"""Tests for app.guided.browse_view — offline, no streamlit mocking needed."""
import pytest
from app.guided.demo import build_demo_server
from app.guided.browse_view import _browse_display_frame
from app.logic.browse import fetch_data_table


class TestBrowseDisplayFrame:
    """Test _browse_display_frame() helper."""

    def test_browse_display_frame_has_correct_columns(self):
        """DataFrame has 5 display columns (no scan_id)."""
        server = build_demo_server()
        rows = fetch_data_table(server, "DEMO_PROJECT")
        assert isinstance(rows, list)

        df = _browse_display_frame(rows)
        assert list(df.columns) == ["Subject", "Experiment", "Date", "Type", "# Files"]

    def test_browse_display_frame_has_rows(self):
        """Demo server produces ≥2 rows."""
        server = build_demo_server()
        rows = fetch_data_table(server, "DEMO_PROJECT")
        assert isinstance(rows, list)
        assert len(rows) >= 2

        df = _browse_display_frame(rows)
        assert len(df) >= 2

    def test_browse_display_frame_synthetic_subjects_visible(self):
        """Synthetic subject labels (DEMO_S*) are in the Subject column."""
        server = build_demo_server()
        rows = fetch_data_table(server, "DEMO_PROJECT")
        assert isinstance(rows, list)

        df = _browse_display_frame(rows)
        subjects = df["Subject"].tolist()

        # At least one of the demo subjects should appear
        assert any(subj.startswith("DEMO_S") for subj in subjects), \
            f"Expected DEMO_S* subjects, got: {subjects}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
