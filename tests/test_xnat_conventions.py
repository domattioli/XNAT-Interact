"""
Tests for src.services.xnat_conventions.

Locks all path/label strings byte-for-byte against current usage.
SC-003 + SC-006: Ensures canonicalized builders return exactly the strings
currently embedded in xnat_experiment_data.py, xnat_resource_data.py,
utilities.py, and annotations/io_xnat.py.
"""

import pytest
from src.services.xnat_conventions import (
    SCAN_DEFAULT,
    project_qs,
    subject_qs,
    experiment_qs,
    scan_qs,
    source_data_label,
    consensus_label,
    ResourceLabel,
)


class TestConstants:
    """Test constant values."""

    def test_scan_default(self):
        """SCAN_DEFAULT is '0'."""
        assert SCAN_DEFAULT == '0'


class TestProjectQueryString:
    """Test project_qs builder."""

    def test_project_qs_basic(self):
        """project_qs builds canonical project path."""
        result = project_qs('GROK_AHRQ_Data')
        assert result == '/project/GROK_AHRQ_Data'

    def test_project_qs_matches_existing_inline_string(self):
        """project_qs matches the hardcoded string in xnat_experiment_data.py:151."""
        # From xnat_experiment_data.py line 151:
        #   proj_qs = '/project/' + xnat_connection.xnat_project_name
        project_name = 'GROK_AHRQ_Data'
        expected = '/project/' + project_name
        assert project_qs(project_name) == expected

    def test_project_qs_empty_name(self):
        """project_qs handles empty project name."""
        result = project_qs('')
        assert result == '/project/'


class TestSubjectQueryString:
    """Test subject_qs builder."""

    def test_subject_qs_basic(self):
        """subject_qs builds canonical subject path."""
        result = subject_qs('GROK_AHRQ_Data', '1.2.840.113619.2.123')
        assert result == '/project/GROK_AHRQ_Data/subject/1.2.840.113619.2.123'

    def test_subject_qs_with_uid_dots(self):
        """subject_qs preserves UID dots (no conversion to underscore here)."""
        # UIDs from pydicom naturally have dots; XNAT accepts them as-is.
        result = subject_qs('MyProject', '1.2.3')
        assert result == '/project/MyProject/subject/1.2.3'

    def test_subject_qs_root_slash(self):
        """subject_qs includes leading /."""
        result = subject_qs('P', 'S')
        assert result.startswith('/')


class TestExperimentQueryString:
    """Test experiment_qs builder."""

    def test_experiment_qs_basic(self):
        """experiment_qs builds canonical experiment path."""
        result = experiment_qs('GROK_AHRQ_Data', '1.2.840', 'SOURCE_DATA-1.2.840')
        assert result == '/project/GROK_AHRQ_Data/subject/1.2.840/experiment/SOURCE_DATA-1.2.840'

    def test_experiment_qs_root_slash(self):
        """experiment_qs includes leading /."""
        result = experiment_qs('P', 'S', 'E')
        assert result.startswith('/')

    def test_experiment_qs_nesting(self):
        """experiment_qs nests correctly under subject."""
        project = 'P'
        subject = 'S'
        exp_label = 'SOURCE_DATA-123'
        result = experiment_qs(project, subject, exp_label)
        subj = subject_qs(project, subject)
        assert result.startswith(subj)


class TestScanQueryString:
    """Test scan_qs builder."""

    def test_scan_qs_default_scan_label(self):
        """scan_qs with default scan='0' builds canonical scan path."""
        result = scan_qs('GROK_AHRQ_Data', '1.2.840', 'SOURCE_DATA-1.2.840')
        assert result == '/project/GROK_AHRQ_Data/subject/1.2.840/experiment/SOURCE_DATA-1.2.840/scan/0'

    def test_scan_qs_custom_scan_label(self):
        """scan_qs with custom scan label."""
        result = scan_qs('P', 'S', 'E', scan='1')
        assert result.endswith('/scan/1')

    def test_scan_qs_matches_existing_inline_string(self):
        """scan_qs matches hardcoded logic from xnat_experiment_data.py."""
        # From xnat_experiment_data.py lines 146-155:
        # exp_label = 'SOURCE_DATA' + '-' + self.intake_form.uid
        # scan_label = '0'
        # proj_qs = '/project/' + xnat_connection.xnat_project_name
        # subj_qs = proj_qs / 'subject' / str(self.intake_form.uid)
        # exp_qs = subj_qs / 'experiment' / exp_label
        # scan_qs = exp_qs / 'scan' / scan_label
        project = 'GROK_AHRQ_Data'
        subject = '1.2.840.113619.2.123'
        experiment = 'SOURCE_DATA-1.2.840.113619.2.123'
        scan_label = '0'
        result = scan_qs(project, subject, experiment, scan=scan_label)
        assert result == '/project/GROK_AHRQ_Data/subject/1.2.840.113619.2.123/experiment/SOURCE_DATA-1.2.840.113619.2.123/scan/0'

    def test_scan_qs_root_slash(self):
        """scan_qs includes leading /."""
        result = scan_qs('P', 'S', 'E')
        assert result.startswith('/')


class TestSourceDataLabel:
    """Test source_data_label builder."""

    def test_source_data_label_basic(self):
        """source_data_label builds experiment label."""
        uid = '1.2.840.113619.2.123'
        result = source_data_label(uid)
        assert result == 'SOURCE_DATA-1.2.840.113619.2.123'

    def test_source_data_label_matches_existing_inline_string(self):
        """source_data_label matches hardcoded logic from xnat_experiment_data.py:146."""
        # From xnat_experiment_data.py line 146:
        #   exp_label = ('SOURCE_DATA' + '-' + self.intake_form.uid)
        uid = '1.2.840'
        expected = 'SOURCE_DATA' + '-' + uid
        assert source_data_label(uid) == expected

    def test_source_data_label_format(self):
        """source_data_label format is 'SOURCE_DATA-{uid}'."""
        uid = 'test_uid'
        result = source_data_label(uid)
        assert result.startswith('SOURCE_DATA-')
        assert result == f'SOURCE_DATA-{uid}'


class TestConsensusLabel:
    """Test consensus_label builder."""

    def test_consensus_label_basic(self):
        """consensus_label builds consensus label."""
        uid = '1.2.840.113619.2.123'
        result = consensus_label(uid)
        assert result == 'SEGMENTATION_CONSENSUS-1.2.840.113619.2.123'

    def test_consensus_label_format(self):
        """consensus_label format is 'SEGMENTATION_CONSENSUS-{uid}'."""
        uid = 'test_uid'
        result = consensus_label(uid)
        assert result.startswith('SEGMENTATION_CONSENSUS-')
        assert result == f'SEGMENTATION_CONSENSUS-{uid}'


class TestResourceLabel:
    """Test ResourceLabel constant registry."""

    def test_src_constant(self):
        """ResourceLabel.SRC matches hardcoded 'SRC' from xnat_experiment_data.py:157."""
        assert ResourceLabel.SRC == 'SRC'

    def test_intake_form_constant(self):
        """ResourceLabel.INTAKE_FORM matches hardcoded 'INTAKE_FORM' from xnat_resource_data.py:776."""
        assert ResourceLabel.INTAKE_FORM == 'INTAKE_FORM'

    def test_annotations_constant(self):
        """ResourceLabel.ANNOTATIONS is 'ANNOTATIONS' (default in io_xnat.py)."""
        assert ResourceLabel.ANNOTATIONS == 'ANNOTATIONS'

    def test_config_constant(self):
        """ResourceLabel.CONFIG matches hardcoded 'config' from utilities.py:91."""
        assert ResourceLabel.CONFIG == 'config'

    def test_backups_constant(self):
        """ResourceLabel.BACKUPS matches hardcoded 'backups' from utilities.py:92."""
        assert ResourceLabel.BACKUPS == 'backups'

    def test_segmentation_consensus_constant(self):
        """ResourceLabel.SEGMENTATION_CONSENSUS is 'SEGMENTATION_CONSENSUS'."""
        assert ResourceLabel.SEGMENTATION_CONSENSUS == 'SEGMENTATION_CONSENSUS'

    def test_all_constants_are_strings(self):
        """All ResourceLabel attributes are strings."""
        assert isinstance(ResourceLabel.SRC, str)
        assert isinstance(ResourceLabel.INTAKE_FORM, str)
        assert isinstance(ResourceLabel.ANNOTATIONS, str)
        assert isinstance(ResourceLabel.CONFIG, str)
        assert isinstance(ResourceLabel.BACKUPS, str)
        assert isinstance(ResourceLabel.SEGMENTATION_CONSENSUS, str)


class TestIntegration:
    """Integration tests combining builders."""

    def test_full_scan_path_construction(self):
        """Full path construction: project → subject → experiment → scan."""
        project = 'GROK_AHRQ_Data'
        subject = '1.2.840'
        uid = subject  # In practice, subject UID = intake_form.uid
        exp_label = source_data_label(uid)

        result = scan_qs(project, subject, exp_label)

        # Verify all levels are present
        assert '/project/GROK_AHRQ_Data' in result
        assert '/subject/1.2.840' in result
        assert '/experiment/SOURCE_DATA-1.2.840' in result
        assert '/scan/0' in result

    def test_path_immutability_across_calls(self):
        """Repeated calls to builders return identical strings."""
        project, subject, experiment = 'P', 'S', 'E'
        result1 = scan_qs(project, subject, experiment)
        result2 = scan_qs(project, subject, experiment)
        assert result1 == result2
        assert result1 is not result2  # Different objects, same value

    def test_resource_label_uniqueness(self):
        """All ResourceLabel constants are distinct."""
        labels = [
            ResourceLabel.SRC,
            ResourceLabel.INTAKE_FORM,
            ResourceLabel.ANNOTATIONS,
            ResourceLabel.CONFIG,
            ResourceLabel.BACKUPS,
            ResourceLabel.SEGMENTATION_CONSENSUS,
        ]
        assert len(labels) == len(set(labels)), "Resource labels should be unique"
