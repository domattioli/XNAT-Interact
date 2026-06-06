"""
XNAT Conventions — canonical path/label strings.

Centralizes path-building query strings, resource labels, and file naming
conventions to prevent inline string duplication. Source of truth for
addressing the XNAT hierarchy.

Constants & Builders
--------------------
SCAN_DEFAULT = '0'  # Single-scan-per-experiment design simplification.

Path Query Strings (PurePosixPath format)
  project_qs(name) -> str
  subject_qs(project, subject) -> str
  experiment_qs(project, subject, experiment) -> str
  scan_qs(project, subject, experiment, scan=SCAN_DEFAULT) -> str

Label & Filename Builders
  source_data_label(uid) -> str
  consensus_label(uid) -> str

Resource Label Registry (string constants)
  ResourceLabel.SRC
  ResourceLabel.INTAKE_FORM
  ResourceLabel.ANNOTATIONS
  ResourceLabel.CONFIG
  ResourceLabel.BACKUPS
  ResourceLabel.SEGMENTATION_CONSENSUS

Design
------
All strings match existing usage in xnat_experiment_data.py, xnat_resource_data.py,
utilities.py, and annotations/io_xnat.py (locked by test_xnat_conventions.py).
Stage 3 routes all call sites to these builders; Stage 2 only defines them.
"""

from pathlib import PurePosixPath
from typing import Optional


# Default scan label — XNAT sessions can hold multiple scans; we use one.
SCAN_DEFAULT = '0'


# ---------------------------------------------------------------------------
# Query String Builders
# ---------------------------------------------------------------------------

def project_qs(name: str) -> str:
    """
    Build project-level query string.

    Args:
        name: XNAT project name (e.g., 'GROK_AHRQ_Data').

    Returns:
        str: Query string (e.g., '/project/GROK_AHRQ_Data').
    """
    return f'/project/{name}'


def subject_qs(project: str, subject: str) -> str:
    """
    Build subject-level query string.

    Args:
        project: XNAT project name.
        subject: Subject UID (from intake_form.uid, usually a DICOM UID).

    Returns:
        str: Query string (e.g., '/project/P/subject/UID').
    """
    proj = project_qs(project)
    return str(PurePosixPath(proj) / 'subject' / subject)


def experiment_qs(project: str, subject: str, experiment: str) -> str:
    """
    Build experiment-level query string.

    Args:
        project: XNAT project name.
        subject: Subject UID.
        experiment: Experiment label (e.g., 'SOURCE_DATA-{uid}').

    Returns:
        str: Query string (e.g., '/project/P/subject/S/experiment/E').
    """
    subj = subject_qs(project, subject)
    return str(PurePosixPath(subj) / 'experiment' / experiment)


def scan_qs(
    project: str,
    subject: str,
    experiment: str,
    scan: str = SCAN_DEFAULT
) -> str:
    """
    Build scan-level query string.

    Args:
        project: XNAT project name.
        subject: Subject UID.
        experiment: Experiment label.
        scan: Scan label (default '0' per design simplification).

    Returns:
        str: Query string (e.g., '/project/P/subject/S/experiment/E/scan/0').
    """
    exp = experiment_qs(project, subject, experiment)
    return str(PurePosixPath(exp) / 'scan' / scan)


# ---------------------------------------------------------------------------
# Label Builders
# ---------------------------------------------------------------------------

def source_data_label(uid: str) -> str:
    """
    Build experiment label for raw/intra-op data.

    Marks the session as containing source (un-derived) data.

    Args:
        uid: Subject/intake UID (usually a DICOM UID with . → _).

    Returns:
        str: Label string (e.g., 'SOURCE_DATA-1_2_840_113619_2_123').
    """
    return f'SOURCE_DATA-{uid}'


def consensus_label(uid: str) -> str:
    """
    Build resource label for consensus/derived annotations.

    Default label for assessor-backed segmentation consensus.

    Args:
        uid: Subject/intake UID.

    Returns:
        str: Label string (e.g., 'SEGMENTATION_CONSENSUS-{uid}').
    """
    return f'SEGMENTATION_CONSENSUS-{uid}'


# ---------------------------------------------------------------------------
# Resource Label Registry
# ---------------------------------------------------------------------------

class ResourceLabel:
    """
    Registry of all resource labels used across the XNAT hierarchy.

    Each label is a string constant; resources attach at the level where
    they live (project, subject, experiment, scan, or assessor).

    Attributes:
        SRC              : Zipped DICOM/MP4 source data (attaches to scan).
        INTAKE_FORM      : Surgical metadata JSON (attaches to subject).
        ANNOTATIONS      : Annotation blobs + manifest (attaches to scan).
        CONFIG           : Project configuration JSON (attaches to project).
        BACKUPS          : Config backups (attaches to project).
        SEGMENTATION_CONSENSUS : Consensus segmentation (attaches to assessor).
    """

    SRC = 'SRC'
    INTAKE_FORM = 'INTAKE_FORM'
    ANNOTATIONS = 'ANNOTATIONS'
    CONFIG = 'config'
    BACKUPS = 'backups'
    SEGMENTATION_CONSENSUS = 'SEGMENTATION_CONSENSUS'
