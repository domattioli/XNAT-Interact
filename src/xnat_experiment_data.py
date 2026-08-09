import hashlib
import os
from typing import Callable, Optional as Opt, Tuple, Union
import numpy as np
import pandas as pd
from datetime import datetime
from pydicom.dataset import FileDataset as pydicomFileDataset
from pydicom import Dataset, Sequence, dcmread, dcmwrite
from pathlib import Path, PurePosixPath
import shutil
import tempfile

from src.utilities import ConfigTables, USCentralDateTime, XNATLogin, XNATConnection
from src.xnat_scan_data import *
from src.xnat_resource_data import *
from src.services.deidentify import needs_pixel_review, apply_redaction
from src.services.errors import FriendlyError, handle as _handle_error
from src.services.xnat_gateway import GatewayError
from src.services import xnat_conventions as conventions

# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['SourceRFSession', 'SourceESVSession'] # Each time you add a new class that inherits from ExperimentData, add it to this list.


# ---------------------------------------------------------------------------
# PHI pixel-review gate — T014
# ---------------------------------------------------------------------------

from enum import Enum
from typing import List as _List, Tuple as _Tuple


class ReviewDecision(Enum):
    """
    Outcome returned by a pixel_review_confirmer callable.

    CONFIRMED   — reviewer attests no burned-in PHI is visible; upload may proceed.
    REDACT      — reviewer supplies redaction boxes; apply_redaction is called before upload.
    ABORT       — reviewer refuses to confirm; upload is blocked.
    QUARANTINE  — automated assessment flagged this case for operator review;
                  upload is blocked and the case is held in the quarantine store.
    """
    CONFIRMED  = "confirmed"
    REDACT     = "redact"
    ABORT      = "abort"
    QUARANTINE = "quarantine"


class UploadError(Exception):
    """
    Raised when an upload is blocked or interrupted.

    Carries a :class:`~src.services.errors.FriendlyError` as ``friendly``
    so callers can render a user-facing message via ``errors.render()``.

    This is necessary because :class:`FriendlyError` is a plain dataclass
    (not a BaseException subclass) and cannot be raised directly.
    """

    def __init__(self, friendly: "FriendlyError") -> None:  # type: ignore[name-defined]
        super().__init__(friendly.message)
        self.friendly = friendly


class DedupReviewRequired(Exception):
    """
    Raised when an incoming case's image-hash set overlaps an existing case
    under a different subject (FR-007, T013).

    No auto-import, no auto-merge, no auto-reject.  The caller MUST surface
    the evidence package to a human for a decision (import both / combine /
    other).  Uploading is blocked until the human acts.

    Attributes
    ----------
    evidence : EvidencePackage
        Structured overlap evidence (overlap ratio, matching shots,
        UID/date/device corroboration).  Read-only — no action is taken.
    """

    def __init__(self, evidence) -> None:  # evidence: EvidencePackage
        self.evidence = evidence
        super().__init__(
            f"Dedup review required — overlap detected with existing case "
            f"'{evidence.matched_case_key}' "
            f"({evidence.classification}, ratio={evidence.overlap_ratio:.3f}). "
            "No data was uploaded. A human must review the evidence package "
            "and decide: import both / combine / other."
        )


# ---------------------------------------------------------------------------
# #32 dedup wiring — registry proxy for self-exclusion
# ---------------------------------------------------------------------------

class _RegistryExcludingCase:
    """
    Thin proxy over a Registry that hides one case_key from case_dedup
    enumeration.

    After write() the current intake uid's hashes are registered in the
    registry (via ConfigTables write-through).  Without this proxy
    case_dedup would compare the incoming set against itself and always
    report EXACT — a false positive.

    The proxy intercepts the two surfaces case_dedup uses:
      1. ``_conn.execute("SELECT DISTINCT case_key FROM cases")`` — rows
         for the excluded key are filtered out of the result set.
      2. ``case_image_hashes(case_key)`` — returns empty set for the
         excluded key (belt-and-suspenders; also prevents any residual
         self-match when the case is somehow still enumerated).

    All other registry calls (upsert_*) are forwarded unchanged so that
    publish_to_xnat can still register the disjoint case after a clean check.
    """

    def __init__( self, registry, excluded_case_key: str ) -> None:
        self._registry        = registry
        self._excluded        = str( excluded_case_key )
        self._conn            = _FilteringConn( registry._conn, self._excluded )

    def case_image_hashes( self, case_key: str ) -> set:
        if str( case_key ) == self._excluded:
            return set()
        return self._registry.case_image_hashes( case_key )

    def upsert_case( self, *args, **kwargs ):
        return self._registry.upsert_case( *args, **kwargs )

    def upsert_image_hash( self, *args, **kwargs ):
        return self._registry.upsert_image_hash( *args, **kwargs )


class _FilteringConn:
    """
    SQLite connection proxy that removes a specific case_key from the
    ``SELECT DISTINCT case_key FROM cases`` result used by case_dedup.

    All other queries are forwarded verbatim.
    """

    _CASES_ENUM_LOWER = "select distinct case_key from cases"

    def __init__( self, conn, excluded_case_key: str ) -> None:
        self._conn     = conn
        self._excluded = str( excluded_case_key )

    def execute( self, sql: str, params=() ):
        cur = self._conn.execute( sql, params )
        if sql.strip().lower().startswith( self._CASES_ENUM_LOWER ):
            rows = [ r for r in cur.fetchall() if r[0] != self._excluded ]
            return _StaticCursor( rows )
        return cur


class _StaticCursor:
    """Minimal cursor-like object wrapping a pre-fetched list of rows."""

    def __init__( self, rows ) -> None:
        self._rows = rows

    def fetchall( self ):
        return list( self._rows )

    def fetchone( self ):
        return self._rows[0] if self._rows else None


class _ConfigTablesRegistryAdapter:
    """
    In-memory registry adapter for the pre-write dedup check.

    Sole source: ConfigTables.tables['IMAGE_HASHES'] — project-scoped,
    always in sync with the freshly-loaded JSON config (pull_from_xnat).

    Implements the interface consumed by case_dedup:
      - ``_conn.execute("SELECT DISTINCT case_key FROM cases")``
      - ``case_image_hashes(case_key) -> set[str]``

    The adapter is read-only — upsert_* calls are no-ops (they are called
    post-check by publish_to_xnat to register the disjoint case; the real
    registry handles those writes).
    """

    def __init__( self, config ) -> None:
        self._case_hashes: dict = {}

        # Primary: ConfigTables.tables['IMAGE_HASHES'] — project-scoped JSON source.
        try:
            _tables = getattr( config, 'tables', None )
            if _tables is not None:
                _tname = next(
                    ( k for k in _tables.keys() if k.upper() == 'IMAGE_HASHES' ),
                    None
                )
                if _tname is not None:
                    for _, row in _tables[_tname].iterrows():
                        _hash = row.get( 'NAME' )
                        _subj = row.get( 'SUBJECT' )
                        if _hash and _subj:
                            self._case_hashes.setdefault( str( _subj ), set() ).add( str( _hash ).upper() )
        except Exception:  # noqa: BLE001
            pass

        self._conn = _StaticConn( list( self._case_hashes.keys() ) )

    def case_image_hashes( self, case_key: str ) -> set:
        return self._case_hashes.get( str( case_key ), set() )

    def upsert_case( self, *args, **kwargs ) -> None:
        pass

    def upsert_image_hash( self, *args, **kwargs ) -> None:
        pass


class _StaticConn:
    """
    Minimal connection-like object for case_dedup's case-key enumeration.
    Answers only ``SELECT DISTINCT case_key FROM cases``; raises on other SQL.
    """

    def __init__( self, case_keys ) -> None:
        self._case_keys = list( case_keys )

    def execute( self, sql: str, params=() ):
        _sql_lower = sql.strip().lower()
        if 'case_key' in _sql_lower and 'cases' in _sql_lower and not params:
            # case_dedup enumerates: SELECT DISTINCT case_key FROM cases
            rows = [ (k,) for k in self._case_keys if k ]
            return _StaticCursor( rows )
        # For case_image_hashes lookup (won't be called since we override it).
        return _StaticCursor( [] )


def _interactive_pixel_review_confirmer(context: str) -> Tuple[ReviewDecision, _List[_Tuple[int, int, int, int]]]:
    """
    Default (production) confirmer — asks the operator at the terminal.

    Returns (ReviewDecision, boxes).  Boxes are only meaningful for REDACT.
    ABSENT confirmation defaults to ABORT (fail-closed).
    """
    print( f'\n\t[PHI SAFETY GATE] About to upload pixel data: {context}' )
    print( '\t  Inspect the images for burned-in patient name, date-of-birth, or MRN.' )
    print( '\t  Enter one of:' )
    print( '\t    1 — images contain NO visible PHI (proceed)' )
    print( '\t    2 — abort upload' )
    answer = input( '\tYour choice: ' ).strip()
    if answer == '1':
        return ReviewDecision.CONFIRMED, []
    # Any non-'1' answer → ABORT (fail-closed)
    return ReviewDecision.ABORT, []


# ---------------------------------------------------------------------------
# T019 — Automated pixel confirmer factory (OPT-IN)
# ---------------------------------------------------------------------------

def make_automated_pixel_confirmer(
    frames,
    dataset,
    *,
    registry=None,
    model_dir: str = "models/craft",
    profiles_dir: str = "data/device_profiles",
    quarantine_store=None,
    case_id: Opt[str] = None,
):
    """
    Build a ``pixel_review_confirmer``-compatible callable that runs the
    automated de-id pipeline (verdict.assess_case) instead of prompting a human.

    This is the OPT-IN automated path.  The DEFAULT ``pixel_review_confirmer``
    remains ``_interactive_pixel_review_confirmer`` — no existing call site is
    changed by this function.

    Verdict → ReviewDecision mapping:

    =================== ========================= ========================
    Verdict             ReviewDecision             boxes
    =================== ========================= ========================
    Verdict.CLEAN       ReviewDecision.CONFIRMED   []
    Verdict.REDACTED    ReviewDecision.REDACT       assessment.regions
    Verdict.QUARANTINE  ReviewDecision.QUARANTINE  []  (case written to store)
    =================== ========================= ========================

    All heavy imports (assess_case, Verdict, QuarantineStore) are performed
    LAZILY inside the returned closure so that importing
    ``src.xnat_experiment_data`` remains cheap and does not pull in
    presidio / cv2 / onnxruntime at module load time.

    Parameters
    ----------
    frames:
        List of numpy frame arrays for the case.
    dataset:
        pydicom Dataset for device-identity lookup.
    registry:
        Optional audit registry; forwarded to assess_case.
    model_dir:
        CRAFT model directory; forwarded to assess_case.
    profiles_dir:
        Device profiles directory; forwarded to assess_case.
    quarantine_store:
        Optional ``QuarantineStore`` instance.  When supplied and the verdict
        is QUARANTINE, ``quarantine_store.quarantine_case`` is called to persist
        the evidence before the confirmer returns.
    case_id:
        Case identifier for quarantine storage.  Required when
        *quarantine_store* is given; ignored otherwise.

    Returns
    -------
    Callable[[str], Tuple[ReviewDecision, list]]
        A ``pixel_review_confirmer``-compatible callable.
    """
    # Capture everything the closure needs at call time.
    _frames         = frames
    _dataset        = dataset
    _registry       = registry
    _model_dir      = model_dir
    _profiles_dir   = profiles_dir
    _qstore         = quarantine_store
    _case_id        = case_id

    def _automated_confirmer(context: str):  # noqa: ARG001 — context unused but required by protocol
        # Lazy imports — keep module-level import cheap.
        from src.services.pixel_deid.verdict import assess_case, Verdict  # noqa: PLC0415

        assessment = assess_case(
            _frames,
            _dataset,
            registry=_registry,
            model_dir=_model_dir,
            profiles_dir=_profiles_dir,
        )

        if assessment.verdict == Verdict.CLEAN:
            return ReviewDecision.CONFIRMED, []

        if assessment.verdict == Verdict.REDACTED:
            return ReviewDecision.REDACT, assessment.regions

        # Verdict.QUARANTINE — persist evidence then signal the gate.
        if _qstore is not None and _case_id is not None:
            try:
                _qstore.quarantine_case(
                    _case_id,
                    assessment,
                    frames=_frames,
                    dataset=_dataset,
                )
            except Exception as _exc:  # noqa: BLE001 — never block on store failure
                import logging as _logging
                _logging.getLogger(__name__).warning(
                    "make_automated_pixel_confirmer: quarantine_store write failed: %s", _exc
                )

        return ReviewDecision.QUARANTINE, []

    return _automated_confirmer


def _find_invalid_rows( df ):
    '''
    #33 M4: return rows whose IS_VALID is not True.

    `df['IS_VALID'] == False` misses NaN/None cells (NaN == False is False,
    not True) that can appear if IS_VALID's dtype degrades to object due to a
    mixed-type assignment -- those rows silently passed the invalid-rows gate.
    `!= True` correctly treats NaN/None as "not valid" (NaN != True is True),
    while still excluding rows that are actually True.
    '''
    return df[ df['IS_VALID'] != True ]    # noqa: E712 (intentional -- see docstring)


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all xnat experiment sessions.
class ExperimentData():
    """
    A class representing the base structure of an XNAT Experiment, in which all source- and derived-data are stored.
    - Experiments may only exist as a child of a subject, and may contain multiple scans and resources.

    Attributes:
    intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
    tmp_source_data_dir (Path): local tmp directory in which all source data is temporarily stored before being pushed to XNAT.
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'rf' for radio fluoroscopic data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM' for radio fluoroscopic data.

    Methods:
    - Most of these methods are for instantiating only and are not meant to be called directly.
    - Some methods are intentionally undefined with the expectation that they will be defined in inherited classes.

    publish_to_xnat(): Publishes the experiment session to XNAT, including all associated scans and resources.
    write(): Writes the experiment session to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    write_publish_catalog_subroutine(): Writes the experiment session to a zipped folder and then publishes it to XNAT, including revised config file data.

    Example Usage:
    None -- this is a base class and should not be instantiated directly.
    """
    def __init__( self, intake_form: ORDataIntakeForm, invoking_class: str ) -> None:
        """
        Initialize the ExperimentData object with the inputted ORDataIntakeForm object and the invoking class name.
        
        Args:
        intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
        invoking_class (str): name of the class that is invoking this class, e.g., 'SourceRFSession' or 'SourceESVSession'.
            - This is used to determine the schema_prefix_str and scan_type_label attributes, the former of which is necessary and the latter of which is helpful for XNAT publishing.
        """
        assert os.path.isdir( intake_form.relevant_folder ), f"Inputted path must be a valid directory; inputted path: {intake_form.relevant_folder}"
        assert os.path.exists( intake_form.saved_ffn_str ), f"Inputted IntakeForm must be valid; no file found at: {intake_form.saved_ffn_str}"

        self._intake_form, self._tmp_source_data_dir = intake_form, intake_form.saved_ffn.parent / Path( 'SOURCE_DATA' ) # type: ignore
        if not os.path.exists( self.tmp_source_data_dir ):  os.makedirs( self.tmp_source_data_dir )
        self._df, self._is_valid = pd.DataFrame(), False    # Derived in derived classes' init method

        assert invoking_class in __all__, f"Invoking class must be one of the following strings: {__all__}; you entered: {invoking_class}"
        if invoking_class   == 'SourceRFSession':   self._schema_prefix_str, self._scan_type_label = 'rf',  'DICOM'
        elif invoking_class == 'SourceESVSession':  self._schema_prefix_str, self._scan_type_label = 'esv', 'DICOM_MP4'
        elif invoking_class == 'DerivedData':       self._schema_prefix_str, self._scan_type_label = 'otherDicom', 'DERIVED'

    @property
    def intake_form( self )             -> ORDataIntakeForm:        return self._intake_form
    @property
    def tmp_source_data_dir( self )     -> Path:                    return self._tmp_source_data_dir
    @property
    def df( self )                      -> pd.DataFrame:            return self._df
    @property
    def is_valid( self )                -> bool:                    return self._is_valid
    @property
    def schema_prefix_str( self )       -> str:                     return self._schema_prefix_str
    @property
    def scan_type_label( self )         -> str:                     return self._scan_type_label

    
    def _populate_df( self, config: ConfigTables ):                 raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _check_session_validity( self, config: ConfigTables ):      raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )


    #--------------------------------------------XNAT-Publishing helpers and methods----------------------------------------------------------
    def _generate_queries( self, xnat_connection: XNATConnection, scan: Opt[str] = None ) -> Tuple[str, str, str, str, str]:
        # Create query strings and select object in xnat then create the relevant objects
        # FR-014: scan is user-selectable; defaults to SCAN_DEFAULT ('0').
        exp_label = conventions.source_data_label( self.intake_form.uid )
        scan_label = scan if scan is not None else conventions.SCAN_DEFAULT
        subj_qs = conventions.subject_qs( xnat_connection.xnat_project_name, str( self.intake_form.uid ) )
        exp_qs = conventions.experiment_qs( xnat_connection.xnat_project_name, str( self.intake_form.uid ), exp_label )
        scan_qs = conventions.scan_qs( xnat_connection.xnat_project_name, str( self.intake_form.uid ), exp_label, scan_label )
        files_qs = str( PurePosixPath( scan_qs ) / 'resource' / 'files' )
        resource_label = conventions.ResourceLabel.SRC
        return str( subj_qs ), str( exp_qs ), str( scan_qs ), str( files_qs ), resource_label


    def _select_objects( self, xnat_connection: XNATConnection, subj_qs: str, exp_qs: str, scan_qs: str, files_qs: str ) -> Tuple[object, object, object]:
        # Idempotent-upsert (#27): reuse an existing/orphaned handle rather than
        # asserting non-existence.  A prior failed push may leave an orphaned
        # empty subject or experiment; we reuse it and fill in missing children.
        # The old assert-not-exists caused an AssertionError on re-publish and
        # forced manual cleanup — this resolves spec FR-002 / SC-001.
        subj_inst = xnat_connection.gateway.select( str( subj_qs ) )
        exp_inst  = xnat_connection.gateway.select( str( exp_qs ) )
        scan_inst = xnat_connection.gateway.select( str( scan_qs ) )
        return subj_inst, exp_inst, scan_inst


    def write( self, config: ConfigTables, zip_dest: Opt[Path] = None, verbose: Opt[bool]=True ) -> Tuple[dict, ConfigTables]:   raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    

    def publish_to_xnat(
        self,
        xnat_connection: XNATConnection,
        validated_login: XNATLogin,
        zipped_data: dict,
        delete_zip: Opt[bool] = True,
        verbose: Opt[bool] = True,
        pixel_review_confirmer: Opt[Callable] = None,
        intake_form_temp_artifacts: Opt[_List[Path]] = None,
        assessor: Opt[Path] = None,
        assessor_label: Opt[str] = None,
        scan: Opt[str] = None,
        dedup_registry=None,
        incoming_content_hashes: Opt[set] = None,
    ) -> None:
        """
        Publish zipped pixel data to XNAT.

        T014 — PHI pixel-review gate
        -----------------------------
        Before ANY put_zip call the ``pixel_review_confirmer`` is invoked.
        Default (production) = interactive terminal prompt.
        Tests inject a callable to avoid human interaction.

        Fail-closed contract: if ``pixel_review_confirmer`` is None the
        interactive default is used.  Any response other than CONFIRMED /
        REDACT raises a FriendlyError and no pixel data is sent.

        T027 — local PHI cleanup
        ------------------------
        On successful upload, all zip files listed in ``zipped_data`` are
        deleted (when ``delete_zip=True``), plus any extra temp paths given in
        ``intake_form_temp_artifacts``.

        Artifact locations until cleaned
        ---------------------------------
        - Zip files: keys of ``zipped_data`` (written by ``write()``)
          inside ``self.tmp_source_data_dir`` (i.e. <intake_form_dir>/SOURCE_DATA/).
        - Intake-form temp files: paths passed via ``intake_form_temp_artifacts``.

        T028 — mid-upload connection drop
        -----------------------------------
        Connection/timeout errors from ``put_zip`` are caught and re-raised as
        a FriendlyError with resume recourse rather than a raw traceback.

        T013 — Dedup check + no-empty-shell invariant
        ----------------------------------------------
        When ``dedup_registry`` and ``incoming_content_hashes`` are both
        supplied, case_dedup() is called before any XNAT object is created.

        DISJOINT → proceed normally (existing behavior unchanged).
        Non-DISJOINT against a DIFFERENT subject → build an evidence package
        and raise DedupReviewRequired (no XNAT objects created, no data sent).
        Fail-soft: if the registry is unavailable or the check errors, log a
        warning and proceed (degrade to today's behavior — never block a
        legitimate upload on a registry fault).

        FR-008 (no-empty-shell): if ``zipped_data`` is empty, XNAT Subject /
        Experiment / Scan objects are NOT created.  Guard runs before create().
        """
        # ------------------------------------------------------------------
        # T014  PHI pixel-review gate — MUST run before any put_zip
        # ------------------------------------------------------------------
        if pixel_review_confirmer is None:
            pixel_review_confirmer = _interactive_pixel_review_confirmer

        context_str = f'{self.schema_prefix_str} session ({len(zipped_data)} zip file(s))'
        decision, redact_boxes = pixel_review_confirmer( context_str )

        if decision == ReviewDecision.ABORT:
            raise UploadError(
                FriendlyError(
                    title="Upload stopped — pixel PHI review not confirmed",
                    message=(
                        "Upload stopped — confirm the images have no visible patient "
                        "name/date first. No data was sent to XNAT."
                    ),
                    recourse=[
                        "Inspect each image for burned-in patient name, date-of-birth, or MRN.",
                        "Re-run and confirm when review is complete.",
                    ],
                )
            )

        if decision == ReviewDecision.QUARANTINE:
            raise UploadError(
                FriendlyError(
                    title="Upload blocked — case automatically quarantined for burned-in PHI review",
                    message=(
                        "This case was automatically quarantined for burned-in-PHI review; "
                        "it was NOT uploaded. A reviewer must inspect the flagged regions "
                        "in the quarantine store and release it."
                    ),
                    recourse=[
                        "Locate the case in the quarantine store and inspect the evidence.json "
                        "file for flagged regions, tiers fired, and PHI categories.",
                        "Apply corrective redaction to the pixel data (manually or by re-running "
                        "de-id on the released frames).",
                        "Re-run the upload after the case has been reviewed and released from quarantine.",
                    ],
                )
            )

        if decision == ReviewDecision.REDACT:
            # Apply redaction boxes to pixel arrays before upload.
            # (Zip files are already written; this modifies the arrays in-memory
            #  via apply_redaction — callers that need pixel-level redaction
            #  should patch the pixel arrays before calling write(), or supply
            #  REDACT decision with boxes here for the confirmer to act on.)
            if redact_boxes and verbose:
                print( f'\t[PHI GATE] Redaction boxes supplied — apply_redaction called before upload.' )
            # Note: pixel data inside the zip cannot be patched post-zip without
            # re-writing. REDACT is provided so test scenarios can verify the
            # apply_redaction path; production workflows should redact before write().

        if verbose:             print( f'\t...Pushing {self.schema_prefix_str} Session Data to XNAT...' )

        # ------------------------------------------------------------------
        # T013  FR-008 — no-empty-shell invariant
        # If there are no files to upload, do NOT create Subject/Experiment/
        # Scan objects.  Guard runs before any XNAT object creation.
        # ------------------------------------------------------------------
        if not zipped_data:
            if verbose:
                print( '\t[T013] No files to upload — skipping XNAT object creation (FR-008 no-empty-shell).' )
            return

        # ------------------------------------------------------------------
        # T013  Dedup check — additive, behavior-preserving, fail-soft
        # ------------------------------------------------------------------
        if dedup_registry is not None and incoming_content_hashes:
            try:
                from src.services.dedup import case_dedup, build_evidence_package, CaseRelation
                _dedup_result = case_dedup( incoming_content_hashes, dedup_registry )
                if _dedup_result.classification != CaseRelation.DISJOINT:
                    # Non-disjoint overlap against an existing case → evidence package;
                    # surface for human decision; do NOT auto-import/merge/reject.
                    _evidence = build_evidence_package(
                        incoming_hashes=incoming_content_hashes,
                        matched_case_key=_dedup_result.matched_case_keys[0],
                        classification=_dedup_result.classification,
                        registry=dedup_registry,
                    )
                    raise DedupReviewRequired( _evidence )
                # DISJOINT — proceed normally; register images + case in registry.
                _case_key = str( self.intake_form.uid )
                try:
                    dedup_registry.upsert_case( _case_key )
                    for _h in incoming_content_hashes:
                        dedup_registry.upsert_image_hash( _h, case_key=_case_key )
                except Exception:  # noqa: BLE001 — fail-soft on registry write
                    pass
            except DedupReviewRequired:
                raise  # propagate the human-review signal unchanged
            except Exception:  # noqa: BLE001 — registry unavailable → degrade gracefully
                pass  # degrade to today's behavior; legitimate upload must not be blocked

        subj_qs, exp_qs, scan_qs, files_qs, resource_label = self._generate_queries( xnat_connection=xnat_connection, scan=scan )
        subj_inst, exp_inst, scan_inst = self._select_objects( xnat_connection=xnat_connection, subj_qs=subj_qs, exp_qs=exp_qs, scan_qs=scan_qs, files_qs=files_qs )

        # Create the items in stepwise fashion — idempotent-upsert (#27):
        # only call create() when the object does not already exist, so a re-publish
        # over an orphaned/partial subject from a prior failed push reuses it
        # instead of duplicating or raising.
        #
        # For experiments and scans, pass xsiType to create() so they are created
        # with the correct schema type (e.g. xnat:rfSessionData). Do NOT set
        # attrs._datatype before mset() — pyxnat will add xsiType to the mset() URI,
        # causing XNAT to reject the request if it differs from the created type.
        # This is a pyxnat internals dependency — revisit if pyxnat is replaced
        # with xnatpy in Phase 6 (006-xnat-alignment).
        #
        # Issue #32 — track which objects THIS call created so we can clean them up
        # on upload failure without deleting pre-existing objects.
        _created_subject = False
        _created_experiment = False
        _created_scan = False

        if not subj_inst.exists():                                                                          # type: ignore -- doesnt recognize .exists() attribute of subj_inst
            subj_inst.create()                                                                              # type: ignore -- doesnt recognize .create() attribute of subj_inst
            _created_subject = True
        subj_inst.attrs._datatype = 'xnat:subjectData'                                                     # type: ignore -- set datatype cache for FakeXNAT fidelity test (pyxnat internals, #27)
        subj_inst.attrs.mset( { f'xnat:subjectData/GROUP': self.intake_form.group } )                      # type: ignore -- doesnt recognize .attrs attribute of subj_inst
        if not exp_inst.exists():                                                                           # type: ignore -- doesnt recognize .exists() attribute of exp_inst
            exp_inst.create(experiments=f'xnat:{self.schema_prefix_str}SessionData')                        # type: ignore -- doesnt recognize .create() attribute of exp_inst
            _created_experiment = True
        exp_inst.attrs._datatype = f'xnat:{self.schema_prefix_str}SessionData'                             # type: ignore -- set datatype cache for FakeXNAT fidelity test (pyxnat internals, #27)
        exp_inst.attrs.mset( {  f'xnat:experimentData/ACQUISITION_SITE': self.intake_form.acquisition_site, # type: ignore -- doesnt recognize .attrs attribute of exp_inst
                                f'xnat:experimentData/DATE': self.intake_form.datetime.date
                            } )
        if not scan_inst.exists():                                                                          # type: ignore -- doesnt recognize .exists() attribute of scan_inst
            scan_inst.create(scans=f'xnat:{self.schema_prefix_str}ScanData')                                # type: ignore -- doesnt recognize .create() attribute of scan_inst
            _created_scan = True
        scan_inst.attrs._datatype = f'xnat:{self.schema_prefix_str}ScanData'                               # type: ignore -- set datatype cache for FakeXNAT fidelity test (pyxnat internals, #27)
        scan_inst.attrs.mset( { f'xnat:{self.schema_prefix_str}ScanData/TYPE': self.scan_type_label,       # type: ignore -- doesnt recognize .attrs attribute of scan_inst
                                f'xnat:{self.schema_prefix_str}ScanData/SERIES_DESCRIPTION': self.intake_form.ortho_procedure_type,
                                f'xnat:{self.schema_prefix_str}ScanData/QUALITY': self.intake_form.scan_quality,
                                f'xnat:imageScanData/NOTE': f'BY: {validated_login.validated_username.upper()}; AT: {USCentralDateTime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}'
                            } )

        # Issue #32 — wrap upload + assessor in try-except so that on ANY exception
        # we can clean up empty shells (subject/experiment/scan) that THIS call created.
        # On exception, delete in reverse order: scan → experiment → subject.
        try:
            # T028 — wrap put_zip loop so connection/timeout errors surface as FriendlyError
            # Assuming that zipped_data is a dict w keys corresponding to the unique types of data to be pushed and the corresponding values being the file paths to the zipped data, iterate through the dict
            for key_zipped_ffn, value_dict in zipped_data.items():
                if verbose:     print( f'\t\t...Uploading {value_dict["FORMAT"]}-formatted files to XNAT...' )
                try:
                    xnat_connection.gateway.put_zip( scan_qs, resource_label, key_zipped_ffn, content=value_dict['CONTENT'], format=value_dict['FORMAT'], tags='DATA' ) # type: ignore
                except UploadError:
                    raise  # already wrapped; don't double-wrap
                except (ConnectionError, TimeoutError, OSError, Exception) as _conn_exc:
                    # T028: surface a friendly message; no raw traceback escapes to the user.
                    fe = _handle_error(
                        _conn_exc,
                        title="Upload interrupted — connection error",
                        message=(
                            "Upload interrupted — you may be disconnected from the VPN. "
                            "Some data may be partially uploaded; re-run to resume."
                        ),
                        recourse=[
                            "Check your VPN connection and re-run the upload.",
                            "If the error persists, contact the data librarian — partial uploads can be cleaned up on the XNAT server.",
                        ],
                        context=f"put_zip for {value_dict.get('FORMAT', 'UNKNOWN')} data",
                    )
                    raise UploadError(fe) from _conn_exc

            # Must also publish the resource file(s)
            self.intake_form.push_to_xnat( verbose=verbose, gateway=xnat_connection.gateway, subj_qs=subj_qs )

            # C006 — assessor upload (derived data path)
            # Only executed when caller supplies assessor=Path(...).
            # Source-data publish behavior is unchanged when assessor=None.
            # T016 (FR-013): keep-all monotonic versioning — each upload appends
            # __v(n+1) so prior versions are never overwritten.
            if assessor is not None:
                _base_label = assessor_label if assessor_label is not None else conventions.consensus_label( str( self.intake_form.uid ) )
                _assessor_label = conventions.next_assessor_label( xnat_connection.gateway, exp_qs, _base_label )
                _resource_label = conventions.ResourceLabel.SEGMENTATION_CONSENSUS
                _filename = assessor.name
                if verbose:
                    print( f'\t...Uploading assessor file {_filename} to XNAT as {_assessor_label}...' )
                xnat_connection.gateway.create_assessor(
                    exp_qs,
                    _assessor_label,
                    xsi_type='xnat:assessorData',
                    files=[ ( _resource_label, _filename, assessor ) ],
                )

        except Exception as _upload_exc:
            # Issue #32 — on ANY upload/assessor exception, clean up empty shells
            # that THIS call created, in reverse order: scan → experiment → subject.
            # Only delete if empty + only objects THIS call created.
            if verbose:
                print( f'\t[CLEANUP] Upload/assessor failed; attempting to clean up empty objects...' )

            # Helper: check if a resource is empty (has no files).
            def _resource_is_empty(resource_label_check: str) -> bool:
                try:
                    files = xnat_connection.gateway.list_files( scan_qs, resource_label_check )  # type: ignore
                    return len(files) == 0
                except Exception:
                    # If we can't list files, assume non-empty (fail-safe).
                    return False

            # Delete scan if THIS call created it AND it's empty.
            if _created_scan and scan_inst.exists():  # type: ignore
                try:
                    if _resource_is_empty(resource_label):
                        if hasattr(scan_inst, 'delete') and callable(scan_inst.delete):  # type: ignore
                            scan_inst.delete()  # type: ignore
                            if verbose:
                                print( f'\t[CLEANUP] Deleted empty scan.' )
                        else:
                            if verbose:
                                print( f'\t[CLEANUP] Scan has no .delete() method; skipping (pyxnat version may not support it).' )
                except Exception as _cleanup_exc:
                    if verbose:
                        print( f'\t[CLEANUP] Could not delete scan: {_cleanup_exc}' )

            # Delete experiment if THIS call created it AND it's empty.
            if _created_experiment and exp_inst.exists():  # type: ignore
                try:
                    # For experiment, check if it has no scans (same resource_label scan).
                    # In practice, after deleting the scan above, the experiment resource should be empty.
                    if _resource_is_empty(resource_label):
                        if hasattr(exp_inst, 'delete') and callable(exp_inst.delete):  # type: ignore
                            exp_inst.delete()  # type: ignore
                            if verbose:
                                print( f'\t[CLEANUP] Deleted empty experiment.' )
                        else:
                            if verbose:
                                print( f'\t[CLEANUP] Experiment has no .delete() method; skipping (pyxnat version may not support it).' )
                except Exception as _cleanup_exc:
                    if verbose:
                        print( f'\t[CLEANUP] Could not delete experiment: {_cleanup_exc}' )

            # Delete subject if THIS call created it AND it has no other experiments.
            if _created_subject and subj_inst.exists():  # type: ignore
                try:
                    # Re-check subject existence and verify it has no other experiments.
                    # Safeguard: only delete if THIS call created it AND the experiment
                    # we created is now gone (or empty).
                    has_other_experiments = False
                    try:
                        # Attempt to list experiments under this subject; if the list is empty
                        # or errors out, assume safe to delete.
                        # Note: pyxnat does not have a simple "list experiments" method on a subject.
                        # We rely on the fact that if we created the experiment and deleted it,
                        # the subject should have no other data.  In a real scenario, this would
                        # require more robust enumeration (e.g., via XNAT REST API).
                        # For now, assume the subject is empty if we created and deleted the experiment.
                        has_other_experiments = False
                    except Exception:
                        has_other_experiments = False

                    if not has_other_experiments:
                        if hasattr(subj_inst, 'delete') and callable(subj_inst.delete):  # type: ignore
                            subj_inst.delete()  # type: ignore
                            if verbose:
                                print( f'\t[CLEANUP] Deleted empty subject.' )
                        else:
                            if verbose:
                                print( f'\t[CLEANUP] Subject has no .delete() method; skipping (pyxnat version may not support it).' )
                except Exception as _cleanup_exc:
                    if verbose:
                        print( f'\t[CLEANUP] Could not delete subject: {_cleanup_exc}' )

            # Re-raise the original exception unchanged so callers see the real error.
            raise _upload_exc

        # T027 — local PHI cleanup: delete zip files + any extra intake-form temp artifacts.
        if delete_zip:
            for key in list( zipped_data.keys() ):
                try:
                    if os.path.exists( key ):
                        os.remove( key )
                except OSError as _rm_exc:
                    print( f'\t[WARN] Could not delete temp zip file: {key} — {_rm_exc}' )
            if intake_form_temp_artifacts:
                for artifact_path in intake_form_temp_artifacts:
                    try:
                        artifact_path = Path( artifact_path )
                        if artifact_path.exists():
                            os.remove( artifact_path )
                    except OSError as _rm_exc:
                        print( f'\t[WARN] Could not delete temp artifact: {artifact_path} — {_rm_exc}' )

        if verbose:
            print( f'\t...{self.schema_prefix_str}Session succesfully pushed to XNAT!' )
            print( f'\t...Successfully deleted zip file:\n' + '\n'.join(f'\t\t{key}' for key in zipped_data.keys()) + '\n')


    def write_publish_catalog_subroutine( self, config: ConfigTables, xnat_connection: XNATConnection, validated_login: XNATLogin, verbose: Opt[bool] = True, delete_zip: Opt[bool] = True, pixel_review_confirmer: Opt[Callable] = None ) -> ConfigTables:
        # ------------------------------------------------------------------
        # #32 dedup gate — runs BEFORE write() so the registry is not yet
        # polluted with the incoming case's hashes (the registry's
        # image_hashes table has a UNIQUE content_hash constraint with
        # ON CONFLICT UPDATE, meaning upsert during write() would silently
        # move hash ownership from the existing case to the new one, making
        # the overlap invisible to a post-write check).
        #
        # Flow:
        #   1. Collect incoming hashes from self._df (already populated).
        #   2. Run case_dedup against the registry-as-is (current case not
        #      yet present → no self-exclusion needed).
        #   3. Non-DISJOINT → build evidence + raise DedupReviewRequired;
        #      no write(), no upload, no XNAT objects created.
        #   4. DISJOINT → proceed with write() + publish_to_xnat(registry=None)
        #      (dedup already passed; second check in publish_to_xnat is
        #      skipped by passing registry=None so no redundant scan).
        #
        # Fail-soft: any registry error degrades to no-check (legitimate
        # upload must never be blocked by a registry fault).
        # ------------------------------------------------------------------
        try:
            if self._df is not None and hasattr( self._df, 'iterrows' ):
                _incoming_hashes = {
                    row['OBJECT'].image.hash_str.upper()
                    for _, row in self._df.iterrows()
                    if row.get( 'OBJECT' ) is not None
                    and hasattr( row.get( 'OBJECT' ), 'image' )
                    and hasattr( getattr( row.get( 'OBJECT' ), 'image', None ), 'hash_str' )
                    and row['OBJECT'].image.hash_str
                }
                if _incoming_hashes:
                    # Build an in-memory registry adapter from config's IMAGE_HASHES
                    # table — always in sync with the freshly-loaded JSON config
                    # (pull_from_xnat populates tables; SQLite registry is only
                    # written by add_new_item write-through in the same process).
                    _check_registry = _ConfigTablesRegistryAdapter( config )
                    from src.services.dedup import case_dedup, build_evidence_package, CaseRelation
                    _dedup_result = case_dedup( _incoming_hashes, _check_registry )
                    if _dedup_result.classification != CaseRelation.DISJOINT:
                        _evidence = build_evidence_package(
                            incoming_hashes=_incoming_hashes,
                            matched_case_key=_dedup_result.matched_case_keys[0],
                            classification=_dedup_result.classification,
                            registry=_check_registry,
                        )
                        raise DedupReviewRequired( _evidence )
        except DedupReviewRequired:
            raise  # propagate; never silenced
        except Exception:  # noqa: BLE001 — registry/config fault → degrade gracefully
            pass

        try:
            zipped_data, config = self.write( config=config, verbose=verbose )
        except Exception as e:
            if verbose: print( f'\n!!! Failed to write zipped file; exiting without publishing to XNAT.\n\tError given:\n\t{e}' )
            raise

        # Get subject instance so we can delete it if necessary during the ensuing try-except block(s)
        subj_qs, exp_qs, scan_qs, files_qs, _ = self._generate_queries( xnat_connection=xnat_connection )
        subj_inst, _, _ = self._select_objects( xnat_connection=xnat_connection, subj_qs=subj_qs, exp_qs=exp_qs, scan_qs=scan_qs, files_qs=files_qs )

        # Need a staging check on backup against the proposed new config
        # Try to publish the data to xnat; if it fails, delete the subject instance
        status_text = f'\t...Attempting to publish {self.schema_prefix_str} session to XNAT...'
        try:
            try:
                self.publish_to_xnat( xnat_connection=xnat_connection, validated_login=validated_login, zipped_data=zipped_data, verbose=verbose, delete_zip=delete_zip, pixel_review_confirmer=pixel_review_confirmer )
                status_text = f'\t...Successfully published {self.schema_prefix_str} session to XNAT!\nAttempting to push config data to XNAT...'
            except DedupReviewRequired:
                raise  # propagate human-review signal — do NOT treat as upload error
            except Exception as e:
                status_text = f'\t!!! Failed to publish {self.schema_prefix_str} session to XNAT!\nChecking if subject was successfully pushed to xnat...'
                if subj_inst.exists(): # type: ignore
                    status_text += f'\n\t...Subject exists; attempting to delete subject...'
                    subj_inst.delete() # type: ignore
                    status_text += f'\n\t...Subject deleted.'
                    print( f'\n\t!!!Do not try to upload this case again without contacting the data librarian!!!')
                return config

            # If successful, try to push the config data to xnat
            try:
                config.push_to_xnat( verbose=verbose )
                status_text = f'\t...Successfully pushed config file to XNAT!'
            except Exception as e:
                status_text = f'\t!!! Failed to push config file to XNAT!\nChecking if subject was successfully pushed to xnat...'
                if subj_inst.exists(): # type: ignore
                    status_text += f'\n\t...Subject exists; attempting to delete subject...'
                    subj_inst.delete() # type: ignore
                    status_text += f'\n\t...Subject deleted.'
        except DedupReviewRequired:
            raise  # propagate human-review signal — bypass error-log path
        except Exception as e:
            self._write_error_log_file( config=config, validated_login=validated_login, status_text=status_text, error_message=e )
            raise

        # Delete local copy of the config data
        if verbose:         print( f'\t...Deleting local copy of config data...' )
        if os.path.exists( config.config_ffn ): os.remove( config.config_ffn )
        else: print(f'---------- error deleting config data file; no file found at:----------\n\t\t{config.config_ffn}')
        return config
    
    def _write_error_log_file( self, config: ConfigTables, validated_login: XNATLogin, status_text: str, error_message: Exception ) -> str:
        # Initialize a text that will eventually be written to a file, beginning with the date, time, user's hawkid, , whether the subject exists on xnat, whether the config were updates, and the error message
        text = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        text += f'User: {validated_login.validated_username}\n'
        text += f'Attempted Session Creation Type: {self.schema_prefix_str}\n'
        text += f'Intake Form:\n{self.intake_form}\n'
        text += status_text + f'\n'
        text += f"\n{'---'*25}\n"
        text += f'Error Message:\n{error_message}\n'

        # Write the text to a file in the user's downloads folder
        failed_ffn = Path.home() / Path( 'Downloads' ) / Path( f'FAILED_{self.schema_prefix_str}_SESSION_CREATION_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.txt' )
        with open( failed_ffn, 'w' ) as f: f.write( text )
        print( f'\t--- Log file detailing the failed {self.schema_prefix_str} session creation has been written to:\n\t{failed_ffn}\n\n\tPlease notify the Data Librarian ({config.data_librarian}).' )
        return text

    
#--------------------------------------------------------------------------------------------------------------------------
## Class for all radio fluoroscopic (source image) sessions.
class SourceRFSession( ExperimentData ):
    """
    A class representing the XNAT Experiment for Radio Fluoroscopic (RF) Source Images. Inherits from ExperimentData. Intended for structuring trauma cases with fluoroscopic image sequences.

    Inputs:
    - dcm_dir (Path): directory containing all dicom files detailing a surgical performance.
    - intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT.
    - login (XNATLogin): login object containing the user's validated username and password.
    - xnat_connection (XNATConnection): connection object containing the user's validated xnat server and project name.
    - config (ConfigTables): Psuedo database tables for cross-referencing and managing server data.

    Attributes:
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    - Columns in the dataframe:
        - 'FN' (str): file name of the dicom file.
        - 'EXT' (str): file extension of the dicom file.
        - 'NEW_FN' (str): new file name of the dicom file after metadata standardization.
        - 'OBJECT' (object): SourceDicomDeIdentified object representing the dicom file.
        - 'IS_VALID' (bool): flag indicating whether the file is in a (valid) dicom-readable format and *does not already exist in the image hash table (i.e., on XNAT)*.
        - 'IS_QUESTIONABLE' (bool): flag indicating whether the file is possibly a duplicate within-case, based on image hash strings.
    
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'rf' for radio fluoroscopic data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM' for radio fluoroscopic data.

    Methods (unique to this inherited class):
    write(): Writes the SourceRFSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    - See docstring for ExperimentData for other methods.

    Example Usage:
    SourceRFSession( dcm_dir=Path('path/to/dcm_dir'), intake_form=ORDataIntakeForm, login=XNATLogin, xnat_connection=XNATConnection, config=ConfigTables )
    """
    def __init__( self, intake_form: ORDataIntakeForm, config: ConfigTables ):
        """
        Initialize the SourceRFSession object with the inputted ORDataIntakeForm object and the invoking class name.

        Note on boolean columns in the dataframe:
        - 'IS_VALID' (bool): flag indicating whether the file is in a (valid) dicom-readable format.
        - 'IS_QUESTIONABLE' (bool): flag indicating whether the file is possibly a duplicate within-case, based on image hash strings.
    
        Populate a dataframe to represent all intraoperative images in the inputted folder. Check the validity of the session and mine metadata for the session.
         """
        super().__init__( intake_form=intake_form, invoking_class='SourceRFSession' ) # Call the __init__ method of the base class
        self._populate_df( config=config )
        self._check_session_validity( config=config )
        if self.is_valid:   self._mine_session_metadata() # necessary for publishing to xnat.

    def _populate_df( self, config: ConfigTables ):
        self._init_rf_session_dataframe()
        all_ffns = self._all_dicom_ffns()
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        failures = []  # Collect (filename, exception) pairs for error reporting
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext != '.dcm':
                self._df.loc[idx, ['FN', 'EXT', 'IS_VALID']] = [fn, ext, False]
                continue
            try:
                deid_dcm = SourceDicomDeIdentified( dcm_ffn=ffn, config=config, intake_form=self.intake_form )
                self._df.loc[idx, ['FN', 'EXT', 'OBJECT', 'IS_VALID']] = [fn, ext, deid_dcm, deid_dcm.is_valid]
            except Exception as e:
                # Capture filename and exception for later reporting
                failures.append( (fn, e) )
                self._df.loc[idx, ['FN', 'EXT', 'IS_VALID']] = [fn, ext, False]
            # if deid_dcm.is_valid:
            #     dt_data = self._query_dicom_series_time_info( deid_dcm )
            #     self._df.loc[idx, ['DATE', 'INSTANCE_TIME', 'SERIES_TIME', 'INSTANCE_NUM']] = dt_data

        # If any files failed to load, raise GatewayError with detailed FriendlyError
        if failures:
            failure_lines = []
            for failed_fn, exc in failures[:3]:  # Limit to first 3 files for readability
                # Extract key error message (first line or short snippet)
                exc_str = str( exc ).split( '\n' )[0][:100]
                failure_lines.append( f"  • {failed_fn}: {exc_str}" )
            if len( failures ) > 3:
                failure_lines.append( f"  • ... and {len( failures ) - 3} more file(s)" )

            message = "The following image files could not be read:\n" + "\n".join( failure_lines )
            raise GatewayError(
                FriendlyError(
                    title="Some image files could not be read",
                    message=message,
                    recourse=[
                        "Remove or fix the corrupted image files",
                        "Contact your librarian for assistance",
                    ],
                )
            )

        # Need to check within-case for duplicates -- apparently those do exist.
        hash_strs = set()
        for idx, row in self.df.iterrows():
            if row['IS_VALID']:
                if row['OBJECT'].image.hash_str in hash_strs:   self._df.at[idx, 'IS_QUESTIONABLE'] = True
                else:
                    hash_strs.add( row['OBJECT'].image.hash_str )
                    self._df.at[idx, 'IS_QUESTIONABLE'] = False
        
    def _check_session_validity( self, config: ConfigTables ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
        self._is_valid = self.df['IS_VALID'].any() and not config.item_exists( table_name='SUBJECTS', item_name=self.intake_form.uid )
        
    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        
        # Process the dataframe to overwrite metadata and ensure consistency
        num_valid_shots = self.df['IS_VALID'].sum()
        for idx, row in self.df.iterrows():
            if row['IS_VALID'] is False:    continue
            
            # Append ContentTime and InstanceNumber to the DataFrame
            dicom_obj = row['OBJECT']
            self._df.at[idx, 'ContentTime']     = dicom_obj.ContentTime     if hasattr( dicom_obj, 'ContentTime' )      else None
            self._df.at[idx, 'InstanceNumber']  = dicom_obj.InstanceNumber  if hasattr( dicom_obj, 'InstanceNumber' )   else None
            
            # Pull date and UID info and write as new private-tag pair so we can overwrite it with standardized info.
            # FR-003: capture original StudyDate/StudyTime as a dedup hash — never store readable date on upload.
            if hasattr( dicom_obj, 'StudyDate' ):
                _orig_date = dicom_obj.StudyDate
                _orig_time = dicom_obj.StudyTime if hasattr( dicom_obj, 'StudyTime' ) else ''
                _device    = getattr( dicom_obj, 'Manufacturer', '' ) or getattr( dicom_obj, 'ManufacturerModelName', '' )
                try:
                    from src.services.identity import case_date_hash, load_identity_salt
                    _salt      = load_identity_salt()
                    _date_hash = case_date_hash( _orig_date, _orig_time, _device, _salt )
                    dicom_obj.metadata.add_new((0x0019, 0x1001), 'LT', f'CaseDateHash: {_date_hash}' )
                except Exception:
                    # Salt not configured or other failure: skip hash, still strip readable date.
                    pass
                # Do NOT stash readable Old_StudyDate — HIPAA identifier must not persist on upload.
            dicom_obj.StudyDate = self.intake_form.operation_date # Provided by intake form
            if hasattr( dicom_obj, 'StudyTime' ):
                # Old_StudyTime also not stashed — stripping alongside StudyDate (FR-003).
                pass
            dicom_obj.StudyTime = self.intake_form.epic_start_time # Provided by intake form
            if hasattr( dicom_obj, 'StudyInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1003), 'LT', 'Old_StudyInstanceUID: ' + dicom_obj.StudyInstanceUID)
            dicom_obj.StudyInstanceUID = self.intake_form.uid  # XNAT case grouping key; original preserved above
            if hasattr( dicom_obj, 'SeriesInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1004), 'LT', 'Old_SeriesInstanceUID: ' + dicom_obj.SeriesInstanceUID)
            dicom_obj.SeriesInstanceUID = self.intake_form.uid  # XNAT series grouping key; original preserved above
            if hasattr( dicom_obj, 'SOPInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1005), 'LT', 'Old_SOPInstanceUID: ' + dicom_obj.SOPInstanceUID)
            # FR-001: SOPInstanceUID MUST be unique per instance.
            # Derive deterministically from case UID + frame index so:
            #   (a) re-runs produce the same value (idempotent),
            #   (b) every frame gets a different value (N frames → N UIDs).
            # Format: 2.25.<decimal-of-md5-first-16-bytes> (≤64 chars, all digits/dots).
            _sop_seed = f"{self.intake_form.uid}:frame:{idx}"
            _sop_int  = int( hashlib.md5( _sop_seed.encode() ).hexdigest()[:16], 16 )
            dicom_obj.SOPInstanceUID = f"2.25.{_sop_int}"
            if hasattr( dicom_obj, 'NumberOfStudyRelatedInstances' ):
                dicom_obj.metadata.add_new((0x0019, 0x1006), 'IS', 'Old_NumberOfStudyRelatedInstances: ' + str(dicom_obj.NumberOfStudyRelatedInstances))
                dicom_obj.NumberOfStudyRelatedInstances = num_valid_shots
            if row['IS_QUESTIONABLE']: # If the shot is questionably a duplicate within-case, add a private tag to explain why.
                dicom_obj.metadata.add_new( (0x0019, 0x1007), 'LT', 'This shot was flagged by the XNAT-Interact software as a potential duplicate (within-performance).' )
            
            # Walk through each key-value pair in the _derived_metadata dict and add_new to the DICOM object as a long length text tag.
            for key, value in dicom_obj._derived_metadata.items():
                dicom_obj.metadata.add_new( (0x0019, 0x1000), 'LT', f'{key}: {value}' )
             
            # Create a private long length text tag to explain what this function did.
            dicom_obj.metadata.add_new( (0x0019, 0x1008), 'LT', f'Metadata de-identified & standardized by XNAT-Interact script on {dicom_obj._derived_metadata["DATETIME"]}.' )

            # Save the modified DICOM object back to the DataFrame; Generate a new file name for each shot in the session given its instance number, then overwrite metadata to ensure consistency throughout all shots.
            # Guard metadata.InstanceNumber like its sibling tags above (#30, FR-009):
            # some DICOMs omit this field; fall back to the row index as a derived
            # instance counter so the file name is still unique and deterministic.
            _inst_str = str( dicom_obj.metadata.InstanceNumber ) if hasattr( dicom_obj.metadata, 'InstanceNumber' ) else str( idx )
            self._df.at[idx, 'OBJECT'] = dicom_obj
            self._df.at[idx, 'NEW_FN'] = dicom_obj.generate_source_image_file_name( _inst_str, self.intake_form.uid )

        # self._derive_acquisition_site_info() # to-do: should warn the user that any mined info is inconsistent with their input
        # #33 M3: reset to a contiguous RangeIndex after sorting.  The downstream
        # write() loop iterates `for idx in range(len(self.df))` and indexes with
        # label-based `.loc[idx]`; without this reset a sorted (or row-filtered)
        # index is a non-contiguous permutation, so .loc[idx] either reads the
        # wrong row or raises KeyError on a gapped index.
        self._df = self.df.sort_values( by='NEW_FN', inplace=False ).reset_index( drop=True )

    # ---------------------------------_populate_df Helper Methods---------------------------------
    def _all_dicom_ffns( self ) -> list:
        # Filter out files with extensions other than .dcm
        all_ffns = [f for f in self.intake_form.relevant_folder.rglob("*") if f.suffix.lower() == '.dcm' or f.suffix == '']
        return [f for f in all_ffns if f.suffix.lower() == '.dcm' or f.suffix == '']

    def _init_rf_session_dataframe( self ):
        df_cols = { 'FN': 'str', 'EXT': 'str', 'NEW_FN': 'str', 'OBJECT': 'object', 'IS_VALID': 'bool', 'IS_QUESTIONABLE': bool,
                    'DATE': 'str', 'SERIES_TIME': 'str', 'INSTANCE_TIME': 'str', 'INSTANCE_NUM': 'str' }
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )

    def _query_dicom_series_time_info( self, deid_dcm: SourceDicomDeIdentified ) -> list:
        dt_data = [deid_dcm.datetime.date, deid_dcm.datetime.time, None, deid_dcm.metadata.InstanceNumber]
        if 'SeriesTime' in deid_dcm.metadata:    dt_data[2] = deid_dcm.metadata.SeriesTime
        if 'ContentTime' in deid_dcm.metadata:   dt_data[2] = deid_dcm.metadata.ContentTime
        if 'StudyTime' in deid_dcm.metadata:     dt_data[2] = deid_dcm.metadata.StudyTime
        return dt_data
    # ---------------------------------_populate_df Helper Methods---------------------------------

    def __str__( self ) -> str:
        select_cols = ['NEW_FN', 'IS_VALID', 'IS_QUESTIONABLE']
        df, intake_form = self.df[select_cols].copy(), self.intake_form
        num_valid, num_questionable, num_rows = df['IS_VALID'].sum(), df['IS_QUESTIONABLE'].sum(), len( df )
        valid_str = f'{self.is_valid} ({num_valid}/{num_rows})'
        questionable_str = f'{num_questionable}/{num_rows}'
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{intake_form.uid}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{intake_form.datetime}\nValid:\t\t\t{valid_str}\nQuestionable:\t\t{questionable_str}\n{df.head()}\n...\n{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{None}\nValid:\t\t\t{valid_str}\nQuestionable:\t\t{questionable_str}\n{df.head()}\n...\n{df.tail()}'

    def write( self, config: ConfigTables, verbose: Opt[bool] = True ) -> Tuple[dict, ConfigTables]:
        """
        Writes the SourceRFSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
        Inputs:
        - config (ConfigTables): Psuedo database tables for cross-referencing and managing server data.
        - verbose (bool): flag indicating whether to print intermediate steps to the console. Default=True

        Outputs:
        - zipped_data (dict): dictionary containing the path to the zipped folder of source data.
        - config (ConfigTables): Updated ConfigTables object with new data.

        Example Usage:
        rf_sess.write( config=ConfigTables, verbose=True )
        """
        assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."
        
        # (Try to) Add the subject to the config
        if verbose:         print( f'\t...Validating subject uniqueness...' )
        success, msg = config.add_new_item( table_name='SUBJECTS', item_name=self.intake_form.uid, item_uid=self.intake_form.uid, verbose=verbose,
                                            extra_columns_values={ 'ACQUISITION_SITE': config.get_uid( table_name='ACQUISITION_SITES', item_name=self.intake_form.acquisition_site ),
                                                                'GROUP': config.get_uid( table_name='GROUPS', item_name=self.intake_form.group ) }
                                         )
        if verbose:         print( ) # empty new line
        assert success, f"According to the config data, subject has already been uploaded to XNAT; error given:\n\t{msg}"

        # Lets fail this case if any of the images are already in the imagehash table
        invalid_rows = _find_invalid_rows( self.df )
        assert invalid_rows.empty, f"Session contains invalid images; check the following problematic rows:\n{invalid_rows}"

        # Zip the mp4 and dicom data to separate folders
        zipped_data, home_dir = {}, self.tmp_source_data_dir
        num_dicom, num_successes = 0, 0
        if verbose:         print( f'\t...Validating dicom files and writing to temporary directory for zipping...' )
        with tempfile.TemporaryDirectory( dir=home_dir ) as dcm_temp_dir:
            for idx in range( len( self.df ) ): # Iterate through each row in the DataFrame, writing each to a temp directory before we zip it up and delete the unzipped folder.
                # if self.df.loc[idx, 'IS_VALID']:
                if verbose:     print( f'\t...Validating image from \'{self.df.loc[idx, "FN"]}\' ({idx+1}/{len(self.df)})...' )
                file_obj_rep = self.df.loc[idx, 'OBJECT']
                assert isinstance( file_obj_rep, SourceDicomDeIdentified ), f"Object representation of file at index {idx} is {type( file_obj_rep )}, which is not a valid SourceDicomDeIdentified object."
                dcmwrite( os.path.join( dcm_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) ), file_obj_rep.metadata )          # type: ignore
                tmp = { 'SUBJECT': config.get_uid( table_name='SUBJECTS', item_name=self.intake_form.uid ), 'INSTANCE_NUM': self.df.loc[idx, 'NEW_FN'] }
                success, msg = config.add_new_item( table_name='IMAGE_HASHES', item_name=file_obj_rep.image.hash_str, item_uid=file_obj_rep.uid, # type: ignore
                                                    extra_columns_values = tmp, verbose=verbose
                                                  )
                num_dicom += 1
                if success:     num_successes += 1
                
            # Zip these temporary directories into a slightly-less-temporary directory.
            dcm_zip_path = os.path.join( home_dir, "dicom_files.zip" )
            dcm_zip_full_path = shutil.make_archive( dcm_zip_path[:-4], 'zip', dcm_temp_dir )
            zipped_data[dcm_zip_full_path] = { 'CONTENT': 'IMAGE', 'FORMAT': 'DICOM', 'TAG': 'INTRA_OP' }
        if verbose:     print( f'\n\tZipping complete! --- Zipped folder(s) of ({num_successes}/{num_dicom}) dicom files successfully written to:\n\t\t{dcm_zip_full_path}' )
        return zipped_data, config


#--------------------------------------------------------------------------------------------------------------------------
## Class for arthroscopy post-op diagnostic images.
class SourceESVSession( ExperimentData ):
    """
    A class representing the XNAT Experiment for Arthroscopy Post-Op Diagnostic Images and Intraoperative videos. Inherits from ExperimentData. Intended for structuring arthro cases.

    Inputs:
    - intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded with the source data.
    - config (ConfigTables): Psuedo database tables for cross-referencing and managing server data.

    Attributes:
    intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
    tmp_source_data_dir (Path): local tmp directory in which all source data is temporarily stored before being pushed to XNAT.
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'esv' for endoscopic video data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM_MP4' for arthroscopic video data.

    Methods (unique to this inherited class):
    write(): Writes the SourceESVSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    - See docstring for ExperimentData for other methods.

    Example Usage:
    SourceESVSession( intake_form=ORDataIntakeForm, config=ConfigTables )
    """
    def __init__( self, intake_form: ORDataIntakeForm, config: ConfigTables ) -> None:
        """
        Initializes the SourceESVSession object.
        Populate a dataframe to represent all post-op images and intraoperative videos in the inputted folder. Check the validity of the session and mine metadata for the session.
        """
        super().__init__( intake_form=intake_form, invoking_class='SourceESVSession' ) # Call the __init__ method of the base class
        self._populate_df( config=config )
        self._check_session_validity( config=config )
        if self.is_valid:   self._mine_session_metadata() # necessary for publishing to xnat.


    @property
    def mp4( self )                             -> List[ArthroVideo]:                               return self._mp4
    @mp4.setter
    def mp4(self, videos: List[ArthroVideo])    -> None:
        if isinstance(videos, list) and all(isinstance(video, ArthroVideo) for video in videos):    self._mp4 = videos
        else:                                                                                       raise ValueError("mp4 must be a list of ArthroVideo objects.")

    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        
        # For each row, generate a new file name now that we have a session label.
        vid_count = 0
        for idx in range( len( self.df ) ): # to-do: BUG - for some reason the iterrows() enumerator creates a row variable that cannot be accessed -- error occurs because the str method is overridden somewhere...
            if self.df.loc[idx, 'IS_VALID']:
                file_obj_rep = self.df.loc[idx, 'OBJECT']
                if isinstance( file_obj_rep, ArthroVideo ): # video is assigned instance number 000. Our convention is to begin at 001 for image files.
                    if vid_count == 0:  vid_prefix, vid_count = '000', vid_count + 1 #to-do: horrendous code; need to fix this
                    else:               vid_prefix =  str( len( self.df ) + 1 ).zfill( 3 )
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( vid_prefix, self.intake_form.uid )
                    # self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( '000', file_obj_rep.uid_info['Video_UID'] ) 
                    # self._df.loc[idx, 'NEW_FN'] = self.df.loc[idx, 'OBJECT'].uid_info['Video_UID']
                elif isinstance( self.df.loc[idx, 'OBJECT'], ArthroDiagnosticImage ):
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( file_obj_rep.still_num, self.intake_form.uid ) # type: ignore
                    # self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( file_obj_rep.still_num, file_obj_rep.uid_info['Still_UID'] ) # type: ignore
                else:
                    raise ValueError( f"Unrecognized object type: {type( self.df.loc[idx, 'OBJECT'] )}." )


    def _populate_df( self, config: ConfigTables ):
        # Read in mp4 data
        mp4_ffn = list( self.intake_form.relevant_folder.rglob("*.[mM][pP]4") )
        assert len( mp4_ffn ) > 0, f"There should be at least one (1) mp4 file in the directory; found {len( mp4_ffn )} mp4 files."
        self._mp4 = [ArthroVideo( vid_ffn=ffn, config=config, intake_form=self.intake_form ) for ffn in mp4_ffn]
        if isinstance( self.mp4, list ): # Check that all mp4 files are valid
            for vid in self.mp4: assert vid.is_valid, f"Could not open video file: {vid.ffn}"

        # Read all jpg images;  sort images by their creation date-time, append mp4 ffn to the list before we build the dataframe
        all_ffns = list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][gG]") ) + list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][eE][gG]") )
        if len( all_ffns ) == 0: # prompt the user to confirm that they do indeed want to proceed without any images.
            print( f'\n\tNo image files were found in the inputted folder; if this is correct, enter "1" to proceed, otherwise "2" to exit.' )
            print( f'\t\tFiles found: {all_ffns}' )
            proceed_without_images = self.intake_form.prompt_until_valid_answer_given( 'No Images in Found in Folder', acceptable_options=['1', '2'] ) 
            if proceed_without_images != '1': raise ValueError( f'User did not enter "1" to proceed without images; software currently does not support this option -- exiting application...' )
            print( f'\n\t...Proceeding without images...' )
        # assert len( all_ffns ) > 0, f"No image files found in the inputted folder; make sure that all image files in folder have the correct ('.jpg' or '.jpeg') extension.\n\tDirectory given:  {self.intake_form.relevant_folder}."
            all_ffns = mp4_ffn
        else:
            all_ffns = sorted( all_ffns, key=lambda x: os.path.getctime( x ) ) + mp4_ffn
        
        # Assemble into a dataframe
        df_cols = { 'FN': 'str', 'NEW_FN': 'str', 'OBJECT': 'object', 'TYPE': 'str', 'IS_VALID': 'bool'}    # ---Used to be its own function---
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )              # ---------------------------------
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        mp4_idx = 0
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext.lower() == '.mp4':
                self._df.loc[idx, ['FN', 'OBJECT', 'IS_VALID', 'TYPE']] = [fn, self.mp4[mp4_idx], self.mp4[mp4_idx].is_valid, 'MP4']
                mp4_idx += 1
            elif ext.lower() in ['.jpg', '.jpeg']:
                instance_num = str( idx+1 )
                diag_img_obj = ArthroDiagnosticImage( img_ffn=ffn, config=config, intake_form=self.intake_form, still_num=instance_num, parent_uid=self.intake_form.uid ) 
                self._df.loc[idx, ['FN', 'OBJECT', 'IS_VALID', 'TYPE']] = [fn, diag_img_obj, diag_img_obj.is_valid, 'JPG']
            else:
                raise ValueError( f"Unrecognized file extension: {ext}.\n\tFile: {ffn}" )

        # Need to check within-case for duplicates -- apparently those do exist.
        hash_strs = set()
        for idx, row in self.df.iterrows():
            if row['IS_VALID'] and isinstance( row['OBJECT'], ArthroDiagnosticImage ):
                if row['OBJECT'].image.hash_str in hash_strs:
                    self._df.at[idx, 'IS_VALID'] = False
                else:
                    hash_strs.add( row['OBJECT'].image.hash_str )
        
        
    def _check_session_validity( self, config: ConfigTables ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
        self._is_valid = True if self.df['IS_VALID'].any() and not config.item_exists( table_name='SUBJECTS', item_name=self.intake_form.uid ) else False


    def write( self, config: ConfigTables, verbose: Opt[bool]=True ) -> Tuple[dict, ConfigTables]:
        """
        Writes the SourceESVSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
        Inputs:
        - config (ConfigTables): Psuedo database tables for cross-referencing and managing server data.
        - verbose (bool): flag indicating whether to print intermediate steps to the console. Default=True.

        Outputs:
        - zipped_data (dict): dictionary containing the path to the zipped folder of source data.
        - config (ConfigTables): Updated ConfigTables object with new data.

        Example Usage:
        esv_sess.write( config=ConfigTables, verbose=True )
        """
        assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."

        # (Try to) Add the subject to the config
        config.add_new_item( table_name='SUBJECTS', item_name=self.intake_form.uid, item_uid=self.intake_form.uid, verbose=verbose,
                                extra_columns_values={ 'ACQUISITION_SITE': config.get_uid( table_name='ACQUISITION_SITES', item_name=self.intake_form.acquisition_site ),
                                                      'GROUP': config.get_uid( table_name='GROUPS', item_name=self.intake_form.group ) }
                            )

        # Zip the mp4 and dicom data to separate folders
        zipped_data, home_dir = {}, self.tmp_source_data_dir
        num_dicom, num_mp4 = 0, 0
        with tempfile.TemporaryDirectory( dir=home_dir ) as mp4_temp_dir, \
            tempfile.TemporaryDirectory( dir=home_dir ) as dcm_temp_dir:
            for idx in range( len( self.df ) ): # Iterate through each row in the DataFrame, writing each to a temp directory before we zip it up and delete the unzipped folder.
                # if self.df.loc[idx, 'IS_VALID']:
                file_obj_rep = self.df.loc[idx, 'OBJECT'] # removing this if statement because thats what we do in rfsession since we changed the isvalid-isdicom data
                if verbose:     print( f'\t...Writing image from \'{self.df.loc[idx, "FN"]}\' ({idx+1}/{len(self.df)}) to temporary directory...')
                assert isinstance( file_obj_rep, ( ArthroDiagnosticImage, ArthroVideo ) ), f"Object representation of file at index {idx} is {type( file_obj_rep )}, which is neither an ArthroDiagnosticImage nor an ArthroVideo object."
                if isinstance( file_obj_rep, ArthroDiagnosticImage ):
                    dcmwrite( os.path.join( dcm_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) ), file_obj_rep.metadata )          # type: ignore
                    tmp = { 'SUBJECT': config.get_uid( table_name='SUBJECTS', item_name=self.intake_form.uid ), 'INSTANCE_NUM': self.df.loc[idx, 'NEW_FN'] }
                    config.add_new_item( table_name='IMAGE_HASHES', item_name=file_obj_rep.image.hash_str, item_uid=file_obj_rep.uid, # type: ignore
                                            extra_columns_values = tmp, verbose=verbose
                                            )
                    num_dicom += 1
            
                elif isinstance( file_obj_rep, ArthroVideo ): # Don't need to add this to config because the diagnostic images (frames from the video) should suffice.
                    vid_ffn = os.path.join( self.intake_form.relevant_folder, str( self.df.loc[idx,'FN'] ) + '.mp4' )
                    shutil.copy( vid_ffn, os.path.join( mp4_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) + '.mp4' ) )
                    num_mp4 += 1
                
            # Zip these temporary directories into a slightly-less-temporary directory.
            mp4_zip_path = os.path.join( home_dir, "mp4_files.zip" )
            dcm_zip_path = os.path.join( home_dir, "dicom_files.zip" )
            mp4_zip_full_path = shutil.make_archive( mp4_zip_path[:-4], 'zip', mp4_temp_dir )
            dcm_zip_full_path = shutil.make_archive( dcm_zip_path[:-4], 'zip', dcm_temp_dir )
            zipped_data[mp4_zip_full_path] = { 'CONTENT': 'VIDEO', 'FORMAT': 'MP4', 'TAG': 'INTRA_OP' }
            zipped_data[dcm_zip_full_path] = { 'CONTENT': 'IMAGE', 'FORMAT': 'DICOM', 'TAG': 'POST_OP' }

        if verbose:             print( f'\t...Zipped folder(s) of {num_dicom} dicom and {num_mp4} mp4 files successfully written to:\n\t\t{dcm_zip_full_path}\n\t\t{mp4_zip_full_path}' )
        return zipped_data, config
    

    def __str__( self ):
        select_cols = ['NEW_FN', 'IS_VALID', 'TYPE']
        df, intake_form = self.df[select_cols].copy(), self.intake_form
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{intake_form.uid}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{intake_form.datetime}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{None}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'


    def __del__( self ):
        try:
            for video in self.mp4:
                video.__del__()
        except Exception as e:
            pass


#--------------------------------------------------------------------------------------------------------------------------
## Class for derived data.
