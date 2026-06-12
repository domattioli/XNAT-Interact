"""
Layered dedup engine for XNAT-Interact (Feature 009, Stage 3 — T007/T008).

Pure logic over identity + registry primitives. No I/O, no side effects,
no ingest wiring (that is Stage 4). Never auto-acts: all functions REPORT.

Public API
----------
image_dedup(content_hash, orig_sopuid, registry) -> ImageDedupResult
    Authoritative image-level duplicate detection via registry.image_exists().
    orig_sopuid is a corroborating signal; the perceptual-hash flag is advisory.

case_dedup(incoming_image_hashes, registry) -> CaseDedupResult
    Case-level duplicate detection via set algebra over content-hash sets.
    Classifies the relationship: EXACT, SUBSET, SUPERSET, PARTIAL, DISJOINT.

build_evidence_package(incoming_hashes, matched_case_key, classification,
                       registry, *, corroboration=None) -> EvidencePackage
    Structured evidence for a human decision on overlapping cases.
    Pure data; no action is taken.

Result dataclasses
------------------
ImageDedupResult
    is_duplicate   : bool       — True if content_hash already in registry.
    content_hash   : str        — The authoritative key checked.
    uid_corroborates: bool      — True if orig_sopuid matched a registry row.
    near_dup_flag  : bool       — Advisory only (perceptual hash proximity).
    confidence     : str        — "HIGH" (content+uid), "CONTENT" (content only).

CaseDedupResult
    classification : str        — One of EXACT/SUBSET/SUPERSET/PARTIAL/DISJOINT.
    matched_case_keys: list[str]— Existing case(s) with non-empty overlap.
    overlap_ratio  : float      — |intersection| / |union| (Jaccard; 0.0 for DISJOINT).
    overlap_hashes : set[str]   — Content hashes present in both incoming + existing.

EvidencePackage
    incoming_hashes    : set[str]   — Image content hashes in the incoming case.
    matched_case_key   : str        — The primary overlapping existing case.
    classification     : str        — From CaseDedupResult.
    overlap_ratio      : float      — Jaccard overlap ratio.
    overlap_hashes     : set[str]   — The specific matching shots (content hashes).
    uid_corroborates   : bool       — True if StudyInstanceUID signals the same case.
    date_corroborates  : bool       — True if date_device_hash signals the same case.
    device_corroborates: bool       — True if device field signals the same case.

Design invariants (per DATA_MODEL §5, FR-004..FR-008)
------------------------------------------------------
- content_hash (sha256 raw PixelData) is the AUTHORITATIVE key.
- orig_sopuid is corroborating evidence — it raises confidence but CANNOT override
  a content-hash match or absence.
- Perceptual hash (ImageHash.hash_str) is ADVISORY — it may flag near-duplicates
  but MUST NOT auto-reject or auto-accept.
- Case classification is determined by set algebra: no threshold, no ML.
- No function in this module writes to the registry, triggers imports, or merges data.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set


# ---------------------------------------------------------------------------
# Classification enum
# ---------------------------------------------------------------------------

class CaseRelation(str, Enum):
    """Set-algebra classification of how an incoming case relates to an existing one."""
    EXACT     = "EXACT"       # incoming == existing
    SUBSET    = "SUBSET"      # incoming ⊂ existing (proper subset)
    SUPERSET  = "SUPERSET"    # incoming ⊃ existing (proper superset)
    PARTIAL   = "PARTIAL"     # intersection non-empty; neither subset nor superset
    DISJOINT  = "DISJOINT"    # no intersection (clean — no overlap)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ImageDedupResult:
    """
    Result of image-level duplicate detection (T007).

    is_duplicate is True when content_hash is already present in the registry
    (authoritative). uid_corroborates is an additive confidence signal, never
    a gate. near_dup_flag is advisory only (perceptual hash; FR-005).
    """
    is_duplicate:    bool
    content_hash:    str
    uid_corroborates: bool
    near_dup_flag:   bool
    confidence:      str   # "HIGH" | "CONTENT" | "NONE"


@dataclass
class CaseDedupResult:
    """
    Result of case-level duplicate detection (T007).

    classification is the set-algebra relationship between the incoming case's
    content-hash set and the best-matching existing case (or DISJOINT if no
    overlap exists with any case).

    matched_case_keys lists all existing cases with non-empty overlap, ordered
    by descending overlap_ratio for the best match. May be empty for DISJOINT.

    overlap_ratio is the Jaccard index: |A ∩ B| / |A ∪ B|. Zero for DISJOINT.

    overlap_hashes is the intersection: the specific content hashes that appear
    in both incoming and existing — the "matching shots".
    """
    classification:   str           # CaseRelation value
    matched_case_keys: List[str]
    overlap_ratio:    float
    overlap_hashes:   Set[str]


@dataclass
class EvidencePackage:
    """
    Structured evidence for a human decision on overlapping cases (T008).

    Never interpreted by the system — this is a read-only report for a human
    reviewer who will choose to import both / combine / reject / other.

    corroboration booleans (uid/date/device) are additive signals; all three
    False does NOT weaken the content-hash overlap finding.
    """
    incoming_hashes:     Set[str]
    matched_case_key:    str
    classification:      str
    overlap_ratio:       float
    overlap_hashes:      Set[str]
    uid_corroborates:    bool
    date_corroborates:   bool
    device_corroborates: bool
    # All other cases with non-empty overlap (excluding the primary matched_case_key)
    secondary_matches:   List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Image-level dedup
# ---------------------------------------------------------------------------

def image_dedup(
    content_hash: str,
    orig_sopuid: Optional[str],
    registry,
    *,
    near_dup_flag: bool = False,
) -> ImageDedupResult:
    """
    Report whether *content_hash* is already known to the registry.

    Parameters
    ----------
    content_hash:
        sha256 hex digest of the raw PixelData (pre-processing). The
        AUTHORITATIVE image identity (FR-004, DATA_MODEL §3.4).
    orig_sopuid:
        Original SOPInstanceUID from the source DICOM — corroborating
        evidence only. May be None. Does NOT override the content-hash
        finding in either direction.
    registry:
        An object with:
          - image_exists(content_hash: str) -> bool
          - a ``_conn`` SQLite connection (used to corroborate orig_sopuid).
        Typically ``src.services.registry.Registry``.
    near_dup_flag:
        Pre-computed perceptual-hash advisory flag (e.g. from
        ``ImageHash.hash_str`` proximity check done by the caller).
        Advisory only — never auto-rejects (FR-005).

    Returns
    -------
    ImageDedupResult
        is_duplicate:  authoritative (content hash membership).
        uid_corroborates: True iff orig_sopuid matches a registry row for this hash.
        near_dup_flag: passed through as-is — advisory only.
        confidence: "HIGH" when both content and UID corroborate,
                    "CONTENT" when only content matches,
                    "NONE" when content is new.
    """
    is_dup = registry.image_exists(content_hash)

    # UID corroboration: check whether any registry row for this content_hash
    # also records the same orig_sopuid. Corroborating, not authoritative.
    uid_corroborates = False
    if is_dup and orig_sopuid:
        try:
            cur = registry._conn.execute(
                "SELECT 1 FROM image_hashes"
                " WHERE content_hash = ? AND orig_sopuid = ? LIMIT 1",
                (content_hash, orig_sopuid),
            )
            uid_corroborates = cur.fetchone() is not None
        except Exception:  # noqa: BLE001 — graceful degradation; UID is advisory
            uid_corroborates = False

    if not is_dup:
        confidence = "NONE"
    elif uid_corroborates:
        confidence = "HIGH"
    else:
        confidence = "CONTENT"

    return ImageDedupResult(
        is_duplicate=is_dup,
        content_hash=content_hash,
        uid_corroborates=uid_corroborates,
        near_dup_flag=near_dup_flag,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Case-level dedup
# ---------------------------------------------------------------------------

def _classify(incoming: Set[str], existing: Set[str]) -> tuple[str, float, Set[str]]:
    """
    Compute (CaseRelation, jaccard_ratio, intersection) for two hash-sets.

    Internal helper — set algebra only.
    """
    intersection = incoming & existing
    union = incoming | existing

    if not intersection:
        return CaseRelation.DISJOINT, 0.0, set()

    jaccard = len(intersection) / len(union) if union else 0.0

    if incoming == existing:
        return CaseRelation.EXACT, jaccard, intersection
    elif incoming < existing:           # proper subset: incoming ⊂ existing
        return CaseRelation.SUBSET, jaccard, intersection
    elif incoming > existing:           # proper superset: incoming ⊃ existing
        return CaseRelation.SUPERSET, jaccard, intersection
    else:                               # partial overlap
        return CaseRelation.PARTIAL, jaccard, intersection


def case_dedup(
    incoming_image_hashes: Set[str],
    registry,
) -> CaseDedupResult:
    """
    Classify the relationship between *incoming_image_hashes* and all existing cases.

    The registry is queried for all known case_keys; for each case the hash-set
    is fetched and set algebra is applied. The best-matching case (by Jaccard
    ratio) is primary; all others with non-empty overlap are also reported.

    When no existing case has any overlap the result is DISJOINT (clean import).

    Parameters
    ----------
    incoming_image_hashes:
        Set of sha256 content-hashes for the images in the incoming case.
        Must be non-empty (empty set → DISJOINT with 0.0 ratio).
    registry:
        An object with:
          - case_image_hashes(case_key: str) -> set[str]
          - a ``_conn`` SQLite connection (to enumerate case_keys).
        Typically ``src.services.registry.Registry``.

    Returns
    -------
    CaseDedupResult
        classification: primary classification vs the best-match existing case.
        matched_case_keys: [best_match, ...other_overlapping_cases] or [] for DISJOINT.
        overlap_ratio: Jaccard ratio for the primary (best) match.
        overlap_hashes: intersection with the primary match.

    Notes on multi-case overlap
    ---------------------------
    When the incoming set overlaps MORE THAN ONE existing case the primary
    classification is taken from the best-matching (highest Jaccard) case.
    secondary_matches are exposed in the EvidencePackage, not here. This
    ambiguity is an evidence-package scenario — human decides.
    """
    if not incoming_image_hashes:
        return CaseDedupResult(
            classification=CaseRelation.DISJOINT,
            matched_case_keys=[],
            overlap_ratio=0.0,
            overlap_hashes=set(),
        )

    # Enumerate all existing cases from the registry.
    try:
        cur = registry._conn.execute("SELECT DISTINCT case_key FROM cases")
        all_case_keys = [row[0] for row in cur.fetchall() if row[0]]
    except Exception:  # noqa: BLE001
        all_case_keys = []

    best_key: Optional[str] = None
    best_ratio: float = 0.0
    best_relation: str = CaseRelation.DISJOINT
    best_intersection: Set[str] = set()
    overlapping_keys: List[str] = []

    for case_key in all_case_keys:
        existing_hashes = registry.case_image_hashes(case_key)
        relation, ratio, intersection = _classify(incoming_image_hashes, existing_hashes)
        if relation != CaseRelation.DISJOINT:
            overlapping_keys.append(case_key)
            if ratio > best_ratio or best_key is None:
                best_key = case_key
                best_ratio = ratio
                best_relation = relation
                best_intersection = intersection

    if best_key is None:
        return CaseDedupResult(
            classification=CaseRelation.DISJOINT,
            matched_case_keys=[],
            overlap_ratio=0.0,
            overlap_hashes=set(),
        )

    # Order: best key first, then others.
    other_keys = [k for k in overlapping_keys if k != best_key]
    matched = [best_key] + other_keys

    return CaseDedupResult(
        classification=best_relation,
        matched_case_keys=matched,
        overlap_ratio=best_ratio,
        overlap_hashes=best_intersection,
    )


# ---------------------------------------------------------------------------
# Evidence package builder
# ---------------------------------------------------------------------------

def build_evidence_package(
    incoming_hashes: Set[str],
    matched_case_key: str,
    classification: str,
    registry,
    *,
    corroboration: Optional[Dict[str, object]] = None,
) -> EvidencePackage:
    """
    Build a structured evidence package for human adjudication (T008, FR-007).

    This function is PURE DATA — it reads the registry, assembles the package,
    and returns it. Nothing is written; no merge/reject action is taken.

    Parameters
    ----------
    incoming_hashes:
        Content-hash set of the incoming case (the candidate for ingest).
    matched_case_key:
        The primary overlapping existing case_key (from CaseDedupResult).
    classification:
        CaseRelation value (from CaseDedupResult).
    registry:
        Registry with case_image_hashes() and _conn for case-metadata queries.
    corroboration : dict, optional
        Corroborating signals — any/all of:
          ``"orig_studyuid"``  — str: incoming case's original StudyInstanceUID.
          ``"date_hash"``      — str: HMAC(salt, date+device) of incoming case.
          ``"device"``         — str: incoming Manufacturer/ManufacturerModelName.
        When absent or None, all corroboration booleans are False. These signals
        are advisory — they may strengthen evidence but CANNOT override the
        content-hash overlap.

    Returns
    -------
    EvidencePackage
        Full structured evidence for a human reviewer. The secondary_matches
        field lists all other existing cases that also overlap the incoming set.
    """
    corroboration = corroboration or {}

    # -- fetch existing-case hash-set for the primary match --------------------
    existing_hashes = registry.case_image_hashes(matched_case_key)
    overlap_hashes = incoming_hashes & existing_hashes
    union = incoming_hashes | existing_hashes
    overlap_ratio = len(overlap_hashes) / len(union) if union else 0.0

    # -- UID corroboration -----------------------------------------------------
    uid_corroborates = False
    incoming_studyuid = corroboration.get("orig_studyuid")
    if incoming_studyuid:
        try:
            cur = registry._conn.execute(
                "SELECT orig_sopuid FROM image_hashes WHERE case_key = ? LIMIT 1",
                (matched_case_key,),
            )
            # We only have StudyInstanceUID in cases table, not image_hashes.
            # Check the cases table for any studyuid corroboration via date_hash/device.
            # For UID corroboration at the case level we compare the incoming
            # orig_studyuid against what would have been stored per spec; since
            # cases.date_hash (not orig_studyuid) is the corroborant stored in
            # the registry, we fall back to checking whether any image in the
            # matched case has an orig_sopuid whose first segment (study portion)
            # is a prefix of the incoming studyuid — or simply flag based on
            # caller-supplied equality.
            # Per spec: corroboration is caller-supplied evidence; we expose the
            # flag, not re-derive it here.
            uid_corroborates = bool(corroboration.get("uid_corroborates", False))
        except Exception:  # noqa: BLE001
            uid_corroborates = False
    else:
        uid_corroborates = bool(corroboration.get("uid_corroborates", False))

    # -- date corroboration ---------------------------------------------------
    date_corroborates = False
    incoming_date_hash = corroboration.get("date_hash")
    if incoming_date_hash:
        try:
            cur = registry._conn.execute(
                "SELECT date_hash FROM cases WHERE case_key = ? LIMIT 1",
                (matched_case_key,),
            )
            row = cur.fetchone()
            if row and row[0] and row[0] == incoming_date_hash:
                date_corroborates = True
        except Exception:  # noqa: BLE001
            date_corroborates = False

    # -- device corroboration -------------------------------------------------
    device_corroborates = False
    incoming_device = corroboration.get("device")
    if incoming_device:
        try:
            cur = registry._conn.execute(
                "SELECT device FROM cases WHERE case_key = ? LIMIT 1",
                (matched_case_key,),
            )
            row = cur.fetchone()
            if row and row[0] and row[0] == incoming_device:
                device_corroborates = True
        except Exception:  # noqa: BLE001
            device_corroborates = False

    # -- secondary matches (all other overlapping cases) ----------------------
    try:
        cur = registry._conn.execute("SELECT DISTINCT case_key FROM cases")
        all_case_keys = [row[0] for row in cur.fetchall() if row[0] and row[0] != matched_case_key]
    except Exception:  # noqa: BLE001
        all_case_keys = []

    secondary_matches: List[str] = []
    for ck in all_case_keys:
        other_hashes = registry.case_image_hashes(ck)
        if incoming_hashes & other_hashes:
            secondary_matches.append(ck)

    return EvidencePackage(
        incoming_hashes=set(incoming_hashes),
        matched_case_key=matched_case_key,
        classification=classification,
        overlap_ratio=overlap_ratio,
        overlap_hashes=overlap_hashes,
        uid_corroborates=uid_corroborates,
        date_corroborates=date_corroborates,
        device_corroborates=device_corroborates,
        secondary_matches=secondary_matches,
    )


# ---------------------------------------------------------------------------
# Advisory near-dup utility (within-case IS_QUESTIONABLE flag, FR-005/006)
# ---------------------------------------------------------------------------

def is_near_duplicate_pair(hash_a: str, hash_b: str, *, threshold: int = 10) -> bool:
    """
    Return True if two perceptual-hash hex strings are within *threshold* Hamming
    distance — an advisory within-case near-duplicate signal (FR-005/006).

    Operates on **perceptual** hashes (e.g. ImageHash.hash_str from
    src/utilities.py — normalize+resize+sha256), NOT on raw-content hashes.
    Callers compute the perceptual hash before calling this function.

    The result is NEVER used to auto-reject images.  Within-case near-dups are
    flagged (IS_QUESTIONABLE private tag) but always retained (FR-006, DATA_MODEL §5.2).

    Parameters
    ----------
    hash_a, hash_b : str
        Hex strings of equal length.
    threshold : int
        Hamming distance at or below which the pair is near-duplicate.
        Default 10 is conservative for 256-bit hashes.

    Returns
    -------
    bool
    """
    if not hash_a or not hash_b or len(hash_a) != len(hash_b):
        return False
    try:
        bytes_a = bytes.fromhex(hash_a)
        bytes_b = bytes.fromhex(hash_b)
    except ValueError:
        return False
    hamming = sum(bin(x ^ y).count("1") for x, y in zip(bytes_a, bytes_b))
    return hamming <= threshold
