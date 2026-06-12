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