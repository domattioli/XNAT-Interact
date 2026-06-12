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