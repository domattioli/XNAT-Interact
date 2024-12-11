import subprocess
import sys
import os
from typing import Optional, Tuple


def pull_from_master() -> None: # Assume that update_and_test.py is in the root directory of the repository
    # Fetch the latest changes from the remote repository # Reset the local repository to match the remote master branch
    subprocess.run( ['git', 'fetch', 'origin'], check=True )
    subprocess.run( ['git', 'reset', '--hard', 'origin/master'], check=True )

def check_that_virtualenv_activated() -> bool:
    return 'VIRTUAL_ENV' in os.environ

def import_all_necessary_modules() -> None:
    """
    Test if all required Python libraries are available in the virtual environment.
    """
# List all libraries used in the project
    import json
    import os
    import glob
    import re
    import ast
    from typing import Optional as Opt, Tuple, Union, List, Any, Hashable
    import cv2
    import io
    import base64
    import numpy as np
    import requests
    import hashlib
    import pandas as pd
    import tabulate
    import datetime
    from dateutil import parser
    import pytz
    import uuid
    from pydicom.dataset import FileDataset as pydicomFileDataset, FileMetaDataset as pydicomFileMetaDataset
    from pydicom import Dataset as pydicomDataset, Sequence, dcmread, dcmwrite
    from pydicom.dataelem import DataElement
    from pydicom.datadict import dictionary_VR, dictionary_has_tag
    from pydicom.uid import UID as pydicomUID, generate_uid as generate_pydicomUID
    from pyxnat import Interface, schema
    from pathlib import Path, PurePosixPath
    import shutil
    import tempfile
    import sys
    import warnings
    import openpyxl

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
    
    # from src.utilities import ConfigTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash, UIDandMetaInfo, MassUploadRepresentation
    # from src.xnat_experiment_data import SourceRFSession, SourceESVSession
    # from src.xnat_scan_data import SourceDicomDeIdentified, MTurkSemanticSegmentation, ArthroDiagnosticImage, ArthroVideo
    # from src.xnat_resource_data import ORDataIntakeForm

def main() -> None:
    """
    Main function to ensure XNAT-Interact installation is correct, up to date, and all requirements are satisfied.
    """
    print(f'\n\t...ensuring that XNAT-Interact installation is correct and up-to-date...\n')
    try:
        success = check_that_virtualenv_activated()
        print( f'\t--- Virtual environment is active...' )
    except Exception as e:
        print( f'\tERROR\t-- You must activate your virtual environment before running this script.\n' )
        sys.exit( 1 )

    try:
        import_all_necessary_modules()
        print( f'\t--- All necessary modules are available...' )
        print( f'\t--- Tests complete...\n\t...Updating repository...\n' )
    except Exception as e:
        print( f'\tERROR\t-- You must install all necessary modules before running this script.\n\tError printout:\n{e}' )
        sys.exit( 1 )

    try:
        pull_from_master()
        print( f'\tSUCCESS\t-- Codebase is up-to-date with the master branch...\n' )
    except Exception as e:
        print( f'\tFAILURE\t-- Could not automatically download latest master branch...\n\tContact the data librarian for assistance. Error printout:\n{e}' )
        sys.exit( 1 )

    print( f'\n\t...You may now proceed to "main.py" script...\n' )
    sys.exit(0)
    

if __name__ == '__main__':
    main()