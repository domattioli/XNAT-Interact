import subprocess
import sys
import os
from typing import Optional


def is_repo_up_to_date() -> bool:
    """
    Check if the local Git repository is up to date with the remote repository.

    Returns:
        bool: True if the local repository is up to date, False otherwise.
    """
    try:
        # Check if the local repository is up to date with the remote
        result = subprocess.run(['git', 'fetch'], check=True, capture_output=True, text=True)
        status = subprocess.run(['git', 'status'], capture_output=True, text=True)
        if 'Your branch is up to date' in status.stdout:
            return True
        else:
            return False
    except subprocess.CalledProcessError as e:
        print(f'\tFAILURE\t--\tFailed to check repository status.')
        print(e)
        return False


def update_repo() -> str:
    """
    Update the local Git repository if it is not up to date.

    Returns:
        str: A message indicating the success or failure of the repository update.
    """
    if not is_repo_up_to_date():
        try:
            # Stash any local changes
            print('\n\t...stashing local changes...\n')
            subprocess.run(['git', 'stash'], check=True)
            
            # Pull all changes from the remote repository, including deletions
            print('\nt\t...updating repository...\n')
            subprocess.run(['git', 'pull', '--prune'], check=True)
            
            # Apply the stashed changes
            print('\n\t...applying stashed changes...\n')
            subprocess.run(['git', 'stash', 'pop'], check=True)
            
            return f'\tSUCCESS\t--\tRepository updated successfully!'
        except subprocess.CalledProcessError as e:
            return f'\tFAILURE\t--\tFailed to update the repository.'
    else:   return f'\tINFO\t--\tRepository is already up to date.'



def check_that_virtualenv_activated() -> bool:
    """
    Check if a virtual environment is activated.

    Returns:
        bool: True if a virtual environment is activated, False otherwise.
    """
    if 'VIRTUAL_ENV' in os.environ: return True
    else:                           return False
    

def test_virtual_env() -> bool:
    """
    Test if all required Python libraries are available in the virtual environment.

    Returns:
        bool: True if all required libraries are available.
    """
    # List all libraries used in the project
    import typing
    import json
    import os
    import glob
    import re
    import cv2
    import numpy as np
    import pandas as pd
    import datetime
    import dateutil
    import pytz
    import typing
    import pydicom
    import pathlib
    import pyxnat 
    import io
    import base64
    import requests
    import hashlib
    import shutil
    import tempfile
    import pwinput
    import sys
    import tabulate
    from matplotlib import pyplot as plt

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
    from src.utilities import ConfigTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
    from src.xnat_experiment_data import SourceRFSession, SourceESVSession
    from src.xnat_scan_data import SourceDicomDeIdentified, MTurkSemanticSegmentation, ArthroDiagnosticImage, ArthroVideo
    from src.xnat_resource_data import ORDataIntakeForm
    return True

def main() -> None:
    """
    Main function to ensure XNAT-Interact installation is correct, up to date, and all requirements are satisfied.
    """
    print(f'\n\t...ensuring that XNAT-Interact installation is correct and up-to-date...\n')
    try:
        check_that_virtualenv_activated()
    except Exception as e:
        print(f'\tERROR\t-- You must activate your virtual environment before running this script.\n')
        sys.exit(1)
    try: # Call update_repo function if needed
        update_message = update_repo()
        print(update_message)
        test_virtual_env()
        print(f'\tSUCCESS\t-- XNAT-Interact test and update was successful!')
    except Exception as e:
        print(f'\tFAILURE\t-- XNAT-Interact test and update failed; double check install requirements are all installed to your venv.\n')
        print(e)
        sys.exit(1)

    print( f'\n\t...You may now proceed to "main.py" script...\n' )
    sys.exit(0)
    

if __name__ == '__main__':
    main()