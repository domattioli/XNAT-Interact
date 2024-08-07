import subprocess
import sys
import os
import typing


def is_repo_up_to_date():
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

def update_repo():
    if not is_repo_up_to_date():
        try:
            # Pull all changes from the remote repository, including deletions
            subprocess.run(['git', 'pull', '--prune'], check=True)
            return f'\tSUCCESS\t--\tRepository updated successfully!'
        except subprocess.CalledProcessError as e:
            return f'\tFAILURE\t--\tFailed to update the repository.'
            print(e)
            sys.exit(1)
    else:
        return f'\tINFO\t--\tRepository is already up to date.'



def check_that_virtualenv_activated():
    if 'VIRTUAL_ENV' in os.environ: return True
    else:                           return False
    

def test_virtual_env():
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

    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))
    from utilities import MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
    from xnat_experiment_data import SourceRFSession, SourceESVSession
    from xnat_scan_data import SourceDicomDeIdentified, MTurkSemanticSegmentation, ArthroDiagnosticImage, ArthroVideo
    from xnat_resource_data import ORDataIntakeForm
    return True

def main():
    print(f'\n\t...ensuring that XNAT-INTERACT installation is correct and up-to-date...\n')
    try:
        check_that_virtualenv_activated()
    except Exception as e:
        print(f'\tERROR\t-- You must activate your virtual environment before running this script.\n')
        sys.exit(1)
    try:
        test_virtual_env()
        print(f'\tSUCCESS\t-- XNAT-INTERACT installation was successful!')
    except Exception as e:
        print(f'\tFAILURE\t-- Installation failed; double check install requirements are all installed to your venv.\n')
        print(e)
        sys.exit(1)

    # Call update_repo function if needed
    out_str = update_repo()
    print(out_str)
    print( f'\n\t...testing complete -- you may now proceed to "main.py" script...\n' )
    sys.exit(0)
    

if __name__ == '__main__':
    main()