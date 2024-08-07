import os
import sys

def test_virtualenv_activated():
    if 'VIRTUAL_ENV' in os.environ:
        print("Virtual environment is activated.")
        return True
    else:
        print("Virtual environment is not activated.")
        return False
    
def main():
    try:
        test_virtualenv_activated()
    except Exception as e:
        print( f'Error: You must activate your virtual environment before running this script.\n')
        return

    try: # Check contents of venv
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
        
        # Insert the src directory into the system path
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

        from utilities import MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
        from xnat_experiment_data import SourceRFSession, SourceESVSession
        from xnat_scan_data import SourceDicomDeIdentified, MTurkSemanticSegmentation, ArthroDiagnosticImage, ArthroVideo
        from xnat_resource_data import ORDataIntakeForm

        print( f'\tXNAT-INTERACT installation was successful!' )

    except Exception as e:
        print( f'Installion failed; double check install requirements are all installed to your venv.\n')
        print( e)

if __name__ == '__main__':
    main()
