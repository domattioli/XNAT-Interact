

def main():
    try:
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
        print( f'hello world' )
    except Exception as e:
        print( f'Installion failed; double check install requirements are all installed to your venv.\n')
        print( e)

if __name__ == '__main__':
    main()
