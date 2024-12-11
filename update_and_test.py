import subprocess
import pkg_resources
import importlib
import sys
import os
from typing import Optional, Tuple
from pathlib import Path

def pull_from_master() -> None: # Assume that update_and_test.py is in the root directory of the repository
    # Fetch the latest changes from the remote repository # Reset the local repository to match the remote master branch
    subprocess.run( ['git', 'fetch', 'origin'], check=True )
    subprocess.run( ['git', 'reset', '--hard', 'origin/master'], check=True )

def check_that_virtualenv_activated() -> bool:
    return 'VIRTUAL_ENV' in os.environ

def check_and_install_requirements( requirements_file: Path ) -> None:
    with open(requirements_file, 'r') as file:
        requirements = file.readlines()

    for requirement in requirements:
        requirement = requirement.strip()
        if requirement:
            try:
                pkg_resources.require(requirement)
                print(f"\t\t'{requirement}' is installed.")
            except pkg_resources.DistributionNotFound:
                print(f"\t\t'{requirement}' is NOT installed. Installing...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", requirement])
            except pkg_resources.VersionConflict as e:
                print(f"\t\t'{requirement}' has a version conflict: {e}.\n\t\t-- Installing correct version...")
                subprocess.check_call([sys.executable, "-m", "pip", "install", requirement])

def import_all_necessary_modules( requirements_file: Path ) -> None:
    # Check and install requirements
    print( f"\t...Checking and installing requirements...")
    check_and_install_requirements(requirements_file)
    
    # Import necessary modules
    with open(requirements_file, 'r') as file:
        requirements = file.readlines()

    for requirement in requirements:
        requirement = requirement.strip()
        if requirement:
            # Extract the library name (ignoring version specifications)
            library_name = requirement.split('==')[0].split('>=')[0].split('<=')[0].split('>')[0].split('<')[0]
            try:
                importlib.import_module(library_name)
                print(f"\tSuccessfully imported {library_name}")
            except ImportError as e:
                print(f"\tError importing {library_name}: {e}")

def main() -> None:
    """
    Main function to ensure XNAT-Interact installation is correct, up to date, and all requirements are satisfied.
    """
    print(f'\n...ensuring that XNAT-Interact installation is correct and up-to-date...\n')
    try:
        success = check_that_virtualenv_activated()
        if not success:
            raise Exception( 'Virtual environment not activated...' )
        print( f'--- Virtual environment is active...' )
    except Exception as e:
        print( f'ERROR\t-- You must activate your virtual environment before running this script.\n' )
        sys.exit( 1 )

    this_directory = Path(__file__).parent
    requirements_file = 'requirements.txt'
    ffn = this_directory / requirements_file
    try:
        import_all_necessary_modules( requirements_file=ffn )
        print( f'--- All necessary modules are available...' )
        print( f'--- Tests complete...\n\t...Updating repository...\n' )
    except Exception as e:
        print( f'ERROR\t-- You must install all necessary modules before running this script.\n\tError printout:\n{e}' )
        sys.exit( 1 )

    # Before updating local files, we must ask the user to confirm that all files that might be overwritten are closed on their local machine.
    print( f'--- Pulling latest changes from the master branch...\n' )
    print( f'\tWARNING: Before proceeding, ensure that all files that might be overwritten are closed on your local machine.\n' )
    print( f'\tHave you closed all files that might be overwritten? Input 1 for yes and 2 for no)\n' )
    batch_upload_file = input( f'\tAnswer:\t' )
    if batch_upload_file != '1':
        print( f'\nERROR\t-- Please close all files that might be overwritten before proceeding...\n' )
        sys.exit( 1 )
    try:
        pull_from_master()
        print( f'SUCCESS\t-- Codebase is up-to-date with the master branch...\n' )
    except Exception as e:
        print( f'FAILURE\t-- Could not automatically download latest master branch...\n\tContact the data librarian for assistance. Error printout:\n{e}' )
        sys.exit( 1 )

    print( f'\n...You may now proceed to "main.py" script...\n' )
    sys.exit(0)
    

if __name__ == '__main__':
    main()