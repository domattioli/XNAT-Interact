from typing import Optional as Opt, Tuple, Union, List
import sys
import os
from pathlib import Path
import pwinput
import argparse
import pandas as pd
from tabulate import tabulate
import datetime
import xml.etree.ElementTree as ET
import requests
from requests.auth import HTTPBasicAuth
from pyxnat import Interface
from pyxnat.core.jsonutil import JsonTable

from src.utilities import ConfigTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
from src.xnat_experiment_data import *
from src.xnat_scan_data import *
from src.xnat_resource_data import ORDataIntakeForm
from src.batch_upload import BatchUploadRepresentation
from src.services.config import AppConfig as _AppConfig
from src.services.errors import FriendlyError

_app_config = _AppConfig.load()

def parse_args() -> Tuple[str, bool]:
    """
    Parse command-line arguments for XNAT login and connection.

    Returns:
        Tuple[str, bool]: A tuple containing the XNAT username and a flag indicating verbose mode.

    NOTE: --password has been intentionally removed.  Credentials must not be
    passed via argv (visible in process listings and shell history).  The
    password is collected via the secure interactive prompt in prompt_login().
    """
    parser = argparse.ArgumentParser(description='XNAT Login and Connection')
    parser.add_argument('--username', type=str, default=None, help='XNAT username')
    parser.add_argument('--verbose', action='store_true', default=True, help='Enable verbose output')
    args = parser.parse_args()
    if args.verbose:    print( "~~Verbosity turned on~~\n" )
    else:               print( "~~Verbosity turned off~~\n" )
    return args.username, args.verbose


def prompt_function( verbose: bool ) -> str:
    """
    Prompt the user to select a task to perform.

    Args:
        verbose (bool): Whether to enable verbose output.

    Returns:
        str: The selected task ('1' for Uploading Source Data, '2' for Uploading Derived Data, or '3' for Downloading Data).
    """
    if verbose: print( '\n...selecting task to perform...' )
    print( '\tTask selection:\n\t\t-- Please enter "1" for Uploading Source Data, "2" for Uploading Derived Data, or "3" for Downloading Data.' )
    return ORDataIntakeForm.prompt_until_valid_answer_given( 'Select a task', acceptable_options=['1', '2', '3'] )


def prompt_individual_or_batch_upload( verbose: bool ) -> str:
    """
    Prompt the user to select an individual or batch upload.

    Args:
        verbose (bool): Whether to enable verbose output.

    Returns:
        str: The selected upload type ('1' for Individual Upload or '2' for Batch Upload).
    """
    if verbose: print( '\n...selecting upload type...' )
    print( '\tUpload type selection:\n\t\t-- Please enter "1" for Individual Upload or "2" for Batch Upload.' )
    return ORDataIntakeForm.prompt_until_valid_answer_given( 'Select an upload type', acceptable_options=['1', '2'] )


def prompt_login( username: Opt[str]=None, password: Opt[str]=None ) -> Tuple[str, str]:
    """
    Prompt the user for their XNAT login credentials if not provided.

    Args:
        username (Optional[str]): XNAT username.
        password (Optional[str]): XNAT password.

    Returns:
        Tuple[str, str]: A tuple containing the username and password.
    """
    if username is None:
        username = input( "\tHawkID Username:\t" )
    if password is None:
        password = pwinput.pwinput( prompt=f"\t{username.upper()} Password:\t", mask="*" )
    return username, password


def prompt_source_and_group() -> Tuple[str, str]:
    """
    Prompt the user to provide the acquisition site and surgical procedure.

    Returns:
        Tuple[str, str]: Acquisition site and surgical procedure name.
    """
    return input( "Acquisition Site: " ), input( "Surgical Procedure: " )
    

def try_login_and_connection( username: Opt[str]=None, verbose: Opt[bool]=True ) -> Tuple[XNATLogin, XNATConnection, ConfigTables]:
    """
    Attempt to login and connect to the XNAT server.

    Args:
        username (Optional[str]): XNAT username (may be pre-supplied via --username).
        verbose (Optional[bool]): Whether to enable verbose output.

    Returns:
        Tuple[XNATLogin, XNATConnection, ConfigTables]: Validated login, XNAT connection, and config.

    Raises:
        ValueError: If the login credentials do not lead to a successful connection.

    NOTE: Password is always collected via the secure interactive prompt.
    It is never read from argv, env vars, or config files.
    """
    if verbose: print( '\n\tPlease enter your XNAT login credentials to connect to the server:' )
    username, password = prompt_login( username=username, password=None )
    if verbose: print( '\n...logging in and trying to connect to the server...\n' )
    validated_login = XNATLogin( { 'Username': username, 'Password': password, 'Url': _app_config.server_url }, verbose=verbose )
    xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbose )

    if xnat_connection.is_verified and xnat_connection.is_open:
        config = ConfigTables( validated_login, xnat_connection, verbose=False ) # don't want to see all this info every time because there is so much and it is really only intended for the librarian to debug with
        if verbose: print( '\n-- A connection was successfully established!\n' )
    else:
        raise ValueError( "\tThe provided login credentials did not lead to a successful connection. Please try again, or contact the Data Librarian for help." )
    return validated_login, xnat_connection, config


def upload_new_case( validated_login: XNATLogin, xnat_connection: XNATConnection, config: ConfigTables, verbose: Opt[bool]=False ) -> ConfigTables:
    """
    Begin the data intake process to upload new source data to XNAT.

    Args:
        validated_login (XNATLogin): Validated login object.
        xnat_connection (XNATConnection): Established XNAT connection.
        config (ConfigTables): Psuedo database tables for cross-referencing and managing server data
        verbose (Optional[bool]): Whether to enable verbose output.

    Returns:
        ConfigTables: Updated ConfigTables after data upload.

    Raises:
        ValueError: If the procedure type is not supported or if the intake form creation fails.
    """
    print( '\n-----Beginning data intake process-----\n' )
    if verbose: print( "\n...Uploading new source data to XNAT...")

    # Digitize/load an intake form (if it exists already).
    print( '\tHave you already created a *DIGITAL* intake form with this software for this procedure?\n\t\t-- Please enter "1" for Yes or "2" for No.' )
    form_exists = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
    
    if form_exists == '1':
        form_pn = input( "\n\tPlease enter the full path to the *parent folder* of the intake form:\t" )
        while not os.path.exists( form_pn ):
            print( "\t\tThe provided path either (1) does not exist or (2) does not contain a 'RECONSTRUCTED_OR_DATA_INTAKE_FORM' in it. Please try again." )
            form_pn = input( "\n\tPlease enter the full path to the *parent folder* of the intake form:\t" )
        try:
            intake_form = ORDataIntakeForm( config=config, validated_login=validated_login, input_data=Path( form_pn ), verbose=verbose )
            while True:
                print( f'\n\tPlease review the created intake form:\n{intake_form}' )
                print( '\n\t--- Is everything correct?\n\t\t-- Please enter "1" for Yes or "2" for No (re-create the intake form).' )
                accept_form = ORDataIntakeForm.prompt_until_valid_answer_given( 'Accept Intake Form As-Is', acceptable_options=['1', '2'] )
                if accept_form == '2':
                    print( '\n\tRe-doing the form...' )
                    intake_form = ORDataIntakeForm( config=config, validated_login=validated_login, verbose=verbose ) #to-do: causes an error and the above try block fails.
                else:            break
        except KeyboardInterrupt:
            print( '\n\n...User cancelled task via Ctrl+C...' )
            sys.exit( 0 )
        except:                 raise ValueError( "\t\tThe provided path did not lead to a successful intake form. Please try again, or contact the Data Librarian for help." )
    else: # Prompt user to create a new intake form; then print it to confirm
        while True:
            intake_form = ORDataIntakeForm( config=config, validated_login=validated_login, verbose=verbose )
            print( f'\n\tPlease review the created intake form:\n{intake_form}' )
            print( '\n\tIs everything correct?\n\t\t-- Please enter "1" for Yes or "2" for No (re-create the intake form).' )
            accept_form = ORDataIntakeForm.prompt_until_valid_answer_given( 'Accept Intake Form As-Is', acceptable_options=['1', '2'] )
            if accept_form == '2':
                print( '\n\tRe-doing the form...' )
                continue
            else:               break
    if verbose:                 print( '\n\tProceeding with the intake form as is...\n' )
    
    # Depending on the procedure type, create the appropriate source data object.
    if intake_form.ortho_procedure_type.upper() == 'ARTHROSCOPY':
        source_data = SourceESVSession( intake_form=intake_form, config=config )
    elif intake_form.ortho_procedure_type.upper() == 'TRAUMA':
        # raise ValueError( f"\tThe provided procedure type is not yet supported. Please contact the Data Librarian for help." )
        source_data = SourceRFSession( intake_form=intake_form, config=config )
    else:                       raise ValueError( "\tThe provided procedure type is not yet supported. This is a bug that should be reported to Data Librarian." )
    
    # Publish data to xnat
    config = source_data.write_publish_catalog_subroutine( config=config, xnat_connection=xnat_connection, validated_login=validated_login, verbose=verbose, delete_zip=False )
    if verbose: print( '\n-----Concluding Upload Process-----\n' )
    return config


# ---- Functions for querying and downloading data ----
def format_as_table( json_table: JsonTable ) -> pd.DataFrame:
    """
    Convert a JSON table to a Pandas DataFrame.

    Args:
        json_table (JsonTable): JSON data to be converted.

    Returns:
        pd.DataFrame: Converted Pandas DataFrame.
    """
    return pd.DataFrame( [item for item in json_table] )


def print_preview_of_xnat_data( pd_table: pd.DataFrame) -> None:
    """
    Print a preview of the XNAT data.

    Args:
        pd_table (pd.DataFrame): DataFrame containing the XNAT data to preview.
    """
    print(f'\n{"---"*15} Preview of XNAT Data {"---"*15}')
    print( f'\tTotal # Performances: {len(pd_table)}' )

    # Columns to display: procedure operation_date upload_date upload_time
    pd_copy = pd_table.copy()
    pd_copy.index = pd_copy.index + 1
    pd_copy['upload_time'] = pd_copy['upload_time'].apply(lambda x: x.strftime('%H:%M'))
    pd_copy.reset_index(inplace=True)
    pd_copy.rename(columns={'index': 'Row'}, inplace=True)
    columns = ['Row', 'procedure', 'operation_date', 'upload_date', 'upload_time']
    table = pd_copy[columns].values.tolist()
    headers = columns

    # Generate the table with tabulate
    table_str = tabulate(table, headers, tablefmt='plain', stralign='left', numalign='left')

    # Add a tab to each line of the table
    table_str_with_tab = '\n'.join(['\t' + line for line in table_str.split('\n')])

    # Print the table with tabs
    print(table_str_with_tab)
    print( f'{"---"*50}\n' )


def query_experiments_rest(validated_login: XNATLogin, project_name: str) -> pd.DataFrame:
    """
    Query experiments from XNAT via REST API instead of pyxnat search.

    Args:
        validated_login (XNATLogin): Validated login object containing credentials.
        project_name (str): Name of the XNAT project to query.

    Returns:
        pd.DataFrame: DataFrame with columns: expt_id, experiment, xsiType, subject_id,
                     subject_label, operation_date, insert_date.

    Raises:
        ValueError: If the HTTP request fails or returns an error. The message
                    carries the FriendlyError title + message (FriendlyError is
                    a plain dataclass and cannot be raised directly).
    """
    # Build the REST API endpoint URL
    server_url = _app_config.server_url.rstrip('/')
    endpoint = (
        f"{server_url}/data/experiments?"
        f"project={project_name}&"
        f"columns=ID,label,xsiType,subject_ID,subject_label,date,insert_date&"
        f"format=json"
    )

    # Create basic auth from login credentials
    auth = HTTPBasicAuth(validated_login.validated_username, validated_login.validated_password)

    try:
        response = requests.get(endpoint, auth=auth, verify=True, timeout=30)
        response.raise_for_status()  # Raise exception for HTTP errors
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code if e.response is not None else 'unknown'
        fe = FriendlyError(
            title="REST API query failed",
            message=f"Failed to query experiments from XNAT for project '{project_name}'. HTTP error: {status_code}",
            recourse=[
                "Verify that the project name is correct.",
                "Check that your XNAT credentials are valid.",
                "Ensure you have permission to access this project.",
                "Contact the Data Librarian if the problem persists.",
            ],
        )
        raise ValueError(f"{fe.title}: {fe.message}")
    except requests.exceptions.RequestException as e:
        fe = FriendlyError(
            title="Network error during REST API query",
            message=f"Failed to connect to XNAT server at {server_url}: {str(e)}",
            recourse=[
                "Verify that you are on the UIowa network (VPN or in-person).",
                "Check that the server URL is correct.",
                "Try again after a moment.",
                "Contact the Data Librarian if the problem persists.",
            ],
        )
        raise ValueError(f"{fe.title}: {fe.message}")

    # Parse the JSON response
    try:
        data = response.json()
    except ValueError as e:
        fe = FriendlyError(
            title="Invalid JSON response from XNAT",
            message="The XNAT server response could not be parsed as JSON.",
            recourse=[
                "Try the query again.",
                "Contact the Data Librarian if the problem persists.",
            ],
        )
        raise ValueError(f"{fe.title}: {fe.message}")

    # Extract the result set (XNAT wraps results in a 'ResultSet' object)
    if isinstance(data, dict) and 'ResultSet' in data:
        result = data['ResultSet'].get('Result', [])
    elif isinstance(data, list):
        result = data
    else:
        result = []

    # If no results, return empty DataFrame with correct columns
    if not result:
        return pd.DataFrame(columns=['expt_id', 'experiment', 'xsiType', 'subject_id', 'subject_label', 'operation_date', 'insert_date'])

    # Convert to DataFrame
    df = pd.DataFrame(result)

    # Rename columns to match expected output names
    rename_map = {
        'ID': 'expt_id',
        'label': 'experiment',
        'xsiType': 'xsiType',
        'subject_ID': 'subject_id',
        'subject_label': 'subject_label',
        'date': 'operation_date',
        'insert_date': 'insert_date'
    }

    # Only rename columns that exist in the returned data
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)

    # Ensure all required columns exist (may be empty if not returned)
    required_cols = ['expt_id', 'experiment', 'xsiType', 'subject_id', 'subject_label', 'operation_date', 'insert_date']
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Select and reorder columns to match expected output
    df = df[required_cols]

    return df


def download_queried_data( validated_login: XNATLogin, xnat_connection: XNATConnection, config: ConfigTables, verbose: Opt[bool]=False ) -> None:
    # for the given project, prompt the user which subjects they want to query, then of those subjects, which of their data they want to download, then download it.
    print( '\n-----Beginning data download process-----\n' )

    # Ask user for a download location.
    print( '\tPlease enter the full path to the folder where you would like to download the data:\t' )
    download_folder = input( '\tAnswer:\t' )
    while not os.path.exists( download_folder ):
        print( '\t--- The provided path does not exist. Please try again.' )
        download_folder = input( '\tAnswer:\t' )
    # download_folder = Path( os.path.expanduser( "~" ) ) / "Downloads"
    download_folder = Path( download_folder )
    download_folder = download_folder / 'XNAT_Download'
    if not os.path.exists( download_folder ):    os.makedirs( download_folder )
    
    # Open the connection then begin a query/download
    with Interface( server=validated_login.xnat_project_url, user=validated_login.validated_username, password=validated_login.validated_password ) as xnat:
        # Ask user if they want to preview all data currently in the database and then specify a query.
        print( '\n\tWould you like to preview all data currently in the database, or perform a specific query?\t--\tPlease enter "1" for Yes or "2" for No.' )
        preview_data = ORDataIntakeForm.prompt_until_valid_answer_given( 'Preview Data?', acceptable_options=['1', '2'] )
        if preview_data == '1':
            # Query experiments using REST API instead of pyxnat search
            all_data_pd = query_experiments_rest(validated_login, _app_config.project_name)

            all_data_pd = all_data_pd.reset_index(drop=True)
            all_data_pd['procedure'] = all_data_pd['xsiType']
            all_data_pd['subject_id'] = all_data_pd['subject_label']
            all_data_pd['insert_date'] = pd.to_datetime(all_data_pd['insert_date']) # Split insert_date into two columns
            all_data_pd['upload_date'] = all_data_pd['insert_date'].dt.date
            all_data_pd['upload_time'] = all_data_pd['insert_date'].dt.time
            all_data_pd.drop( columns=['insert_date'], inplace=True )
            print_preview_of_xnat_data( all_data_pd )

            # Now we can ask user to specify their query.
            print( '\n\tWould you like to filter the data by Operation Date?\t--\tPlease enter "1" for Yes or "2" for No.' )
            filter_by_date = ORDataIntakeForm.prompt_until_valid_answer_given( 'Filter by Operation Date?', acceptable_options=['1', '2'] )
            if filter_by_date == '1':                                                       # Filter by date
                print( '\tPlease enter the start- and end-date for the operation date (YYYY-MM-DD):\t' )
                attempts = 0
                while attempts < 3:
                    start_date = input( '\tStart Date:\t' )
                    try:
                        # Validate the date input
                        datetime.datetime.strptime(start_date, '%Y-%m-%d')
                        break
                    except ValueError:
                        print("Invalid date format. Please enter the start-date in the format YYYY-MM-DD.")
                        attempts += 1
                if attempts == 3:
                    print("Exceeded maximum number of attempts!")
                    sys.exit(1)
                while attempts < 3:
                    end_date = input( '\tEnd Date:\t' )
                    try:
                        # Validate the date input
                        datetime.datetime.strptime(end_date, '%Y-%m-%d')
                        break
                    except ValueError:
                        print("Invalid date format. Please enter the end-date in the format YYYY-MM-DD.")
                        attempts += 1
                if attempts == 3:
                    print("Exceeded maximum number of attempts!")
                    sys.exit(1)
                
                # Filter the data by the operation date
                all_data_pd['operation_date'] = pd.to_datetime( all_data_pd['operation_date'] ).dt.date
                all_data_pd = all_data_pd[(all_data_pd['operation_date'] >= start_date) & (all_data_pd['operation_date'] <= end_date)]
                print_preview_of_xnat_data( all_data_pd )

            # print( f'Would you like to filter the data by Procedure?\t--\tPlease enter "1" for Yes or "2" for No.' )
            # filter_by_procedure = ORDataIntakeForm.prompt_until_valid_answer_given( 'Filter by Procedure?', acceptable_options=['1', '2'] )
            # if filter_by_procedure == '1':                                                      # Filter by procedure
            #     indent_str = f'\n\t\t-- '
            #     print( f'\n\tWhich Ortho Procedure Type are do you want?{indent_str}Please enter "1" for Arthro, "2" for Trauma, "3" for Arthro & Trauma, "4" for Derived, or "5" for All' )
            #     ortho_procedure_type = ORDataIntakeForm.prompt_until_valid_answer_given( 'Type of Orthro Procedure', acceptable_options=['1', '2', '3', '4', '5'] )
            #     if ortho_procedure_type == '1':     ortho_procedure_type = 'Arthroscopy'.upper()
            #     elif ortho_procedure_type == '2':   ortho_procedure_type = 'Trauma'.upper()
            #     elif ortho_procedure_type == '3':   ortho_procedure_type = 'BOTH'.upper()
            #     elif ortho_procedure_type == '4':   ortho_procedure_type = 'Derived'.upper(); raise ValueError( f"\tThe provided procedure type is not yet supported. Please contact the Data Librarian for help." )
            #     elif ortho_procedure_type == '5':   ortho_procedure_type = 'All'.upper(); raise ValueError( f"\tThe provided procedure type is not yet supported. Please contact the Data Librarian for help." )

            #     # acceptable_ortho_procedure_names = ORDataIntakeForm._construct_dict_of_ortho_procedure_names( config=metatabl es )
            #     # Separate items into arthroscopy and trauma items
            #     items = config.list_of_all_items_in_table( table_name='Groups' )
            #     arthroscopy_items = [item for item in items if 'arthroscopy' in item.lower()]
            #     trauma_items = [item for item in items if 'arthroscopy' not in item.lower()]

            #     # Create keys for arthroscopy items and for trauma items, combine them into a dictionary
            #     arthroscopy_keys    = [f"1{letter.upper()}" for letter      in string.ascii_lowercase[:len( arthroscopy_items )]]
            #     other_keys          = [f"2{letter.upper()}" for letter      in string.ascii_lowercase[:len( trauma_items )]]
            #     arthroscopy_dict    = {key: item            for key, item   in zip( arthroscopy_keys, arthroscopy_items )}
            #     other_dictacceptable_ortho_procedure_names= {key: item            for key, item   in zip( other_keys, trauma_items )}
            #     acceptable_procedure_options = {**arthroscopy_dict, **other_dict}

            #     acceptable_ortho_procedure_name_options_encoded = {key: value for key, value in acceptable_ortho_procedure_names.items() if key.startswith( ortho_procedure_type )}
            #     options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_ortho_procedure_name_options_encoded.items()] )
            #     print( f'\n\t(7/34)\tWhat is the name of Ortho Procedure?{indent_str}Please select from the following options:\n{options_str}' )
            #     procedure_name_key = ORDataIntakeForm.prompt_until_valid_answer_given( 'Ortho Procedure Name', acceptable_options = list( acceptable_ortho_procedure_name_options_encoded ) )
            #     all_data_pd = all_data_pd[ all_data_pd['procedure'].str.contains(procedure_name_key) ]
            #     print_preview_of_xnat_data( all_data_pd )


        # Ask user if they want to download all data currently in the database.
        print( '\n\tWould you like to download all data currently in the database?\t--\tPlease enter "1" for Yes or "2" for No.' )
        download_all_data = ORDataIntakeForm.prompt_until_valid_answer_given( 'Preview Data?', acceptable_options=['1', '2'] )
        if download_all_data == '1':
            print( '\t...Downloading all data currently in the database...' )
            print( f'\t...Downloading all source data to:\t{download_folder}' )
            print( '\t...')
        
            # Get all subject instances w their labels and corresponding experiment labels.
            proj_inst = xnat.select.project( config.xnat_project_name )
            subj_labels = []
            for s in proj_inst.subjects().get():
                subject_xml = proj_inst.subject(s).get().decode('utf-8')
                root = ET.fromstring(subject_xml)
                label = root.attrib.get('label')
                subj_labels.append(label)
            exp_labels = ['SOURCE_DATA-'+s for s in subj_labels]

            # Write data to specified folder, creating a new subfolder called XNAT_Query, mimick xnat directory structure
            count_files = 0
            for el, sl in zip( exp_labels, subj_labels ):
                print( 'hello')
                f = xnat.select( f'/project/{xnat_connection.xnat_project_name}/subject/{sl}/experiment/{el}/scan/0/resource/SRC/*' )
                source_dir, derived_dir = rf'{download_folder}\{sl}', rf'{download_folder}\{sl}\DERIVED' # SRC is automatically created by .get()
                if not os.path.exists( source_dir ):    os.makedirs( source_dir )
                if not os.path.exists( derived_dir ):   os.makedirs( derived_dir )
                    # os.mkdir( )
                
                # # Notify the user that data exists here already and ask if its ok to overwrite it
                # if os.path.exists( source_dir ) and os.path.exists( derived_dir ):
                #     print( f'\t...Data already exists in {source_dir}. This function may overwrite it -- is that ok?\t--\tPlease enter "1" for Yes or "2" for No.' )
                #     overwrite = ORDataIntakeForm.prompt_until_valid_answer_given( 'Overwrite Data?', acceptable_options=['1', '2'] )
                #     assert overwrite == '1', 'To-Do: Implement exception to user declaring that they do not want to overwrite data (prompt for a new directory).'
                write_d = f.get( dest_dir=source_dir, extract=True ) # type: ignore
                count_files += 1
            
                print( f'\t...Downloaded {count_files} files to {download_folder}!' )
        else: # Present database preview to user and allow them to specify their query.
            pass
        print( '\n-----Concluding Download Process-----\n' )


def header_footer_print( header_or_footer: str ):
    if header_or_footer == 'header': # Print header
        command = 'cls' if os.name == 'nt' else 'clear'
        os.system( command )
        print( '===' *50 )
        print( 'Welcome. Follow the prompts to upload new source data to XNAT; make sure you have your OR Intake Form ready and follow all prompts.')
        print( '\tPress Ctrl+C to cancel at any time.\n')
    elif header_or_footer == 'footer': # Print footer
        print( '\n...Exiting application...' )
        print( '===' *50 + '\n' )


def ask_user_to_confirm_that_they_are_on_the_uiowa_network() -> bool:
    """
    Ask the user to confirm that they are on the UIowa network.

    Returns:
        bool: True if the user confirms they are on the UIowa network.
    """
    print( '\n\tFirst confirm that you are currently on the UIowa network (VPN or in=person)\t--\tPlease enter "1" for Yes or "2" for No.' )
    on_uiowa_network = ORDataIntakeForm.prompt_until_valid_answer_given( 'On UIowa Network?', acceptable_options=['1', '2'] )
    return on_uiowa_network == '1'


def main():
    header_footer_print( header_or_footer='header' )
    username, verbose = parse_args()
    assert ask_user_to_confirm_that_they_are_on_the_uiowa_network(), 'You must be on the UIowa network to use this application.'
    validated_login, xnat_connection, config = try_login_and_connection( username=username, verbose=verbose )
    assert validated_login.is_valid, 'The login credentials provided are not valid, please confirm your username and password; if the error persists, contact the Data Librarian.\n{validated_login}'
    try:
        while True:
            choice = prompt_function( verbose=verbose )
            if choice == '1':
                upload_type = prompt_individual_or_batch_upload( verbose=verbose )
                if upload_type == '1':
                    config = upload_new_case( validated_login=validated_login, xnat_connection=xnat_connection, config=config, verbose=verbose )
                elif upload_type == '2':
                    # Prompt user to input the location of the batch_upload file.
                    print( "\t\t-- Please enter the full path to the batch upload file; do not use apostrophes (') or quotes (\"):\t" )
                    batch_upload_file = input( '\tAnswer:\t' )
                    data = BatchUploadRepresentation( xls_ffn=Path( batch_upload_file ), config=config, verbose=verbose )
                    data.upload_sessions( config=config, validated_login=validated_login, xnat_connection=xnat_connection, verbose=verbose )
            elif choice == '3':
                download_queried_data( validated_login=validated_login, xnat_connection=xnat_connection, config=config, verbose=verbose ) 
            # # elif choice == 2:
            # #     function3()
            # else:
            #     print( "Invalid choice" )

            # prompt user to continue
            print( '\n\tPerform another task?:\tEnter "1" for Yes or "2" for No.' )
            another_task = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
            if another_task == '2':     break
    except KeyboardInterrupt:
        print( '\n\n...User cancelled task via Ctrl+C...' )
    except Exception as e:
        print( f'\n...Task failed due to the following error:\n\t{e}' )
        print( '\n...Closing connection and exiting application...' )
    finally:
        xnat_connection.close()
        header_footer_print( header_or_footer='footer' )


if __name__ == '__main__':
    main()