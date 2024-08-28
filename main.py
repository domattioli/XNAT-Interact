from typing import Optional as Opt, Tuple, Union, List

import os
from pathlib import Path

import pwinput
import argparse 

import xml.etree.ElementTree as ET

from pyxnat import Interface
from src.utilities import MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
from src.xnat_experiment_data import *
from src.xnat_scan_data import *
from src.xnat_resource_data import ORDataIntakeForm


def parse_args() -> Tuple[str, str, bool]:
    parser = argparse.ArgumentParser(description='XNAT Login and Connection')
    parser.add_argument('--username', type=str, default=None, help='XNAT username')
    parser.add_argument('--password', type=str, default=None, help='XNAT password')
    parser.add_argument('--verbose', action='store_true', help='Enable verbose output')
    args = parser.parse_args()
    if args.verbose:    print( f"~~Verbosity turned on~~\n" )
    else:               print( f"~~Verbosity turned off~~\n" )
    return args.username, args.password, args.verbose


def prompt_function( verbose: bool ) -> str:
    if verbose: print( f'\n...selecting task to perform...' )
    print( f'\tTask selection:\n\t\t-- Please enter "1" for Uploading Source Data, "2" for Uploading Derived Data, or "3" for Downloading Data.' )
    return ORDataIntakeForm.prompt_until_valid_answer_given( 'Select a task', acceptable_options=['1', '2', '3'] )


def prompt_login( username: Opt[str]=None, password: Opt[str]=None ) -> Tuple[str, str]:
    if username is None:
        username = input( f"\tHawkID Username:\t" )
    if password is None:
        password = pwinput.pwinput( prompt=f"\t{username.upper()} Password:\t", mask="*" )
    return username, password


def try_login_and_connection( username: Opt[str]=None, password: Opt[str]=None, verbose: Opt[bool]=True ) -> Tuple[XNATLogin, XNATConnection, MetaTables]:
    if username is not None and password is not None:
        if verbose: print( f"\n...logging in and trying to connect to the server as '{username}' with password {'*' * len( password )} ...\n" )
        validated_login = XNATLogin( { 'Username': username, 'Password': password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbose )
        xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbose )
    else:
        if verbose: print( f'Please enter your XNAT login credentials to connect to the server:' )
        username, password = prompt_login( username=username, password=password )
        if verbose: print( f'\n...logging in and trying to connect to the server...\n' )
        validated_login = XNATLogin( { 'Username': username, 'Password': password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbose )
        xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbose )

    if xnat_connection.is_verified and xnat_connection.is_open:
        metatables = MetaTables( validated_login, xnat_connection, verbose=False ) # don't want to see all this info every time because there is so much and it is really only intended for the librarian to debug with
        if verbose: print( f'\n-- A connection was successfully established!\n' )
    else:
        raise ValueError( f"\tThe provided login credentials did not lead to a successful connection. Please try again, or contact the Data Librarian for help." )
    return validated_login, xnat_connection, metatables


def prompt_source_and_group() -> Tuple[str, str]:
    return input( "Acquisition Site: " ), input( "Surgical Procedure: " )
    

def upload_new_case( validated_login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables, verbose: Opt[bool]=False ) -> MetaTables:
    print( f'\n-----Beginning data intake process-----\n' )
    if verbose: print( f"\n...Uploading new source data to XNAT...")

    # Digitize/load an intake form (if it exists already).
    print( f'\tHave you already created a *DIGITAL* intake form with this software for this procedure?\n\t\t-- Please enter "1" for Yes or "2" for No.' )
    form_exists = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
    
    if form_exists == '1':
        form_pn = input( f"\n\tPlease enter the full path to the *parent folder* of the intake form:\t" )
        while not os.path.exists( form_pn ):
            print( f"\t\tThe provided path either (1) does not exist or (2) does not contain a 'RECONSTRUCTED_OR_DATA_INTAKE_FORM' in it. Please try again." )
            form_pn = input( f"\n\tPlease enter the full path to the *parent folder* of the intake form:\t" )
        try:
            intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login, parent_folder=form_pn, verbose=verbose )
            while True:
                print( f'\n\tPlease review the created intake form:\n{intake_form}' )
                print( f'\n\tIs everything correct?\n\t\t-- Please enter "1" for Yes or "2" for No (re-create the intake form).' )
                accept_form = ORDataIntakeForm.prompt_until_valid_answer_given( 'Accept Intake Form As-Is', acceptable_options=['1', '2'] )
                if accept_form == '2':    intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login, verbose=verbose ) #to-do: causes an error and the above try block fails.
                else:            break
        except:
            raise ValueError( f"\t\tThe provided path did not lead to a successful intake form. Please try again, or contact the Data Librarian for help." )
    else: # Prompt user to create a new intake form; then print it to confirm
        while True:
            intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login, verbose=verbose )
            print( f'\n\tPlease review the created intake form:\n{intake_form}' )
            print( f'\n\tIs everything correct?\n\t\t-- Please enter "1" for Yes or "2" for No (re-create the intake form).' )
            accept_form = ORDataIntakeForm.prompt_until_valid_answer_given( 'Accept Intake Form As-Is', acceptable_options=['1', '2'] )
            if accept_form == '2':
                print( f'\n\tRe-doing the form...' )
                continue
            else:               break
    if verbose:                 print( f'\n\tProceeding with the intake form as is...' )
    
    # Depending on the procedure type, create the appropriate source data object.
    if intake_form.ortho_procedure_type.upper() == 'ARTHROSCOPY':
        source_data = SourceESVSession( intake_form=intake_form, metatables=metatables )
    elif intake_form.ortho_procedure_type.upper() == 'TRAUMA':
        raise ValueError( f"\tThe provided procedure type is not yet supported. Please contact the Data Librarian for help." )
        source_data = SourceRFSession( metatables=metatables, login=validated_login, verbose=verbose )
    else:
        raise ValueError( f"\tThe provided procedure type is not yet supported. This is a bug that should be reported to Data Librarian." )
    
    # Publish data to xnat
    metatables = source_data.write_publish_catalog_subroutine( metatables=metatables, xnat_connection=xnat_connection, validated_login=validated_login, verbose=verbose, delete_zip=False )
    if verbose: print( f'\n-----Concluding Upload Process-----\n' )
    return metatables


def download_queried_data( validated_login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables, verbose: Opt[bool]=False ) -> None:
    # for the given project, prompt the user which subjects they want to query, then of those subjects, which of their data they want to download, then download it.
    print( f'\n-----Beginning data download process-----\n' )

    download_folder = Path( os.path.expanduser( "~" ) ) / "Downloads"
    print( f'\t...Downloading all source data to:\t{download_folder}' )
    print( f'\t...')
    
    # contraints = [ ('xnat:subjectData/PROJECT', '=', metatables.xnat_project_name)]
    with Interface( server=validated_login.xnat_project_url, user=validated_login.validated_username, password=validated_login.validated_password ) as xnat:
        # Get all subject instances w their labels and corresponding experiment labels.
        proj_inst = xnat.select.project( metatables.xnat_project_name )
        subj_labels = []
        for s in proj_inst.subjects().get():
            subject_xml = proj_inst.subject(s).get().decode('utf-8')
            root = ET.fromstring(subject_xml)
            label = root.attrib.get('label')
            subj_labels.append(label)
        exp_labels = ['SOURCE_DATA-'+s for s in subj_labels]

        # Write data to downloads folder, mimicky xnat directory structure
        home_dl_dir = rf'C:\Users\dmattioli\Downloads\Source_Data'
        count_files = 0
        for el, sl in zip( exp_labels, subj_labels ):
            f = xnat.select( f'/project/{xnat_connection.xnat_project_name}/subject/{sl}/experiment/{el}/scan/0/resource/SRC/*' )
            source_dir, derived_dir = rf'{home_dl_dir}\{sl}', rf'{home_dl_dir}\{sl}\DERIVED' # SRC is automatically created by .get()
            if not os.path.exists( source_dir ):    os.makedirs( source_dir )
            if not os.path.exists( derived_dir ):   os.makedirs( derived_dir )
                # os.mkdir( )
            
            # Notify the user that data exists here already and ask if its ok to overwrite it
            if os.path.exists( source_dir ) and os.path.exists( derived_dir ):
                print( f'\t...Data already exists in {source_dir}. This function may overwrite it -- is that ok?\t--\tPlease enter "1" for Yes or "2" for No.' )
                overwrite = ORDataIntakeForm.prompt_until_valid_answer_given( 'Overwrite Data?', acceptable_options=['1', '2'] )
                assert overwrite == '1', 'To-Do: Implement exception to user declaring that they do not want to overwrite data (prompt for a new directory).'
            write_d = f.get( dest_dir=source_dir, extract=True ) # type: ignore
            count_files += 1
        
        print( f'\t...Downloaded {count_files} files to {home_dl_dir}...' )

        # Print out the folder hierarchy of that folder
        folder_hierarchy = "\t"
        for root, dirs, files in os.walk(home_dl_dir):
            level = root.replace(home_dl_dir, '').count(os.sep)
            indent = ' ' * 4 * level
            folder_hierarchy += f'{indent}{os.path.basename(root)}/\n'
            sub_indent = ' ' * 4 * (level + 1)
            for f in files:
                folder_hierarchy += f'{sub_indent}{f}\n'
        
        print(folder_hierarchy)

    # # Prompt user for if they want to download data for a specific institution
    # print( f'\tWould you like to download data for a specific institution?\t--\tPlease enter "1" for Yes or "2" for No.' )
    # institution_specific = intake_form.prompt_until_valid_answer_given( 'Query by Institution?', acceptable_options=['1', '2'] )
    # if institution_specific == '1':
    #     acceptable_institution_options_encoded = {str(i+1): institution for i, institution in enumerate( metatables.list_of_all_items_in_table( table_name='ACQUISITION_SITES' ) )}
    #     options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_institution_options_encoded.items()] )
    #     print( f'\tPlease select from the following institutions:\n{options_str}' )
    #     institution_name_key = intake_form.prompt_until_valid_answer_given( 'Institution Name', acceptable_options=list( acceptable_institution_options_encoded ) )
    #     institution_name = acceptable_institution_options_encoded[institution_name_key]
    # else:   institution_name = None

    # print( f'\tWould you like to download data for a specific ortho procedure type?\t--\tPlease enter "1" for Yes or "2" for No.' )
    # ortho_procedure_specific = intake_form.prompt_until_valid_answer_given( 'Query Orthro Procedure Type?', acceptable_options=['1', '2'] )
    # if ortho_procedure_specific == '1':
    #     acceptable_procedure_options_encoded = {str(i+1): procedure for i, procedure in enumerate( ['ARTHRO', 'TRAUMA'] )}
    #     options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_procedure_options_encoded.items()] )
    #     print( f'\tPlease select from the following ortho procedure types:\n{options_str}' )
    #     ortho_procedure_type_key = intake_form.prompt_until_valid_answer_given( 'Ortho Procedure Type', acceptable_options=list( acceptable_procedure_options_encoded ) )
    #     ortho_procedure_type = acceptable_procedure_options_encoded[ortho_procedure_type_key]
    # else:   ortho_procedure_type = None


def header_footer_print( header_or_footer: str ):
    if header_or_footer == 'header': # Print header
        command = 'cls' if os.name == 'nt' else 'clear'
        os.system( command )
        print( f'===' *50 )
        print( f'Welcome. Follow the prompts to upload new source data to XNAT; make sure you have your OR Intake Form ready and follow all prompts.')
        print( f'\tPress Ctrl+C to cancel at any time.\n')
    elif header_or_footer == 'footer': # Print footer
        print( f'\n...Exiting application...' )
        print( f'===' *50 + f'\n' )




def main():
    header_footer_print( header_or_footer='header' )
    username, password, verbose = parse_args()
    validated_login, xnat_connection, metatables = try_login_and_connection( username=username, password=password, verbose=verbose )
    
    try:
        while True:
            choice = prompt_function( verbose=verbose )
            if choice == '1':
                metatables = upload_new_case( validated_login=validated_login, xnat_connection=xnat_connection, metatables=metatables, verbose=verbose )
            elif choice == '3':
                download_queried_data( validated_login=validated_login, xnat_connection=xnat_connection, metatables=metatables, verbose=verbose ) 
            # # elif choice == 2:
            # #     function3()
            # else:
            #     print( "Invalid choice" )

            # prompt user to continue
            print( f'\n\tPerform another task?:\tEnter "1" for Yes or "2" for No.' )
            another_task = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
            if another_task == '2':     break
    except KeyboardInterrupt:
        print( f'\n\n...User cancelled task via Ctrl+C...' )
    except Exception as e:
        print( f'\n...Task failed due to the following error:\n{e}' )
        print( f'\n...Closing connection and exiting application...' )
    finally:
        xnat_connection.close()
        header_footer_print( header_or_footer='footer' )


if __name__ == '__main__':
    main()