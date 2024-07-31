from typing import Optional as Opt, Tuple, Union, List

import os
from pathlib import Path

import pwinput
import argparse 

from src.utilities import MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
from src.xnat_experiment_data import *
from src.xnat_scan_data import *
from src.xnat_resource_data import ORDataIntakeForm

from src.tests import *


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
    print( f'\tSelect a task\t--\tPlease select a task from the following list:\tEnter "1" for Uploading Source Data, "2" for Uploading Derived Data, or "3" for Downloading Data' )
    return ORDataIntakeForm.prompt_until_valid_answer_given( 'Select a task', acceptable_options=['1', '2', '3'] )


def prompt_login() -> Tuple[str, str]:
    xnat_user = input( "HawkID Username: " )
    xnat_pass = pwinput.pwinput( prompt="HawkID Password: ", mask="*" )
    return xnat_user, xnat_pass


def try_login_and_connection( username: Opt[str]=None, password: Opt[str]=None, verbose: Opt[bool]=False ) -> Tuple[XNATLogin, XNATConnection, MetaTables]:
    if username is not None and password is not None:
        input_username, input_password = username, password
        if verbose: print( f'\n...logging in and trying to connect to the server...\n' )
        validated_login = XNATLogin( { 'Username': input_username, 'Password': input_password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbose )
        xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbose )
    else:
        if verbose: print( f'Please enter your XNAT login credentials to connect to the server:' )
        input_username, input_password = prompt_login()
        if verbose: print( f'\n...logging in and trying to connect to the server...\n' )
        validated_login = XNATLogin( { 'Username': input_username, 'Password': input_password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbose )
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
    if verbose: print( f"\nUploading new source data to XNAT...")

    # Digitize/load an intake form (if it exists already).
    print( f'\tDoes an *DIGITAL* intake form already exist? \t--\tPlease select a task from the following list:\tEnter "1" for Yes or "2" for No.' )
    form_exists = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
    
    if form_exists == '1':
        form_ffn = input( f"Please enter the full file path to the intake form: " )
        try:
            intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login, ffn=form_ffn, verbose=False, write_to_file=False )
        except:
            raise ValueError( f"\tThe provided file path did not lead to a successful intake form. Please try again, or contact the Data Librarian for help." )
    else:
        intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login, verbose=False, write_to_file=False )
    
    # Depending on the procedure type, create the appropriate source data object.
    # try:
    #     if intake_form.ortho_procedure_type.upper() == 'ARTHROSCOPY':
    #         source_data = SourceESVSession( intake_form=intake_form, metatables=metatables )
    #     elif intake_form.ortho_procedure_type.upper() == 'TRAUMA':
    #         raise ValueError( f"\tThe provided procedure type is not yet supported. Please contact the Data Librarian for help." )
    #         source_data = SourceRFSession( metatables=metatables, login=validated_login, verbose=verbose )
    #     else:
    #         raise ValueError( f"\tThe provided procedure type is not yet supported. This is a bug that should be reported to Data Librarian." )
    # except:
    #     raise ValueError( f"\tFailed to load in the data pointed-to by the intake form process. Please contact the Data Librarian for help." )
    
    # Publish data to xnat
    # metatables = source_data.write_publish_catalog_subroutine( metatables=metatables, xnat_connection=xnat_connection, validated_login=validated_login, verbose=verbose, delete_zip=False )
    if verbose: print( f'\n-----Exiting Upload Process-----\n' )
    return metatables


def header_footer_print( header_or_footer: str ):
    if header_or_footer == 'header': # Print header
        print( '\n'*15 + f'===' *50 )
        print( f'Welcome. Follow the prompts to upload new source data to XNAT; make sure you have your OR Intake Form ready and follow all prompts.')
        print( f'\tPress Ctrl+C to cancel at any time.\n')
    elif header_or_footer == 'footer': # Print footer
        print( f'\n...Exiting application...' )
        print( f'===' *50 )


def main():
    header_footer_print( header_or_footer='header' )
    username, password, verbose = parse_args()
    validated_login, xnat_connection, metatables = try_login_and_connection( username=username, password=password, verbose=verbose )
    
    while True:
        choice = prompt_function( verbose=verbose )
        print( choice )
        if choice == '1':
            metatables = upload_new_case( validated_login=validated_login, xnat_connection=xnat_connection, metatables=metatables, verbose=verbose )
        else:
            print( f'choice: {choice}\ttype: {type(choice)}' )
        # # elif choice == 2:
        #     # function2()
        # # elif choice == 3:
        # #     function3()
        # else:
        #     print( "Invalid choice" )

        # prompt user to continue
        print( f'\n\tPerform another task?:\tEnter "1" for Yes or "2" for No.' )
        another_task = ORDataIntakeForm.prompt_until_valid_answer_given( 'Intake Form Declaration', acceptable_options=['1', '2'] )
        if another_task == '2':
            break

    xnat_connection.close()
    header_footer_print( header_or_footer='footer' )


if __name__ == '__main__':
    main()