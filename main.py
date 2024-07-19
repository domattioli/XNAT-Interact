
import pwinput
import argparse

from pathlib import Path

from typing import Tuple, Optional as Opt

from src.utilities import LibrarianUtilities, MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
from src.xnat_experiment_data import *
from src.xnat_scan_data import *
from src.xnat_resource_data import ORDataIntakeForm

from src.tests import *



def prompt_function( verbose: bool ) -> int:
    if verbose: print( f'\n...selecting task to perform...' )
    print( 'Please enter 1, 2, or 3 to indicate the task that you want to perform:')
    print( f'\t1. Upload new source data to XNAT.' )
    print( f'\t2. Upload derived data for existing source data to XNAT.' )
    print( f'\t3. Download queried data from XNAT.' )
    return int( input('\t\tSelection --> '))


def prompt_verbosity() -> bool:
    # return input( "Print out verbose updates? (y/n): " ).lower() == 'y'
    parser = argparse.ArgumentParser(description='Print-out verbose updates?')
    parser.add_argument('--verbose', type=bool, default=False, help='increase output verbosity')
    args = parser.parse_args()
    if args.verbose:
        print("~~Verbosity turned on~~\n")
    return args.verbose


def prompt_login() -> Tuple[str, str]:
    xnat_user = input( "HawkID Username: " )
    xnat_pass = pwinput.pwinput( prompt="HawkID Password: ", mask="*" )
    return xnat_user, xnat_pass

def try_login_and_connection( verbose: bool ) -> Tuple[XNATLogin, XNATConnection, MetaTables]:
    if verbose: print( f'Please enter your XNAT login credentials to connect to the server:' )
    input_username, input_password = prompt_login()
    if verbose: print( f'\n...logging in and trying to connect to the server...\n' )
    validated_login = XNATLogin( { 'Username': input_username, 'Password': input_password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbose )
    xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbose )
    if verbose: print( f'...connection established...\n' )
    if xnat_connection.is_verified and xnat_connection.is_open:
        metatables = MetaTables( validated_login, xnat_connection, verbose=False ) # don't want to see all this info every time because there is so much and it is really only intended for the librarian to debug with
    else:
        raise ValueError( f"\tThe provided login credentials did not lead to a successful connection. Please try again, or contact the Data Librarian for help." )
    return validated_login, xnat_connection, metatables


def prompt_source_and_group() -> Tuple[str, str]:
    return input( "Acquisition Site: " ), input( "Surgical Procedure: " )
    

def upload_new_case( validated_login: XNATLogin, connection: XNATConnection, metatables: MetaTables, intake_form: ORDataIntakeForm, verbose: Opt[bool]=False ) -> None:
    if verbose: print( f"\nUploading new source data to XNAT...")
    in_dir = Path( intake_form.relevant_folder ) # type: ignore -- for all of these intake form fields because they are Opt[str] and thus its possible that theyre None.
    group = intake_form.group
    acquisition_site = intake_form.acquisition_site
    procedure_type = intake_form.ortho_procedure_type
    dt = intake_form.datetime

    if procedure_type.upper() == 'TRAUMA': # type: ignore
        source_images = SourceESVSession( pn=in_dir,
                                         login=validated_login, xnat_connection=connection, metatables=metatables,
                                         acquisition_site=acquisition_site, group=group, datetime=dt, # type: ignore
                                         resource_files=[intake_form])
    elif procedure_type.upper() == 'ARTHROSCOPY': # type: ignore
        source_images = SourceRFSession( dcm_dir=in_dir,
                                        login=validated_login, xnat_connection=connection, metatables=metatables,
                                        acquisition_site=acquisition_site, group=group ) # type: ignore
    
    # Write zip file
    zipped_ffn = source_images.write( verbose=verbose )
    print( zipped_ffn)
    # write_publish_catalog_subroutine
    try:
        # source_images.publish_to_xnat( zipped_ffn=zipped_ffn, verbose=verbose, delete_zip=True )
        # source_images.catalog_new_data()
        pass
    except:
        print( f'\n\n'+'!!!'*3+'\nInputted directory:\n{dcm_dir}\n\t--Duplicate case!\n'+'!!!'*3+'\n\n' )

    connection.close()

def main():
    print( '\n'*15 + f'===' *50 )
    print( f'Welcome. Follow the prompts to upload new source data to XNAT; make sure you have your OR Intake Form ready and follow all prompts.')
    print( f'\tPress Ctrl+C to cancel at any time.\n')
    verbosity = prompt_verbosity()
    validated_login, connection, metatables = try_login_and_connection( verbose=verbosity )
    
    choice = prompt_function( verbose=verbosity )

    if choice == 1:
        print( f'\n...Beginning data intake process...\n' )
        intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login )
        upload_new_case( validated_login=validated_login, connection=connection, metatables=metatables, intake_form=intake_form, verbose=verbosity )
    # elif choice == 2:
        # function2()
    # elif choice == 3:
    #     function3()
    else:
        print( "Invalid choice" )
    print( '...Exiting...' )
    print( f'===' *50 )


if __name__ == '__main__':
    main()