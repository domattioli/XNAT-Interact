
import pwinput
import argparse

from typing import Tuple

from src.utilities import LibrarianUtilities, MetaTables, USCentralDateTime, XNATLogin, XNATConnection, ImageHash
from src.xnat_experiment_data import *
from src.xnat_scan_data import *
from src.xnat_resource_data import ORDataIntakeForm

from src.tests import *



def prompt_function() -> int:
    print( 'Enter 1, 2, or 3 to choose a function:')
    print( f'\t1. Upload new source data to XNAT.' )
    print( f'\t2. Upload derived data for existing source data to XNAT.' )
    print( f'\t3. Download queried data from XNAT.' )
    return int( input('\t\tSelection --> '))


def prompt_login() -> Tuple[str, str]:
    xnat_user = input( "HawkID Username: " )
    xnat_pass = pwinput.pwinput( prompt="HawkID Password: ", mask="*" )
    return xnat_user, xnat_pass

def prompt_verbosity() -> bool:
    # return input( "Print out verbose updates? (y/n): " ).lower() == 'y'
    parser = argparse.ArgumentParser(description='Print-out verbose updates?')
    parser.add_argument('--verbosity', type=bool, default=False, help='increase output verbosity')
    args = parser.parse_args()
    if args.verbosity:
        print("~~Verbosity turned on~~\n")
    return args.verbosity


def try_login_and_connection() -> Tuple[XNATLogin, XNATConnection, MetaTables, bool]:
    verbosity=prompt_verbosity()
    input_username, input_password = prompt_login()
    validated_login = XNATLogin( { 'Username': input_username, 'Password': input_password, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }, verbose=verbosity )
    if verbosity: print( '...logging in and trying to connect to the server...\n' )
    xnat_connection = XNATConnection( login_info=validated_login, stay_connected=True, verbose=verbosity )
    if xnat_connection.is_verified and xnat_connection.is_open:
        metatables = MetaTables( validated_login, xnat_connection, verbose=False ) # don't want to see all this info every time because there is so much and it is really only intended for the librarian to debug with
    else:
        raise ValueError( f"\tThe provided login credentials did not lead to a successful connection. Please try again, or contact the Data Librarian for help." )
    return validated_login, xnat_connection, metatables, verbosity


def prompt_source_and_group() -> Tuple[str, str]:
    return input( "Acquisition Site: " ), input( "Surgical Procedure: " )
    

# def upload_new_case( validated_login: XNATLogin, metatables: MetaTables ):
#     print( f"\nUploading new source data to XNAT...")
#     print( f'\tRequired inputs:')
#     in_dir = input( f"\t\tInput Directory:\t\t" )
#     group = input( f'\t\tGroup (surgical procedure):\t' )
#     acquisition_site = input( f"\t\tAcquisition Site:\t\t" )
#     with XNATConnection( validated_login, metatables ) as connection:
#         print( connection )
#         source_images = SourceRFSession( dcm_dir=in_dir, login=validated_login, xnat_connection=connection, metatables=metatables, acquisition_site=acquisition_site, group=group, print_out=True )
#         zipped_ffn = source_images.write( print_out=True )
#         try:
#             source_images.publish_to_xnat( zipped_ffn=zipped_ffn, print_out=True, delete_zip=True )
#             source_images.catalog_new_data()
#         except:
#             print( f'\n\n'+'!!!'*3+'\nInputted directory:\n{dcm_dir}\n\t--Duplicate case!\n'+'!!!'*3+'\n\n' )


def main():

    print( '\n'*15 + f'===' *50 )
    print( f'Welcome. Follow the prompts to upload new source data to XNAT; make sure you have your OR Intake Form ready and follow all prompts.')
    print( f'\tPress Ctrl+C to cancel at any time.\n')
    validated_login, xnat_connection, metatables, verbosity = try_login_and_connection()
    print( f'===' *50 )
    print( f'...Beginning data intake process...\n' )
    intake_form = ORDataIntakeForm( metatables=metatables, login=validated_login )
    print( intake_form.)
    choice = prompt_function()
    if choice == 1:
        upload_new_case( validated_login=validated_login, metatables=metatables )
    elif choice == 2:
        function2()
    elif choice == 3:
        function3()
    else:
        print("Invalid choice")


if __name__ == '__main__':
    main()