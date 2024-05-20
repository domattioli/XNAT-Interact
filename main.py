
import pwinput
import argparse

from src.Utilities import *
from src.DataTypes import *

from typing import Tuple


def ask_user_which_function() -> int:
    print( 'Enter 1, 2, or 3 to choose a function:')
    print( f'\t1. Upload new source data to XNAT.' )
    print( f'\t2. Upload derived data for existing source data to XNAT.' )
    print( f'\t3. Download queried data from XNAT.' )
    return int( input('\t\tSelection --> '))

def prompt_login() -> Tuple[str, str]:
    xnat_user = input( "HawkID Username: " )
    xnat_pass = pwinput.pwinput( prompt="HawkID Password: ", mask="*" )
    return xnat_user, xnat_pass

def try_login():
    try:
        xnat_user, xnat_pass = prompt_login()
        login_info = { 'Username': xnat_user, 'Password': xnat_pass, 'Url': 'https://rpacs.iibi.uiowa.edu/xnat/' }
        return XNATLogin( login_info )
    except Exception as e:
        print( f"\t--Login failed; double check credentials..." )
        return None


def prompt_source_and_group() -> Tuple[str, str]:
    return input( "Acquisition Site: " ), input( "Surgical Procedure: " )
    

def main():
    print( f'===' *50)
    validated_login = try_login()
    if validated_login:
        print( f"\t--Successfully logged in as '{validated_login.validated_username}'!" )
        print( f'===' *50 + '\n')
        metatables = MetaTables( validated_login )
        choice = ask_user_which_function()
        if choice == 1:
            upload_new_case( validated_login=validated_login, metatables=metatables )
        elif choice == 2:
            function2()
        elif choice == 3:
            function3()
        else:
            print("Invalid choice")

def upload_new_case( validated_login: XNATLogin, metatables: MetaTables ):
    print( f"\nUploading new source data to XNAT...")
    print( f'\tRequired inputs:')
    in_dir = input( f"\t\tInput Directory:\t\t" )
    group = input( f'\t\tGroup (surgical procedure):\t' )
    acquisition_site = input( f"\t\tAcquisition Site:\t\t" )
    with XNATConnection( validated_login, metatables ) as connection:
        print( connection )
        source_images = SourceRFSession( dcm_dir=in_dir, login=validated_login, xnat_connection=connection, metatables=metatables, acquisition_site=acquisition_site, group=group, print_out=True )
        zipped_ffn = source_images.write( print_out=True )
        try:
            source_images.publish_to_xnat( zipped_ffn=zipped_ffn, print_out=True, delete_zip=True )
            source_images.catalog_new_data()
        except:
            print( f'\n\n'+'!!!'*3+'\nInputted directory:\n{dcm_dir}\n\t--Duplicate case!\n'+'!!!'*3+'\n\n' )


def function2():
    print("Running function 2")

def function3():
    print("Running function 3")

if __name__ == '__main__':
    main()