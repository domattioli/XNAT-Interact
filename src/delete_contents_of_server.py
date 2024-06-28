import os
import argparse
from pyxnat import Interface

def delete_subjects( server: str, username: str, password: str ):
    with Interface( server=server, user=username, password=password ) as xnat:
        all_s_names = xnat.select( '/projects/domSandBox/subjects/*' ).get() # type: ignore
        for s in all_s_names:
            si = xnat.select( '/projects/domSandBox/subjects/' + s )
            try:
                si.delete() # type: ignore
            except:
                print( 'Failed to delete subject: ', s )

if __name__ == "__main__":
    parser = argparse.ArgumentParser( description='Delete subjects from XNAT server.' )
    parser.add_argument( '--server', required=True, help='Server URL' )
    parser.add_argument( '--username', required=True, help='Username' )
    parser.add_argument( '--password', required=True, help='Password' )

    args = parser.parse_args()

    delete_subjects( args.server, args.username, args.password )
    # delete the metatables.json file
    # os.remove( os.path.join( os.getcwd(), 'data', 'metatables.json' ) ) # to-do: generalize this for any user computer?
