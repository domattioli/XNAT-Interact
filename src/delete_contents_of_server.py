import os
import argparse
from pyxnat import Interface

# project_name = r'GROK_AHRQ_real' # invalid as of 7/23/2024
project_name = r'GROK_AHRQ_main'


def delete_subjects( server: Interface ) -> None:
    all_s_names = server.select( f'/projects/{project_name}/subjects/*' ).get() # type: ignore
    for s in all_s_names:
        si = server.select( '/projects/{project_name}/subjects/' + s )
        try:
            si.delete() # type: ignore
        except:
            print( f'\t!!!\tFailed to delete subject: ', s )


def delete_metatables( server: Interface ) -> None:
    project_instance = server.select.project( project_name )
    try:
        f = project_instance.resource('MetaTables').file( 'MetaTables.json' ).delete()
    except:
        print( f'\t!!!\tFailed to delete MetaTables.json file.' )


if __name__ == "__main__":
    parser = argparse.ArgumentParser( description='Delete subjects from XNAT server.' )
    parser.add_argument( '--server', required=True, help='Server URL' )
    parser.add_argument( '--username', required=True, help='Username' )
    parser.add_argument( '--password', required=True, help='Password' )
    parser.add_argument('--method', required=True, choices=['both', 'metatables', 'subjects'], help='Method to delete: subjects, metatables, or both')
    args = parser.parse_args()

    with Interface( args.server, args.username, args.password ) as xnat:
        if args.method in ['subjects', 'both']:
            delete_subjects( server=xnat )
        if args.method in ['metatables', 'both']:
            delete_metatables( server=xnat )
    print( f'\n\t---\tDeletion of {args.method} successfully completed!\t---' )
