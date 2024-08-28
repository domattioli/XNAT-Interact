import json
import os
from pathlib import Path
import cv2
import numpy as np
import pandas as pd
from datetime import datetime
from dateutil import parser
import pytz
import hashlib
import tempfile

from pyxnat import Interface
from pyxnat.core.resources import Project as pyxnatProject

from typing import Optional as Opt, Union, Tuple, List as typehintList, Dict as typehintDict, AnyStr as typehintAnyStr

from pydicom.uid import UID as pydicom_UID, generate_uid as generate_pydicomUID

import matplotlib.pyplot as plt


# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['UIDandMetaInfo', 'XNATLogin', 'MetaTables', 'XNATConnection', 'USCentralDateTime', 'ImageHash']


data_librarian_hawk_id = 'dmattioli' # Update as needed.

#--------------------------------------------------------------------------------------------------------------------------
## Helper class for inserting all information that should only be used locally within the _local_variables class def below:
class _local_variables:
    def __init__( self ):
        self._img_sizes = ( 256, 256 )
        self.__dict__.update( self._set_local_variables() )

        # ensure that the temp folder exists on the local machine. Note sure that this will work for macs/unix
        if not os.path.exists( self.tmp_data_dir ):     os.makedirs( self.tmp_data_dir )


    def _read_template_image( self, template_ffn: str ) -> np.ndarray:
        return cv2.resize( cv2.imread( template_ffn, cv2.IMREAD_GRAYSCALE ), self._img_sizes ).astype( np.uint8 )


    def __getattr__( self, attr ):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            raise AttributeError( f"Attribute {attr} is not yet defined." )
    

    def __str__( self ) -> str:
        return '\n'.join([f'{k}:\t{v}' for k, v in self.__dict__.items()])


    def _set_local_variables( self ) -> dict: # !!DO NOT DELETE!! This is the only place where these local variables/paths are defined.
        # xnat_project_name = 'domSandBox' # original corrupted project.
        # xnat_project_name = 'GROK_AHRQ_main' # another corrupted project -- added a user who wasnt registered, lost ability to do anything except add new data.
        xnat_project_name = 'GROK_AHRQ_Data'
        xnat_url = r'https://rpacs.iibi.uiowa.edu/xnat/'
        xnat_config_folder_name, config_fn = 'config', 'database_config.json'
        template_img_dir = os.path.join( os.getcwd(), 'data', 'image_templates', 'unwanted_dcm_image_template.png' )
        tmp_data_dir = os.path.join( tempfile.gettempdir(), 'XNAT_Interact' ) # create a directory with the software name in the user's Temp folder.
        redacted_string = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"
        local_vars =  {
                        # 'doc_dir': doc_dir,
                        # 'data_dir': data_dir,
                        'tmp_data_dir': tmp_data_dir,
                        'config_fn': config_fn,
                        'config_ffn': os.path.join( tmp_data_dir, config_fn ), # local file for storing all meta information
                        'required_login_keys': ['USERNAME', 'PASSWORD', 'URL'],
                        'xnat_project_name': xnat_project_name,
                        'xnat_project_url': xnat_url,
                        'xnat_config_folder_name': xnat_config_folder_name,
                        'default_meta_table_columns' : ['NAME', 'UID', 'CREATED_DATE_TIME', 'CREATED_BY'],
                        'template_img_dir' : template_img_dir,
                        'data_librarian': data_librarian_hawk_id.upper(),
                        'template_img' : self._read_template_image( template_img_dir ),
                        'acceptable_img_dtypes' : [np.uint8, np.int8, np.uint16, np.int16, np.uint32, np.int32, np.uint64, np.int64],
                        'required_img_size_for_hashing' : self._img_sizes,
                        'mturk_batch_col_names' :['HITId', 'HITTypeId', 'Title', 'Description', 'Keywords', 'Reward',
                                                'CreationTime', 'MaxAssignments', 'RequesterAnnotation',
                                                'AssignmentDurationInSeconds', 'AutoApprovalDelayInSeconds',
                                                'Expiration', 'NumberOfSimilarHITs', 'LifetimeInSeconds',
                                                'AssignmentId', 'WorkerId', 'AssignmentStatus', 'AcceptTime',
                                                'SubmitTime', 'AutoApprovalTime', 'ApprovalTime', 'RejectionTime',
                                                'RequesterFeedback', 'WorkTimeInSeconds', 'LifetimeApprovalRate',
                                                'Last30DaysApprovalRate', 'Last7DaysApprovalRate', 'Input.image_url',
                                                'Approve','Reject'],
                        'redacted_string': redacted_string
                        }
        # local_vars.template_img_hash = ImageHash( local_vars.template_img_dir ).hashed_img
        return local_vars


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all utitlities to inherit from.
class UIDandMetaInfo:
    def __init__( self ):
        self._local_variables = _local_variables()
        self._uid = self.generate_uid()
    
    @property
    def uid( self )                             -> str:                 return self._uid
    @property
    def local_variables( self )                 -> _local_variables:    return self._local_variables
    @property
    def tmp_data_dir( self )                    -> Path:                return self.local_variables.tmp_data_dir
    @property
    def cataloged_resources_ffn( self )         -> str:                 return self.local_variables.cataloged_resources_ffn
    @property
    def config_fn( self )                       -> str:                 return self.local_variables.config_fn
    @property
    def config_ffn( self )                      -> str:                 return self.local_variables.config_ffn
    @property
    def xnat_config_folder_name ( self )        -> str:                 return self.local_variables.xnat_config_folder_name
    @property
    def required_login_keys( self )             -> list:                return self.local_variables.required_login_keys
    @property
    def xnat_project_name( self )               -> str:                 return self.local_variables.xnat_project_name
    @property
    def xnat_project_url( self )                -> str:                 return self.local_variables.xnat_project_url
    @property
    def default_meta_table_columns( self )      -> list:                return self.local_variables.default_meta_table_columns
    @property
    def template_img_dir( self )                -> str:                 return self.local_variables.template_img_dir
    @property
    def template_img( self )                    -> np.ndarray:          return self.local_variables.template_img
    @property
    def acceptable_img_dtypes( self )           -> list:                return self.local_variables.acceptable_img_dtypes
    @property
    def required_img_size_for_hashing( self )   -> tuple:               return self.local_variables.required_img_size_for_hashing
    @property
    def mturk_batch_col_names( self )           -> list:                return self.local_variables.mturk_batch_col_names
    @property
    def redacted_string( self )                 -> str:                 return self.local_variables.redacted_string
    @property
    def data_librarian( self )                  -> str:                 return self.local_variables.data_librarian


    @property
    def now_datetime( self )                    -> str:                 return datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()
    

    def convert_all_kwarg_strings_to_uppercase( **kwargs ):
        return {k: v.upper() if isinstance(v, str) else v for k, v in kwargs.items()}


    def generate_uid( self )                    -> str:                 return str( generate_pydicomUID( prefix=None, entropy_srcs=[self.now_datetime] ) ).replace( '.', '_' )
    def is_valid_pydcom_uid( self, uid_str: str )   -> bool:            return pydicom_UID( uid_str.replace( '_', '.' ) ).is_valid
                                                

#--------------------------------------------------------------------------------------------------------------------------
## Class for validating xnat login information
class XNATLogin( UIDandMetaInfo ):
    '''XNATLogin( input_info = {'URL': '...', 'USERNAME': '...', 'PASSWORD': '...'} )
        A class for storing data that is necessary for logging into the XNAT RPACS server.
        For provided login dictionary, ensure that:
            1.  Provided server url is correct,
            2.  Provided keys are all correct.
                - User-provided key strings are auto-capitalized, but the corresponding value strings are not*, except for the user-provided url.
                - If your attempts to instantiate keep failing, try input all keys with all-caps characters.
        Note: This class does not verify the login with the xnat server, it only validates it according to the above criteria.
            -   For verifying login information, refer to XNATConnection, another class definion that inherits from XNATLogin.

        # Example Usage:
        test1 = XNATLogin( {'URL': 'https://rpacs.iibi.uiowa.edu/xnat/', 'USERNAME': 'iamacomputer', 'PASSWORD': 'hello_world'} )
        print( test1 )
        test2 = XNATLogin( {'URL': 'www.google.com', 'USERNAME': 'dmattioli', 'PASSWORD': 'hello_world'} )
        print( test2 )
    '''

    def __init__( self, input_info: dict, verbose: Opt[bool] = False ):
        super().__init__()  # Call the __init__ method of the base class
        self._validated_username, self._validated_password = '', '', 
        self._is_valid, self._user_role = False, ''
        self._validate_login( input_info )
        if verbose:
            print( self )

    def _validate_login( self, input_info ): # If all checks pass, set _is_valid to True and deal login info
        assert len( input_info ) == 3, f"Provided login info dictionary must only have the following three key-value pairs: {self.required_login_keys}"
        self._provided_info, validated_info = input_info, {k.upper(): v for k, v in input_info.items()}
        assert validated_info['URL'].lower() == self.local_variables.xnat_project_url, f"Provided server URL is invalid: {validated_info['URL']}"
        assert all( k in validated_info for k in self.required_login_keys ), f"Missing login info: {set( self.required_login_keys ) - set( validated_info.keys() )}"
        self._is_valid, self._validated_username, self._validated_password = True , validated_info['USERNAME'], validated_info['PASSWORD']

    @property
    def provided_info( self )       -> dict:    return self._provided_info
    @property
    def is_valid( self )            -> bool:    return self._is_valid
    @property
    def user_role( self )           -> str:     return self._user_role
    @property
    def validated_username( self )  -> str:     return self._validated_username
    @property
    def validated_password( self )  -> str:     return self._validated_password


    def __str__( self ) -> str:
        if self.is_valid:                       return f"-- Validated XNATLogin --\n\tUser: {self.validated_username}\n\tServer: {self.xnat_project_url}\n"
        return f"-- Invalid XNATLogin --\n\tUser: {self.validated_username}\n\tServer: {self.xnat_project_url}\n"

    # def doc( self ) -> str: return self.__doc__


#--------------------------------------------------------------------------------------------------------------------------
## Class for establishing a connection to the xnat server with specific credentials.
class XNATConnection( UIDandMetaInfo ):
    '''
    to-do: document explanation of class

    # Example usage:
    my_login_info = {'URL': 'https://rpacs.iibi.uiowa.edu/xnat/', 'USERNAME': '...', 'PASSWORD': '...'}
    my_login = XNATLogin( login_info )
    my_connection = XNATConnection( my_login, my ) 
    print( my_connection )
    # my_connection = XNATConnection( my_login, my, stay_connected=True ) 
    # print( my_connection )
    '''
    _instance = None

    def __new__( cls, *args, **kwargs ): # Only one instance of this class should be allowed to exist at a time.
        if cls._instance is not None:   cls._instance.__del__()  # Explicitly call __del__ on the existing instance
        cls._instance = super( XNATConnection, cls ).__new__( cls )
        return cls._instance

    def __init__( self, login_info: XNATLogin, stay_connected: bool = False, verbose: Opt[bool] = False ):
        assert login_info.is_valid, f"Provided login info must be validated before accessing xnat server: {login_info}"
        super().__init__()  # Call the __init__ method of the base clas
        self._login_info, self._project_handle, self._is_verified, self._is_open = login_info, None, False, stay_connected
        self._verify_login()
        if stay_connected is False:         self.server.disconnect()
        if verbose:                         print( self )


    @property
    def login_info( self )          -> XNATLogin:       return self._login_info
    @property
    def server( self )              -> Interface:       return self._server
    @property
    def project_query_str( self )   -> str:             return self._project_query_str
    @property
    def project_handle( self )      -> pyxnatProject:   return self._project_handle # type: ignore
    @property
    def is_verified( self )         -> bool:            return self._is_verified
    @property
    def is_open( self )             -> bool:            return self._is_open
    @property
    def get_user( self )            -> Opt[str]:        return self.login_info.validated_username
    @property
    def get_password( self )        -> Opt[str]:        return self.login_info.validated_password


    def _verify_login( self ):
        self._server = self._establish_connection()
        self._grab_project_handle() # If more tests in the future, separate as its own function.
        self._is_verified = True


    def _establish_connection( self ) -> Interface:     return Interface( server=self.xnat_project_url, user=self.get_user, password=self.get_password )


    def _grab_project_handle( self ):
        self._project_query_str = '/project/' + self.xnat_project_name
        project_handle = self.server.select( self.project_query_str )
        if project_handle.exists(): # type: ignore
            self._project_handle = project_handle


    def close( self ):
        if hasattr( self, '_server' ):
            self._server.disconnect()
        self._open = False
        print( f"\n\t*Prior connection to XNAT server, '{self.uid}', has been closed -- local metatable data will be deleted!\n" )


    def __del__( self ):
        self.close() # Close the server connection ***AND*** delete the metatables local data to enforce user to always pull it from the server first.
        if os.path.exists( self.config_ffn ):      os.remove( self.config_ffn )
        XNATConnection._instance = None

    def __enter__( self ):                              return self

    def __exit__( self, exc_type, exc_value, traceback ):   self.close()

    def __str__( self ) -> str:
        return (f"-- XNAT Connection: {'Open' if self.is_open else 'Closed'} --\n"
                f"\tSigned-in as:\t{self.get_user}\n"
                # f"UID:\t\t{self.uid}\n"
                f"\tProject:\t{self.project_handle}" )
    

#--------------------------------------------------------------------------------------------------------------------------
## Class for cataloging all seen data and user info.
class MetaTables( UIDandMetaInfo ):
    '''
    Class for accessing and updating meta information for subjects and experiments. Think of it as a psuedo-relational database.
        Note: all inputted new tables and items are automatically capitalized on the backend to ensure no duplicates.
            - As are usernames.
    
    Built-in tables include:
        1. Acquisition Sites, i.e., hospitals/universities from which **raw** data originates.
            Currently supporting:
            - 'UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS',
            - 'UNIVERSITY_OF_HOUSTON'
        2. Groups, i.e., surgical procedure names for which our data currently can handle.
            Currently supporting:
            - 'PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE',
            - 'DYNAMIC_HIP_SCREW',
            - 'KNEE_ARTHROSCOPY',
            - 'INTERMEDULLARY_NAIL',
            - 'TROCHANTERIC_STABILIZING_PLATE'
        3. Subjects, i.e., unique identifiers for subjects in the database.
        4. Image Hashes, i.e., unique identifiers for images that have been processed.

    **For now we are using a json file to represent our tables. 
        - In the future it might make more sense to use a simple relational database.

    # Example usage (if not building from scratch, try adding a new table other than those listed above):
    my_login_info = { 'USERNAME': '...', 'PASSWORD' : '...', 'URL' : 'https://rpacs.iibi.uiowa.edu/xnat/' }
    my_login = XNATLogin( my_login_info )
    my_connection = XNATConnection( my_login )
    mt = MetaTables( my_login, my_connection )
    print( mt )

    # Create some new tables -- convention is for inputs to be plural. Will automatically caps everything
    mt.add_new_table( table_name='hello' )

    # Add new items to the tables - convention is for inputs to be plural
    mt.add_new_item( table_name='hello', item_name='world' )
    # mt.save()
    '''
    def __init__( self, login_info: XNATLogin, xnat_connection: XNATConnection, verbose: Opt[bool] = False ):
        assert login_info.is_valid, f"Provided login info must be validated before accessing metatables: {login_info}"
        assert xnat_connection.is_open, f"Provided xnat connection must be open before accessing metatables: {xnat_connection}"
        super().__init__()  # Call the __init__ method of the base class to ensure that we inherit all those local variables
        self._login_info, self._xnat_connection = login_info, xnat_connection
        # Need to try to pull it from the xnat server if it exists, otherwise create it from scratch.
        try:
            self.pull_from_xnat( verbose=False )
        except:
            self._instantiate_json_file()
            self._initialize_metatables()
            self.push_to_xnat( verbose=False )
        if verbose:                         print( self )
            

    @property
    def login_info( self )          -> XNATLogin:       return self._login_info
    @property
    def xnat_connection( self )     -> XNATConnection:  return self._xnat_connection
    @property
    def tables( self )              -> dict:            return self._tables
    @property
    def metadata( self )            -> dict:            return self._metadata
    @property
    def accessor_username( self )   -> str:             return self.login_info.validated_username.upper() 
    @property
    def accessor_uid( self )        -> str:             return self.get_uid( 'REGISTERED_USERS', self.accessor_username )


    #==========================================================PRIVATE METHODS==========================================================
    def _reinitialize_metatables_with_extra_columns( self ) -> None:
        # iterate through each table in self._tables and ensure that all columns denoted in self.metadata['TABLE_EXTRA_COLUMNS'] are present.
        for table_name, table in self._tables.items():
            # Ensure default meta table columns are present
            for col in self.default_meta_table_columns:
                if col not in table.columns:
                    table[col] = None
            if table_name in self.metadata['TABLE_EXTRA_COLUMNS']:
                for col in self.metadata['TABLE_EXTRA_COLUMNS'][table_name]:
                    if col not in table.columns:
                        table[col] = None
    

    def _instantiate_json_file( self ):
        '''#Instantiate with a registered users table.'''
        self._validate_login_for_important_functions( assert_librarian=True )
        now_datetime = self.now_datetime
        librarian_uid_init = self.generate_uid()
        default_users = { self.data_librarian: [librarian_uid_init, now_datetime, librarian_uid_init] }
        # if self.accessor_username not in ( k.upper() for k in default_users.keys() ):
        #     default_users[self.accessor_username] = [self.generate_uid(), now_datetime, 'INIT']
        data = [[name] + info for name, info in default_users.items()]
        self._tables = {    'REGISTERED_USERS': pd.DataFrame( data, columns=self.default_meta_table_columns ) }
        self._metadata = {  'CREATED': now_datetime,
                            'LAST_MODIFIED': now_datetime,
                            'CREATED_BY': self.accessor_uid,
                            'TABLE_EXTRA_COLUMNS': {} }
            

    def _initialize_metatables( self ) -> None:
        self.add_new_table( 'AcquisitioN_sites' )
        self.add_new_item( 'acquisitIon_sites', 'UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS' )
        self.add_new_item( 'acqUisition_sites', 'UNIVERSITY_OF_HOUSTON' )
        self.add_new_item( 'ACQUISITION_SItes', 'AMAZON_MECHANICAL_TURK' )

        self.add_new_table( 'gRouPs' )
        # trauma:
        self.add_new_item( 'grOups', 'Open_reduction_hip_fracture–Dynamic_hip_screw' )
        self.add_new_item( 'grOups', 'Open_reduction_hip_fracture–Cannulated_hip_screw' )
        self.add_new_item( 'grOups', 'Closed_reduction_hip_fracture–Cannulated_hip_screw' )
        self.add_new_item( 'grOups', 'Percutaneous_sacroliac_fixation' )
        self.add_new_item( 'groUps', 'PEDIATRIC_SUPRACONDYLaR_HUMERUS_FRACTURE_reduction_and_pinning' ) #FYI caps doesn't matter -- it all gets capitalized anyway.
        self.add_new_item( 'grOups', 'Open_and_percutaneous_pilon_fractures' )
        self.add_new_item( 'grOups', 'Intramedullary_nail-CMN' )
        self.add_new_item( 'grOups', 'Intramedullary_nail-Antegrade_femoral' )
        self.add_new_item( 'grOups', 'Intramedullary_nail-Retrograde_femoral' )
        self.add_new_item( 'grOups', 'Intramedullary_nail-Tibia' )
        self.add_new_item( 'grOups', 'Scaphoid_Fracture' )
        self.add_new_item( 'grOups', 'sacroiliac_screw' )
        self.add_new_item( 'grOups', 'slipped_capital_femoral_epiphysis' )

        # arthro
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Pre_Diagnostic' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Post_Diagnostic' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Rotator_Cuff_Repair' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Distal_Clavical_Resect/Subacrom_Decomp' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Labrum' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Superior_Labrum_Anterior_to_Posterior' )
        self.add_new_item( 'groups', 'SHOULDER_ARTHROSCOPY-Other' ) # to-do: eventually do an accounting of performances selected as other so we can provide more specific options
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Pre_Diagnostic' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Post_Diagnostic' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Cartilage_Resurfacing' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Medial_Patella_Femoral_Ligament' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Meniscal_Transplant' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY-Other' )
        self.add_new_item( 'groups', 'Hip_ARTHROSCOPY' )
        self.add_new_item( 'groups', 'Ankle_ARTHROSCOPY' )

        # Init tables for subjects (performances), image hashes, surgeons, and registered users (of this software)
        self.add_new_table( 'subjects', ['acquisition_site', 'group'] ) # need additional columns to reference uids from other tables
     
        self.add_new_table( 'IMAGE_HASHES', ['subject', 'INSTANCE_NUM'] ) # need additional columns to reference uids from other tables

        self.add_new_table( 'Surgeons', ['first_name', 'last_name', 'middle_initial'] )
        self.add_new_item( 'surgeons', 'unknown', extra_columns_values={'first_name':'', 'last_name': '', 'middle_initial': '' } )
        self.add_new_item( 'surgeons', 'karamm', extra_columns_values={'first_name':'MATTHEW', 'last_name': 'KARAM', 'middle_initial': 'D' } )
        self.add_new_item( 'surgeons', 'kowalskih', extra_columns_values={'first_name':'HEATHER', 'last_name': 'KOWALSKI', 'middle_initial': 'R' } )
        self.add_new_item( 'surgeons', 'mbollier', extra_columns_values={'first_name':'MATTHEW', 'last_name': 'BOLLIER', 'middle_initial': 'J' } )
        self.add_new_item( 'surgeons', 'wolfb', extra_columns_values={'first_name':'BRIAN', 'last_name': 'WOLF', 'middle_initial': 'R' } )
        self.add_new_item( 'surgeons', 'rwestermann', extra_columns_values={'first_name':'ROBERT', 'last_name': 'WESTERMAN', 'middle_initial': '' } )
        self.add_new_item( 'surgeons', 'kduchman', extra_columns_values={'first_name':'KYLE', 'last_name': 'DUCHMAN', 'middle_initial': 'R' } )
        self.add_new_item( 'surgeons', 'tdenhartog', extra_columns_values={'first_name':'TAYLOR', 'last_name': 'DEN_HARTOG', 'middle_initial': 'J' } )
        self.add_new_item( 'surgeons', 'cjmaly', extra_columns_values={'first_name':'CONNOR', 'last_name': 'MALY', 'middle_initial': 'J' } )
        self.add_new_item( 'surgeons', 'ryanse', extra_columns_values={'first_name':'SARAH', 'last_name': 'RYAN', 'middle_initial': 'E' } )
        self.add_new_item( 'surgeons', 'brwilkinson', extra_columns_values={'first_name':'BRADY', 'last_name': 'WILKINSON', 'middle_initial': 'R' } )

        self.add_new_item( 'registered_users', 'gthomas' )
        self.add_new_item( 'registered_users', 'andersondd' )
        self.add_new_item( 'registered_users', 'mtatum' )
        self.add_new_item( 'registered_users', 'stelong' )
        self.add_new_item( 'registered_users', 'jhill7' )
        self.add_new_item( 'registered_users', 'ezwilliams' )


    def _load( self, local_meta_tables_ffn: Opt[Path], verbose: Opt[bool] = False ) -> None:
        assert self.login_info.is_valid, f"Provided login info must be validated before loading metatables: {self.login_info}"
        if local_meta_tables_ffn is None:   load_ffn = self.config_ffn
        else:
            assert os.path.isfile( local_meta_tables_ffn ), f"Provided file path must be a valid file: {local_meta_tables_ffn}"
            load_ffn = local_meta_tables_ffn
        with open( load_ffn, 'r' ) as f:
            data = json.load( f )
        self._tables = { name: pd.DataFrame.from_records( table ) for name, table in data['tables'].items()}
        self._metadata = data['metadata']
        if verbose:         print( f'\tSUCCESS! -- Loaded config file from: {load_ffn}\n' )
    

    def _update_metadata( self, new_table_extra_columns: Opt[dict] = None ) -> None:
        self.metadata.update( {'LAST_MODIFIED': self.now_datetime, 'CREATED_BY': self.accessor_uid} )
        if new_table_extra_columns is not None:
            for k, v in new_table_extra_columns.items():
                assert isinstance( v, list ), f'Extra column names for Table "{k}" must be a list of strings.'
                assert all( isinstance( c, str ) for c in v ), f'Extra column names for Table "{k}" must be a list of strings.'
                assert len( v ) > 0, f'List for Table "{k}" must have at least one extra column name.'
                assert k not in self.metadata['TABLE_EXTRA_COLUMNS'], f'Cannot add Table "{k}" more than once.'
                self._metadata['TABLE_EXTRA_COLUMNS'][k] = [c.upper() for c in v]
    

    def _init_table_w_default_cols( self ) -> pd.DataFrame: return pd.DataFrame( columns=self.default_meta_table_columns ).assign( CREATED_DATE_TIME=self.now_datetime, CREATED_BY=self.accessor_uid )
    

    def _validate_login_for_important_functions( self, assert_librarian: Opt[bool]=False ) ->  None:
        assert self.login_info.is_valid, f"Provided login info must be validated before accessing config file: {self.login_info}"
        if hasattr( self, '_tables' ):      assert self.is_user_registered(), f'User {self.accessor_uid} must first be registed before saving config file.'
        if assert_librarian:                assert self.accessor_username.upper() == self.data_librarian, f'Only user {self.data_librarian} can push config file to the xnat server.'
    
    
    def _custom_json_serializer( self, data, indent=4 ):
        def serialize( obj, depth=0 ):
            if isinstance( obj, dict ):
                items = [f'\n{" " * (depth + indent)}"{k}": {serialize(v, depth + indent)}' for k, v in obj.items()]
                return f'{{{",".join(items)}\n{" " * depth}}}'
            elif isinstance( obj, list ):
                items = [serialize(v, depth) for v in obj]  # Keep depth unchanged for arrays
                return f'[{", ".join(items)}]'
            elif isinstance( obj, str ):    return json.dumps( obj )
            else:                           return str( obj )
        return serialize( data )


    #==========================================================PUBLIC METHODS==========================================================
    def pull_from_xnat( self, write_ffn: Opt[Path]=None, verbose: Opt[bool] = False ) -> Opt[Path]:
        if write_ffn is None:   write_ffn = self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).get_copy( self.config_ffn )
        else:
            assert isinstance( write_ffn, Path ), f"Provided write file path must be a valid Path object: {write_ffn}"
            assert write_ffn.suffix == '.json', f"Provided write file path must have a '.json' extension: {write_ffn}"
            write_ffn = self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).get_copy( write_ffn )
        self._load( write_ffn, verbose )
        if verbose:                     print( f'\t...Metatables successfully populated from XNAT data.\n' )
        
        self._reinitialize_metatables_with_extra_columns()
        return write_ffn


    def ensure_primary_keys_validity( self ) -> None:
        '''Ensure that Subjects found in Image Hashes table are also found in the Subjects table.'''
        # # Test 1: Ensure that all subjects in the Image Hashes table are also in the Subjects table
        # unique_referenced_subject_uids = self._tables['IMAGE_HASHES']['SUBJECT'].unique()
        # unique_subjects = self.list_of_all_items_in_table( 'SUBJECTS' )
        # assert sorted( unique_referenced_subject_uids ) == sorted(unique_subjects ), 'The unique subjects in the IMAGE_HASHES table do not match the unique subjects in the SUBJECTS table'
        # # Test 2: Ensure ...
        pass


    def push_to_xnat( self, verbose: Opt[bool] = False ) -> None:
        self.ensure_primary_keys_validity()
        self.save( verbose )
        self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).put( self.config_ffn, content='META_DATA', format='JSON', tags='DOC', overwrite=True )
        if verbose is True:             print( f'\t...Metatables (config.json) successfully updated on XNAT!\n' )


    def save( self, verbose: Opt[bool] = False ) -> None: # Convert all tables to JSON; Write the data to the file
        '''Only saves locally. To save to the server, all the 'catalog_new_data' method(s) in the experiment class(es) must be called.'''
        self._validate_login_for_important_functions( assert_librarian=False ) # To-do: is this necessary if the user musts create a valid xnat connection first (which should check the same thing)?
        tables_json = {name: df.to_dict( 'records' ) for name, df in self.tables.items()}
        data = {'metadata': self.metadata, 'tables': tables_json }
        # json_str = json.dumps( data, indent=2, separators=( ',', ':' ) )
        json_str = self._custom_json_serializer( data )
        with open( self.config_ffn, 'w' ) as f:         f.write( json_str )
        if verbose:                     print( f'\tSUCCESS! --- saved metatables to: {self.config_ffn}\n' )


    def is_user_registered( self, user_name: Opt[str] = None ) -> bool:
        '''Note that this will automatically capitalize the inputted user_name.'''
        if user_name is None:   user_name = self.accessor_username
        return user_name.upper() in self.tables['REGISTERED_USERS']['NAME'].values


    def register_new_user( self, user_name: str, verbose: Opt[bool] = False ):
        self._validate_login_for_important_functions( assert_librarian=True )   
        if not self.is_user_registered( user_name ):
            self.add_new_item( 'REGISTERED_USERS', user_name )
        if verbose:                     print( f'\tSUCCESS! --- Registered new user: {user_name}\n' )


    def list_of_all_tables( self ) -> list:
        return list( self.tables.keys() )


    def list_of_all_items_in_table( self, table_name: str ) -> list:
        if table_name.upper() in self.tables and not self.tables[table_name.upper()].empty:
            return list (self.tables[table_name.upper()]['NAME'] )
        else:   return [] # Return an empty list if the table does not exist or is empty
            

    def table_exists( self, table_name: str ) -> bool:
        return table_name.upper() in self.list_of_all_tables()


    def item_exists( self, table_name: str, item_name: str ) -> bool:
        return not self.tables[table_name.upper()].empty and item_name.upper() in self.tables[table_name.upper()].values


    def add_new_table( self, table_name: str, extra_column_names: Opt[typehintList[str]] = None, verbose: Opt[bool] = False ) -> None:
        assert self.is_user_registered(), f"User '{self.accessor_username}' must first be registed before adding new items."
        table_name = table_name.upper()
        assert not self.table_exists( table_name ), f'Cannot add table "{table_name}" because it already exists.'
        self._tables[table_name] = self._init_table_w_default_cols()
        if extra_column_names: # checks if it is not None and if the dict is not empty
            for c in extra_column_names: 
                self._tables[table_name][c.upper()] = pd.Series( [None] * len( self._tables[table_name] ) ) # don't forget to convert new column name to uppercase
            self._update_metadata( new_table_extra_columns={table_name: extra_column_names} )
        else:
            self._update_metadata()
        if verbose:                     print( f'\tSUCCESS! --- Added new "{table_name}" table.\n' )


    def add_new_item( self, table_name: str, item_name: str, item_uid: Opt[str] = None, extra_columns_values: Opt[typehintDict[str, str]] = None, verbose: Opt[bool] = False ) -> None:
        assert self.is_user_registered(), f"User '{self.accessor_username}' must first be registed before adding new items."
        table_name, item_name = table_name.upper(), item_name.upper(),
        assert self.table_exists( table_name ), f"Cannot add item '{item_name}' to table '{table_name}' because that table does not yet exist.\n\tTry creating the new table before adding '{item_name}' as a new item."
        assert not self.item_exists( table_name, item_name ), f'Cannot add item "{item_name}" to Table "{table_name}" because it already exists.'
        
        # Ensure all provided extra column names exist in the table, considering case-insensitivity
        if extra_columns_values:            extra_columns_values = {k.upper(): v for k, v in extra_columns_values.items()}
        table_columns_upper = [col.upper() for col in self.tables[table_name].columns]
        assert extra_columns_values is None or all( k in table_columns_upper for k in extra_columns_values.keys()), f"Provided extra column names '{extra_columns_values.keys()}' must exist in table '{table_name}'"

        if item_uid is None:
            new_item_uid = self.generate_uid()
        else:
            assert self.is_valid_pydcom_uid( item_uid ), f"Provided uid '{item_uid}' is not a valid dicom UID."
            new_item_uid = item_uid
        if extra_columns_values: # convert keys to uppercase, make sure all inputted keys were defined when the table was added as new.
            # Add the new row to the table, using the default columns and the extra columns
            new_data = pd.DataFrame( [ [item_name, new_item_uid, self.now_datetime, self.accessor_uid] + list( extra_columns_values.values() ) ], columns=self.tables[table_name].columns)
        else: # No inserted data for extra columns
            new_data = pd.DataFrame( [ [item_name, new_item_uid, self.now_datetime, self.accessor_uid] ], columns=self.tables[table_name].columns )

        self._tables[table_name] = pd.concat( [self.tables[table_name], new_data], ignore_index=True )
        self._update_metadata()
        if verbose:                     print( f'\tSUCCESS! --- Added "{item_name}" to table "{table_name}".\n' )


    def get_uid( self, table_name: str, item_name: str ) -> str:
        table_name, item_name = table_name.upper(), item_name.upper()
        assert self.item_exists( table_name, item_name ), f"Item '{item_name}' does not exist in table '{table_name}'"
        return str( self.tables[table_name].loc[self.tables[table_name]['NAME'] == item_name, 'UID'].values[0] )


    def get_name( self, table_name: str, item_uid: str ) -> str:
        table_name, item_uid = table_name.upper(), item_uid.upper()
        assert self.item_exists( table_name, item_uid ), f"Item '{item_uid}' does not exist in table '{table_name}'"
        return str( self.tables[table_name].loc[self.tables[table_name]['UID'] == item_uid, 'NAME'].values[0] )


    def get_table( self, table_name: str ) -> pd.DataFrame:
        table_name = table_name.upper()
        assert self.table_exists( table_name ), f"Table '{table_name}' does not exist."
        return self.tables[table_name]


    def __str__( self ) -> str:
        output = [f'\n-- MetaTables -- Accessed by: {self.accessor_username}']
        output.append( f'   *Last Modified: {self.metadata["LAST_MODIFIED"]}')
        table_info = pd.DataFrame(columns=['Table Name', '# Items', '# Columns'])
        for table_name, table_data in self.tables.items():
            new_row_df = pd.DataFrame([[table_name, len(table_data), len(table_data.columns)]], 
                                    columns=['Table Name', '# Items', '# Columns'])
            table_info = pd.concat([table_info, new_row_df], ignore_index=True)
        output.append( '\n'.join('\t' + line for line in table_info.to_string( index=False ).split('\n') ) )
        return '\n'.join( output )


#--------------------------------------------------------------------------------------------------------------------------
## Class for ensuring common formatting of date-time strings.
class USCentralDateTime():
    '''
    # Convert to us central standard time.
    # Example usage:
    tst1 = USCentralDateTime( '2022-01-01 11:00:00 PST' )
    print( tst1 )
    print( 'USCentral Date: ' + tst1.date + ', time: ' + tst1.time )
    print( USCentralDateTime( 'nonsense time o'clock' ) )
    '''
    def __init__( self, dt_str: Opt[str] = None ):
        if dt_str is None:    dt_str = '1900-01-01 00:00:00'
        self._date, self._time, self._dt = '', '', None
        self._raw_dt_str = dt_str
        self._parse_date_time()


    def _parse_date_time( self ):
        tzinfos = {'PST': -8 * 3600}
        dt = parser.parse( self._raw_dt_str, fuzzy=True, tzinfos=tzinfos )
        if dt.tzinfo is None or dt.tzinfo.utcoffset( dt ) is None:
            dt = dt.replace( tzinfo=pytz.timezone( 'US/Central' ) )
        self._dt = dt.astimezone( pytz.timezone( 'US/Central') )


    @property
    def date( self )    -> str:     return self.dt.date().strftime( '%Y%m%d' )
    @property
    def time( self )    -> str:     return self.dt.time().strftime( '%H%M%S.%f' )[:-3]
    @property
    def dt( self )      -> datetime:return self._dt # type: ignore
    @property
    def dt_str( self )  -> str:     return str( self.dt.strftime( '%Y-%m-%d %H:%M:%S.%f' ) ) + ' US-CST'


    def __str__( self ) -> str:     return f'{self.dt} US-CST'


#--------------------------------------------------------------------------------------------------------------------------
# Class for representing images as unique hashes.
class ImageHash( UIDandMetaInfo ):
    def __init__( self, reference_table: Opt[MetaTables]=None, img: Opt[np.ndarray] = None ):
        '''ImageHash()
        A class for creating a unique hash for an image. A list of seen-hashes will allow us to prevent duplicate images in the db.
            - Cataloging of the hashes is done elsewhere.

        Hashes are computed through the following subroutine:
        1.  Ensure/convert to grayscale
        2.  Convert to uint8 (normalize to 0-255 pixel values)
            - Currently supported types are signed- and unsigned-int8, 16, 32, and 64 bits.
                - Floats are not supported. Not sure how to handle them.
            - We want to convert images down to uint8 to account for possible outside-transformation of images.
                - Don't want ImageHash( np.int16( img ) ) != ImageHash( np.float32( img ) )
        3. Resize to 256x256.
            - Some of our images derived from the same performance can look to similar
                - Don't want to risk generating the same hash by downsampling too much.
        
        To-do: If need be, revisit the init to require only an image ffn so we can use cv2 ro imread it into a predictable way, i.e., rgb not bgr.

        # Example usage:
        tst1 = ImageHash( reference_table( XNatLogin( {...} ) ) ) # computes hash using the template dicom image stored in the UIDandMetaInfo attributes.
        tst2 = ImageHash( reference_table( XNatLogin( {...} ) ), np.uint32( tst1.raw_img ) )
        tst3 = ImageHash( reference_table( XNatLogin( {...} ) ), np.int16(  tst1.raw_img ) )
        print( tst1 )
        print( tst2 )
        print( tst3 )
        print( 'All hash strings the same:', tst1.hash_str == tst1.hash_str and tst1.hash_str == tst3.hash_str and tst2.hash_str == tst3.hash_str )
        '''
        super().__init__()  # Call the __init__ method of the base class
        self._validate_input( img )
        self._processed_img, self._gray_img, self._hash_str  = self.dummy_image(), self.dummy_image(), ''
        self._meta_tables, self._in_img_hash_metatable = reference_table, False
        self._convert_to_grayscale()
        self._normalize_and_convert_to_uint8()
        self._resize_image()
        self._compute_hash_str()
        if self.metatables is not None and isinstance( self.metatables, MetaTables ):
            self._check_img_hash_metatable()
    

    @property
    def raw_img( self )                 -> np.ndarray:                      return self._raw_img
    @property
    def gray_img_bit_depth( self )      -> int:
        assert self.gray_img is not None, f'Raw image must be defined before checking bit depth.'
        gray_img_dtype = self.gray_img.dtype
        if gray_img_dtype   in ( np.uint8, np.int8 ):
            return 8
        elif gray_img_dtype in ( np.uint16, np.int16 ):
            return 16
        elif gray_img_dtype in ( np.uint32, np.int32, np.float32 ):
            return 32
        elif gray_img_dtype in ( np.uint64, np.int64, np.float64 ):
            return 64
        else:
            raise ValueError( f'Unsupported/unexpected bit depth: {gray_img_dtype}' )
    @property
    def gray_img( self )                -> np.ndarray:                      return self._gray_img
    @property
    def processed_img( self )           -> np.ndarray:                      return self._processed_img
    @property
    def hash_str( self )                -> str:                             return self._hash_str
    @property
    def metatables( self )              -> Opt[Union[MetaTables, list]]:    return self._meta_tables
    @property
    def in_img_hash_metatable( self )   -> bool:                            return self._in_img_hash_metatable
    

    def _validate_input( self, img: Opt[np.ndarray] = None ):
        if img is None:
            self._raw_img = self.template_img
        else:
            self._raw_img = img.astype( np.uint64 ).copy()
        assert self.raw_img.dtype in self.acceptable_img_dtypes, f'Bitdepth "{self.raw_img.dtype}" is unsupported; inputted image must be one of: {self.acceptable_img_dtypes}.'
        assert 2 <= self.raw_img.ndim <= 3, f'Inputted image must be a 2D or 3D array.'


    def _convert_to_grayscale( self ):
        if len( self.raw_img.shape ) == 3:  # Ensure that the image is in grayscale
            self._gray_img = np.mean( self.raw_img, axis=2 )
            # self._processed_img = cv2.cvtColor( self.raw_img, cv2.COLOR_BGR2GRAY )
        else:
            self._gray_img = self.raw_img


    def _normalize_and_convert_to_uint8( self ): # Normalize the image to the range 0-255
        self._gray_img = cv2.normalize( self.gray_img, np.zeros( self.gray_img.shape, np.uint8 ), 0, 255, cv2.NORM_MINMAX ).astype( np.uint8 )


    def _resize_image( self ):
        self._processed_img = cv2.resize( self.gray_img, self.required_img_size_for_hashing )
    

    def _compute_hash_str( self ):
        self._hash_str = hashlib.sha256( self.processed_img.tobytes() ).hexdigest() # alternatively: imagehash.average_hash( Image.fromarray( image ) )
        assert self.hash_str is not None and len( self.hash_str ) == 64, f'Hash string must be 64 characters long.'
    

    def _check_img_hash_metatable( self ): # check if it exists in the metatables
        assert self.processed_img.shape == self.required_img_size_for_hashing, f'Processed image must be of size {self.required_img_size_for_hashing} (is currently size {self.processed_img.shape}).'
        if isinstance( self.metatables, MetaTables ):
            self._in_img_hash_metatable = self.metatables.item_exists( table_name='IMAGE_HASHES', item_name=self.hash_str )
        else:
            self._in_img_hash_metatable = False
    

    def __str__( self ) -> str:
        return f"-- ImageHash --\n\nShape:\t{self.processed_img.shape}\nDType:\t{self.processed_img.dtype}\t(min: {np.min(self.processed_img)}, max: {np.max(self.processed_img)})\nHash:\t{self.hash_str}\tIn metatables:\t{self.in_img_hash_metatable}"


    def plot( self ):
        fig, ax = plt.subplots()
        ax.imshow( self.processed_img, cmap='gray' )
        ax.set_title( self.hash_str) 
        ax.axis('off')
        plt.show()

    def dummy_image( self ) -> np.ndarray:
        return np.full( self.required_img_size_for_hashing, np.nan )

