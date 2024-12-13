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
import re
import warnings
import ast
from typing import List, Dict, Any
from collections.abc import Hashable
from tabulate import tabulate
import textwrap


# Define list for allowable imports from this module -- do not want to import _local_variables. As more classes are added you will need to update this list.
__all__ = ['UIDandMetaInfo', 'XNATLogin', 'ConfigTables', 'XNATConnection', 'USCentralDateTime', 'ImageHash', 'BatchUploadRepresentation']


data_librarian_hawk_id = 'dmattioli' # Update as needed. *** Not sure if changing this to stelong will cause errors.


#--------------------------------------------------------------------------------------------------------------------------
## Helper class for inserting all information that should only be used locally within the _local_variables class def below:
class _local_variables:
    """
    This class defines and stores local variables used in the XNAT-Interact project.
        - Almost all classes defined in this file and the 'xnat_..._data.py' files will inherit from this class so that we can easily access this information.

    Attributes:
        xnat_project_name (str): The name of the XNAT project.
        xnat_url (str): The URL of the XNAT server.
        xnat_config_folder_name (str): The name of the XNAT configuration folder.
        config_fn (str): The name of the database configuration file.
        template_img_dir (str): The directory path of the template image.
        tmp_data_dir (str): The directory path for temporary data storage.
        redacted_string (str): A redacted string used for Python-to-XNAT upload script.
        config_ffn (str): The full file path of the database configuration file.
        required_login_keys (list): A list of required login keys.
        xnat_project_url (str): The URL of the XNAT project.
        default_meta_table_columns (list): A list of default meta table columns.
        template_img (numpy.ndarray): The template image read from the template image directory.
        acceptable_img_dtypes (list): A list of acceptable image data types.
        required_img_size_for_hashing (tuple): The required image size for hashing.
        mturk_batch_col_names (list): A list of MTurk batch column names.
        required_batch_upload_columns (dict): A dictionary of required columns mass upload processing.
    """
    def __init__( self ):
        """
        Initialize the _local_variables instance.
        """
        self._img_sizes = ( 256, 256 )
        self.__dict__.update( self._set_local_variables() )

        # ensure that the temp folder exists on the local machine. Note sure that this will work for macs/unix
        if not os.path.exists( self.tmp_data_dir ):     os.makedirs( self.tmp_data_dir )


    def _read_template_image( self, template_ffn: str ) -> np.ndarray:
        """
        Read the template image from the given file path. The image should be saved in the project files (pulled from github repository).
        """
        return cv2.resize( cv2.imread( template_ffn, cv2.IMREAD_GRAYSCALE ), self._img_sizes ).astype( np.uint8 )


    def __getattr__( self, attr ):
        if attr in self.__dict__:       return self.__dict__[attr]
        else:                           raise AttributeError( f"Attribute {attr} is not yet defined." )
    

    def __str__( self ) -> str:         return '\n'.join([f'{k}:\t{v}' for k, v in self.__dict__.items()])


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
                        'required_batch_upload_columns' : {  "Filer HawkID": "Text; Required; must be registered with this library",
                                                            "Operation Date": "Date; Required; EPIC Operation Date",
                                                            "Quality": "Text; Optional; [Unknown, Questionable, Unusuable, Usable]",
                                                            "Institution Name": "Text; Required; Source of the data, must be registered with this library",
                                                            "Procedure Name": "Text; Required; must be registered  with this library",
                                                            "Epic Start Time": "Time; Required; Official time on EPIC for the start time of the operation",
                                                            "Epic End Time": "Time; Optional",
                                                            "Side of Patient Body": "Text; Optional; [Unknown, Left, Right]",
                                                            "OR Room Name/ Location": "Text; Optional", # Space after the slash is intentional -- for readability in the excel file.
                                                            "Supervising Surgeon HawkID": "Text; Optional",
                                                            "Supervising Surgeon Presence": "Text; Conditional; when HawkID is provided",
                                                            "Performing Surgeon HawkID": "Text; Required",
                                                            "Performing Surgeon # Years Experience": "Int; Optional; e.g., year in residency",
                                                            "Performing Surgeon # Prior Cases": "Int; Optional; number of prior similar cases logged",
                                                            "# of Participating Performing Surgeons": "Int; Optional",
                                                            "Performer HawkID-Task": "CText; onditional; if # of participating surgeons is > 1",
                                                            "Unusual Features": "Text; Optional; Noted by observer",
                                                            "Diagnostic Notes": "Text; Optional; Noted by observer",
                                                            "Additional Comments": "Text; Optional; Noted by observer",
                                                            "Skills Assessment Requested": "Text; Required; [Y, N, Unknown]",
                                                            "Assessor HawkID": "Text; Required only if assessment requested",
                                                            "Additional Assessment Details": "Text; Conditional; only if assessment requested",
                                                            "Name/ Type of Storage Device": "Text; Optional; eg., 'USB-A'", # Space after the slash is intentional -- for readability in the excel file.
                                                            "Full Path to Data": "Text; Required; local path of data used when uploading",
                                                            "Was Radiology Contacted": "OText; ptional; [Y, N, Unknown]",
                                                            "Radiology Contact Date": "Date; Optional",
                                                            "Radiology Contact Time": "Time; Optional"
                                                        },
                        'redacted_string': redacted_string
                        }
        # local_vars.template_img_hash = ImageHash( local_vars.template_img_dir ).hashed_img
        return local_vars


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all utitlities to inherit from.
class UIDandMetaInfo:
    """
    A utility class for managing UID generation and metadata information.

    Attributes:
        uid (str): Unique identifier for the instance.
    
    Methods:
        generate_uid(): Generate a unique identifier.
        is_valid_pydcom_uid(): Validate the format of a UID.
    """
    def __init__( self ):
        """
        Initialize the UIDandMetaInfo instance and generate a unique identifier.
        """
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
    def required_batch_upload_columns( self )   -> dict:                return self.local_variables.required_batch_upload_columns
    @property
    def now_datetime( self )                    -> str:                 return datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()
    

    def convert_all_kwarg_strings_to_uppercase( **kwargs ):
        return {k: v.upper() if isinstance(v, str) else v for k, v in kwargs.items()}


    def generate_uid( self )                    -> str:
        """
        Generate a unique identifier for the instance.

        Returns:
            str: A newly generated unique identifier.
        """
        return str( generate_pydicomUID( prefix=None, entropy_srcs=[self.now_datetime] ) ).replace( '.', '_' )
    
    
    def is_valid_pydcom_uid( self, uid_str: str )   -> bool:
        """
        Validate the format of the given UID.

        Args:
            uid (str): The UID to validate.

        Returns:
            bool: True if the UID is valid, i.e., no periods allowed in xnat entity names, False otherwise.
        """
        return pydicom_UID( uid_str.replace( '_', '.' ) ).is_valid
                                                

#--------------------------------------------------------------------------------------------------------------------------
## Class for validating xnat login information
class XNATLogin( UIDandMetaInfo ):
    '''XNATLogin( input_info = {'URL': '...', 'USERNAME': '...', 'PASSWORD': '...'} )
        A class for storing data that is necessary for logging into the XNAT RPACS server.
        For provided login dictionary, this class ensures that:
            1.  Provided server url is correct,
            2.  Provided keys are all correct.
                - User-provided key strings are auto-capitalized, but the corresponding value strings are not*, except for the user-provided url.
                - If your attempts to instantiate keep failing, try input all keys with all-caps characters.

        Note: This class does not verify the login with the xnat server, it only validates that it is passable login-information according to the above criteria.
            -   For verifying login information, refer to XNATConnection, another class definition.

        # Example Usage:
        test1 = XNATLogin( {'URL': 'https://rpacs.iibi.uiowa.edu/xnat/', 'USERNAME': 'iamacomputer', 'PASSWORD': 'hello_world'} )
        print( test1 )
        test2 = XNATLogin( {'URL': 'www.google.com', 'USERNAME': 'dmattioli', 'PASSWORD': 'hello_world'} )
        print( test2 )
    '''

    def __init__( self, input_info: dict, verbose: Opt[bool] = True ):
        super().__init__()  # Call the __init__ method of the base class
        self._validated_username, self._validated_password = '', '', 
        self._is_valid, self._user_role = False, '' # user_role will be implemented in the future -- pull it from the xnat server if possible (e.g. member, owner, collaborator).
        self._validate_login( input_info )
        if verbose:                             print( self )

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
        if self.is_valid:                       return f"-- Validated XNATLogin --\n\tUsername:\t{self.validated_username}\n\tServer:\t\t{self.xnat_project_url}\n"
        else:                                   return f"-- Invalid XNATLogin --\n\tUsername:\t{self.validated_username}\n\tServer:\t\t{self.xnat_project_url}\n"

    # def doc( self ) -> str: return self.__doc__


#--------------------------------------------------------------------------------------------------------------------------
## Class for establishing a connection to the xnat server with specific credentials.
class XNATConnection( UIDandMetaInfo ):
    """A class representing a connection to an XNAT server.
    Inputs:
    - login_info: An instance of XNATLogin class containing the login information for the XNAT server.
    - stay_connected: A boolean indicating whether to stay connected to the server after verifying the login. Default is False.
    - verbose: A boolean indicating whether to print connection details. Default is False.

    Attributes:
    - login_info: An instance of XNATLogin class containing the login information for the XNAT server.
    - server: An Interface object representing the XNAT server connection.
    - project_query_str: A string representing the query string for the XNAT project.
    - project_handle: A pyxnatProject object representing the XNAT project handle.
    - is_verified: A boolean indicating whether the login has been verified.
    - is_open: A boolean indicating whether the connection is open.
    - get_user: A string representing the validated username.
    - get_password: A string representing the validated password.
    
    ***Troubleshooting/problems encountered:
    1. Each time I changed my password on myUI, remotely accessing XNAT with the new password will not work until you login to the XNAT server through the web interface with your new login info!

    # Example usage:
    my_login_info = {"URL": "https://rpacs.iibi.uiowa.edu/xnat/", "USERNAME": "...", "PASSWORD": "..."}
    my_login = XNATLogin( login_info )
    my_connection = XNATConnection( my_login, my ) 
    print( my_connection )
    # my_connection = XNATConnection( my_login, my, stay_connected=True ) 
    # print( my_connection )
    """
    _instance = None

    def __new__( cls, *args, **kwargs ): # Only one instance of this class should be allowed to exist at a time.
        if cls._instance is not None:   cls._instance.__del__()  # Explicitly call __del__ on the existing instance
        cls._instance = super( XNATConnection, cls ).__new__( cls )
        return cls._instance

    def __init__( self, login_info: XNATLogin, stay_connected: bool = False, verbose: Opt[bool] = True ):
        assert login_info.is_valid, f"Provided login info must be validated before accessing xnat server: {login_info}"
        super().__init__()  # Call the __init__ method of the base clas
        self._login_info, self._project_handle, self._is_verified, self._is_open, self._failed_tests = login_info, None, False, stay_connected, {}
        self._verify_login()
        if self.is_verified and not stay_connected: # Disconnect from the project instance connection if we successfully connected.
            self.server.disconnect()
            self._open = False
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
    @property
    def failed_tests( self )        -> dict:            return self._failed_tests


    def _verify_login( self ):
        '''Explanation of tests:
            1.  Project Handle is None: If the project handle is None, the connection is invalid. Check project url, username, password, and registered users.
            2.  Project Handle label does not match xnat_project_name: If the project handle label does not match the xnat project name, the connection is invalid.
            3.  User is None: If the user is None, the connection is invalid.
            4.  User is not added to project (XNAT-side): If the user is not added to the project on the XNAT side, the connection is invalid.
        '''
        self._server = self._establish_connection()
        self._grab_project_handle() # If more tests in the future, separate as its own function.
        self._is_verified = False
        if self.project_handle is None:
            self._failed_tests['Project Handle is None'] = True
            return
        else:
            self._failed_tests['Project Handle is None'] = False
        if self.project_handle and self.project_handle.label() != self.xnat_project_name:
            self._failed_tests['Project Handle label does not match xnat_project_name'] = True
            return
        else:
            self._failed_tests['Project Handle label does not match xnat_project_name'] = False
        if self.get_user is None:
            self._failed_tests['User is None'] = True
            return
        else:
            self._failed_tests['User is None'] = False
        username = self.get_user.lower()                # type: ignore 
        self._failed_tests['User is not added to project (XNAT-side)'] = False
        if username not in [u.lower() for u in self.project_handle.users()]:
            if username != self.data_librarian.lower(): # this is a quirk specific to domattioli (i'm the only one who's username is not his hawkid (idk how this happened). Might be a problem for others in the future is they somehow do what i did).
                self._failed_tests['User is not added to project (XNAT-side)'] = True
                return
            
        # if all tests pass, set is_verified to True
        self._is_verified = True


    def _establish_connection( self ) -> Interface:     return Interface( server=self.xnat_project_url, user=self.get_user, password=self.get_password )


    def _grab_project_handle( self ):
        self._project_query_str = '/project/' + self.xnat_project_name
        project_handle = self.server.select( self.project_query_str )
        if project_handle.exists():                 self._project_handle = project_handle       # type: ignore


    def close( self ):
        if hasattr( self, '_server' ):  # Delete the local copy of the ConfigTables.
            self._server.disconnect()
            if os.path.exists( self.config_ffn ):       os.remove( self.config_ffn )
        self._open = False
        print( f"\n\t*Prior connection to XNAT server, '{self.uid}', has been closed -- local config data will be deleted!\n" )


    def __del__( self ):
        self.close() # Close the server connection ***AND*** delete the config local data to enforce user to always pull it from the server first.
        if os.path.exists( self.config_ffn ):           os.remove( self.config_ffn )
        XNATConnection._instance = None

    def __enter__( self ):                              return self

    def __exit__( self, exc_type, exc_value, traceback ):   self.close()

    def __str__( self ) -> str:
        project_users = self.project_handle.users() if self.project_handle else 'None'
        data_librarian = self.data_librarian.lower()
        if self.is_verified:    return (f"-- XNAT Connection --\n\tStatus:\t\t{'Open' if self.is_open else 'Closed'}\n\tUsername:\t{self.get_user}\n\tVerified:\t{self.is_verified}\n\tProject:\t{self.project_handle}\n\tLibrarian:\t{data_librarian}\n\tApproved Users:\t{project_users}" )
        else:                   return (f"-- XNAT Connection --\n\tStatus:\t\t{'Open' if self.is_open else 'Closed'}\n\tUsername:\t{self.get_user}\n\tVerified:\t{self.is_verified}\n\tFailed Tests:\t{self.failed_tests}\n\tProject:\t{self.project_handle}\n\tLibrarian:\t{data_librarian}\n\tApproved Users:\t{project_users}" )
          

#--------------------------------------------------------------------------------------------------------------------------
## Class for cataloging all seen data and user info.
class ConfigTables( UIDandMetaInfo ):
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
    mt = ConfigTables( my_login, my_connection )
    print( mt )

    # Create some new tables -- convention is for inputs to be plural. Will automatically caps everything
    mt.add_new_table( table_name='hello' )

    # Add new items to the tables - convention is for inputs to be plural
    mt.add_new_item( table_name='hello', item_name='world' )
    # mt.save()
    '''
    def __init__( self, login_info: XNATLogin, xnat_connection: XNATConnection, verbose: Opt[bool] = True ):
        assert login_info.is_valid, f"Cannot access CongifTables without valid login_info. You provided the following login information:\n{login_info}"
        assert xnat_connection.is_open, f"Cannot access ConfigTables without an open connection to the XNAT server. Your xnat_connection data is:\n{xnat_connection}"
        assert xnat_connection.is_verified, f"Cannot access ConfigTables without a verified connection to the XNAT server. Your xnat_connection data is:\n{xnat_connection}"
        
        super().__init__()  # Call the __init__ method of the base class to ensure that we inherit all those local variables
        self._login_info, self._xnat_connection = login_info, xnat_connection
        
        try: # Need to try to pull it from the xnat server if it exists, otherwise create it from scratch.
            self.pull_from_xnat( verbose=verbose )
        except: # This should only ever happen one time -- when the XNAT database is first created.
            self._instantiate_json_file()
            self._initialize_tables()
            self.push_to_xnat( verbose=verbose )
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
    def accessor_username( self )   -> str:             return self.login_info.validated_username
    @property
    def accessor_uid( self )        -> str:             return self.get_uid( 'REGISTERED_USERS', self.accessor_username )


    #==========================================================PRIVATE METHODS==========================================================
    def _reinitialize_tables_with_extra_columns( self ) -> None:
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
            

    def _initialize_tables( self ) -> None:
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
        self.add_new_item( table_name='Surgeons', item_name='rhguzek',      extra_columns_values={'first_name': 'RYAN', 'last_name': 'GUZEK', 'middle_initial': 'H'} )
        self.add_new_item( table_name='Surgeons', item_name='kgeigr',       extra_columns_values={'first_name': 'KYLE', 'last_name': 'GEIGER', 'middle_initial': 'W'} )
        self.add_new_item( table_name='Surgeons', item_name='willey',       extra_columns_values={'first_name': 'MICHAEL', 'last_name': 'WILLEY', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='eorojas',      extra_columns_values={'first_name': 'EDWARD', 'last_name': 'ROJAS', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='ooreilly',     extra_columns_values={'first_name': 'OLIVIA', 'last_name': 'OREILLY', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='steleary',     extra_columns_values={'first_name': 'STEVEN', 'last_name': 'LEARY', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='bvarone',      extra_columns_values={'first_name': 'BUTTURI-VARONE', 'last_name': 'BRUNO', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='jrhall',       extra_columns_values={'first_name': 'HALL', 'last_name': 'JAMES', 'middle_initial': 'R'} )
        self.add_new_item( table_name='Surgeons', item_name='millerby',     extra_columns_values={'first_name': 'MILLER', 'last_name': 'BENJAMIN', 'middle_initial': 'J'} )
        self.add_new_item( table_name='Surgeons', item_name='mhhogue',      extra_columns_values={'first_name': 'HOGUE', 'last_name': 'MATTHEW', 'middle_initial': 'H'} )
        self.add_new_item( table_name='Surgeons', item_name='rsroundy',     extra_columns_values={'first_name': 'ROUNDY', 'last_name': 'ROBERT', 'middle_initial': 'S'} )
        self.add_new_item( table_name='Surgeons', item_name='austbenson',   extra_columns_values={'first_name': 'BENSON', 'last_name': 'AUSTIN', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='btwds',        extra_columns_values={'first_name': 'WOODS', 'last_name': 'BISON', 'middle_initial': 'T'} )
        self.add_new_item( table_name='Surgeons', item_name='mmclrath',     extra_columns_values={'first_name': 'MCLLRATH', 'last_name': 'MATTHEW', 'middle_initial': 'D'} )
        self.add_new_item( table_name='Surgeons', item_name='dmeek',        extra_columns_values={'first_name': 'MEEKER', 'last_name': 'DANIEL', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='mskalitzky',   extra_columns_values={'first_name': 'SKALITZKY', 'last_name': 'MARY', 'middle_initial': 'K'} )
        self.add_new_item( table_name='Surgeons', item_name='rund',         extra_columns_values={'first_name': 'RUND', 'last_name': 'JOSEPH', 'middle_initial': 'M'} )
        self.add_new_item( table_name='Surgeons', item_name='morness',      extra_columns_values={'first_name': 'ORNESS', 'last_name': 'MICHAEL', 'middle_initial': 'E'} )
        self.add_new_item( table_name='Surgeons', item_name='bjmarshall',   extra_columns_values={'first_name': 'MARSHALL', 'last_name': 'BRANDON', 'middle_initial': 'J'} )
        self.add_new_item( table_name='Surgeons', item_name='gvchristensen',extra_columns_values={'first_name': 'CHRISTENSEN', 'last_name': 'GARRET', 'middle_initial': 'V'} )
        self.add_new_item( table_name='Surgeons', item_name='ademers',      extra_columns_values={'first_name': 'DEMERS', 'last_name': 'ALEX', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='adalamaggas',  extra_columns_values={'first_name': 'DALAMAGGAS', 'last_name': 'ARIANNA', 'middle_initial': 'M'} )
        self.add_new_item( table_name='Surgeons', item_name='jwheelwright', extra_columns_values={'first_name': 'WHEELWRIGHT', 'last_name': 'JOHN', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='mmarinier',    extra_columns_values={'first_name': 'MARINIER', 'last_name': 'MICHAEL', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='ceberlin',     extra_columns_values={'first_name': 'EBERLIN', 'last_name': 'CHRISTOPHER', 'middle_initial': 'T'} )
        self.add_new_item( table_name='Surgeons', item_name='eschoi',       extra_columns_values={'first_name': 'CHOI', 'last_name': 'ERIN', 'middle_initial': 'S'} )
        self.add_new_item( table_name='Surgeons', item_name='mwishman',     extra_columns_values={'first_name': 'WISHMAN', 'last_name': 'MARK', 'middle_initial': 'D'} )
        self.add_new_item( table_name='Surgeons', item_name='lanordstrom',  extra_columns_values={'first_name': 'NORDSTROM', 'last_name': 'LUKE', 'middle_initial': 'A'} )
        self.add_new_item( table_name='Surgeons', item_name='lmalecha',     extra_columns_values={'first_name': 'MALECHA', 'last_name': 'LINDSEY', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='wdodd',        extra_columns_values={'first_name': 'DODD', 'last_name': 'WILLIAM', 'middle_initial': 'S'} )
        self.add_new_item( table_name='Surgeons', item_name='jcdavison',    extra_columns_values={'first_name': 'DAVISON', 'last_name': 'JOHN', 'middle_initial': 'C'} )
        self.add_new_item( table_name='Surgeons', item_name='cjcall',       extra_columns_values={'first_name': 'CALL', 'last_name': 'CORY', 'middle_initial': 'J'} )
        self.add_new_item( table_name='Surgeons', item_name='unknown',      extra_columns_values={'first_name': 'UNKNOWN', 'last_name': 'UNKNOWN', 'middle_initial': ''} )
        self.add_new_item( table_name='Surgeons', item_name='not-applicable', extra_columns_values={'first_name': 'NOT', 'last_name': 'APPLICABLE', 'middle_initial': 'NA'} )

        self.add_new_item( 'registered_users', 'gthomas' )
        self.add_new_item( 'registered_users', 'andersondd' )
        self.add_new_item( 'registered_users', 'mtatum' )
        self.add_new_item( 'registered_users', 'stelong' )
        self.add_new_item( 'registered_users', 'jhill7' )
        self.add_new_item( 'registered_users', 'ezwilliams' )


    def _load( self, local_meta_tables_ffn: Opt[Path], verbose: Opt[bool] = True ) -> None:
        assert self.login_info.is_valid, f"Provided login info must be validated before loading ConfigTables: {self.login_info}"
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
        if assert_librarian:                assert self.accessor_username.upper() == self.data_librarian.upper(), f'Only user {self.data_librarian} can push config file to the xnat server.'
    
    
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
    def pull_from_xnat( self, write_ffn: Opt[Path]=None, verbose: Opt[bool] = True ) -> Opt[Path]:
        if write_ffn is None:   write_ffn = self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).get_copy( self.config_ffn )
        else:
            assert isinstance( write_ffn, Path ), f"Provided write file path must be a valid Path object: {write_ffn}"
            assert write_ffn.suffix == '.json', f"Provided write file path must have a '.json' extension: {write_ffn}"
            write_ffn = self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).get_copy( write_ffn )
        self._load( write_ffn, verbose )
        if verbose:                     print( f'\t...ConfigTables successfully populated from XNAT data.\n' )
        
        self._reinitialize_tables_with_extra_columns()
        return write_ffn


    def ensure_primary_keys_validity( self ) -> None:
        '''Ensure that Subjects found in Image Hashes table are also found in the Subjects table.
        TBD...
        '''
        # # Test 1: Ensure that all subjects in the Image Hashes table are also in the Subjects table
        # unique_referenced_subject_uids = self._tables['IMAGE_HASHES']['SUBJECT'].unique()
        # unique_subjects = self.list_of_all_items_in_table( 'SUBJECTS' )
        # assert sorted( unique_referenced_subject_uids ) == sorted(unique_subjects ), 'The unique subjects in the IMAGE_HASHES table do not match the unique subjects in the SUBJECTS table'
        # # Test 2: Ensure ...
        pass


    def push_to_xnat( self, verbose: Opt[bool] = True ) -> bool:
        try:
            self.ensure_primary_keys_validity()
            if self.save( verbose ) is False:   return False
            self.xnat_connection.server.select.project( self.xnat_connection.xnat_project_name ).resource( self.xnat_config_folder_name ).file( self.config_fn ).put( self.config_ffn, content='META_DATA', format='JSON', tags='DOC', overwrite=True )
            if verbose:                     print( f'\t...ConfigTables (config.json) successfully updated on XNAT!\n' )
            return True
        except Exception as e:
            print( f'\tERROR! --- Failed to push ConfigTables to XNAT server. Error message: {e}\n' )
            return False


    def save( self, verbose: Opt[bool] = True ) -> bool: # Convert all tables to JSON; Write the data to the file
        '''Only saves locally. To save to the server, all the 'catalog_new_data' method(s) in the experiment class(es) must be called.'''
        try:
            self._validate_login_for_important_functions( assert_librarian=False ) # To-do: is this necessary if the user musts create a valid xnat connection first (which should check the same thing)?
            tables_json = {name: df.to_dict( 'records' ) for name, df in self.tables.items()}
            data = {'metadata': self.metadata, 'tables': tables_json }
            # json_str = json.dumps( data, indent=2, separators=( ',', ':' ) )
            json_str = self._custom_json_serializer( data )
            with open( self.config_ffn, 'w' ) as f:         f.write( json_str )
            if verbose:                         print( f'\tSUCCESS! --- saved ConfigTables to: {self.config_ffn}\n' )
            return True
        except Exception as e:
            print( f'\tERROR! --- Failed to save ConfigTables to: {self.config_ffn}. Error message: {e}\n' )
            return False


    def is_user_registered( self, user_name: Opt[str] = None ) -> bool:
        """
        Check if a user is registered in the system.

        Args:
            username (str): The username to check.

        Returns:
            bool: True if the user is registered, False otherwise.
        """
        '''Note that this will automatically capitalize the inputted user_name.'''
        if user_name is None:   user_name = self.accessor_username
        return user_name.upper() in self.tables['REGISTERED_USERS']['NAME'].values


    def register_new_user( self, user_name: str, verbose: Opt[bool] = True ) -> bool:
        try:
            self._validate_login_for_important_functions( assert_librarian=True )   
            if not self.is_user_registered( user_name ):
                self.add_new_item( 'REGISTERED_USERS', user_name )
            if verbose:                     print( f'\tSUCCESS! --- Registered new user: {user_name}\n' )
            return True
        except Exception as e:
            print( f'\tERROR! --- Failed to register new user: {user_name}. Error message: {e}\n' )
            return False


    def list_of_all_tables( self ) -> list:                             return list( self.tables.keys() )


    def list_of_all_items_in_table( self, table_name: str ) -> list:
        """
        List all items in the specified metadata table.

        Args:
            table_name (str): The name of the table.

        Returns:
            List[str]: A list of all items in the specified table.
        """
        if table_name.upper() in self.tables and not self.tables[table_name.upper()].empty:
            return list (self.tables[table_name.upper()]['NAME'] )
        else:   return [] # Return an empty list if the table does not exist or is empty
            

    def table_exists( self, table_name: str ) -> bool:                  return table_name.upper() in self.list_of_all_tables()


    def item_exists( self, table_name: str, item_name: str ) -> bool:   return not self.tables[table_name.upper()].empty and item_name.upper() in self.tables[table_name.upper()].values


    def add_new_table( self, table_name: str, extra_column_names: Opt[typehintList[str]] = None, verbose: Opt[bool] = True ) -> None:
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


    def add_new_item( self, table_name: str, item_name: str, item_uid: Opt[str] = None, extra_columns_values: Opt[typehintDict[str, str]] = None, verbose: Opt[bool] = True ) -> Tuple[bool, str]:
        table_name, item_name = table_name.upper(), item_name.upper()
        assert self.is_user_registered(), f"User '{self.accessor_username}' must first be registed before adding new items."
        assert self.table_exists( table_name ), f"Cannot add item '{item_name}' to table '{table_name}' because that table does not yet exist.\n\tTry creating the new table before adding '{item_name}' as a new item."
        if self.item_exists( table_name, item_name ):
            success, out_str = False, f'\tWARNING! --- Cannot add item "{item_name}" because it already exists in Table "{table_name}".'
        else:   # Ensure all provided extra column names exist in the table, considering case-insensitivity
            if extra_columns_values:    extra_columns_values = {k.upper(): v for k, v in extra_columns_values.items()}
            table_columns_upper = [col.upper() for col in self.tables[table_name].columns]
            assert extra_columns_values is None or all( k in table_columns_upper for k in extra_columns_values.keys()), f"Provided extra column names '{extra_columns_values.keys()}' must exist in table '{table_name}'"

            # Create a uid for the item if one was not provided already.
            if item_uid is None:        new_item_uid = self.generate_uid()
            else:
                assert self.is_valid_pydcom_uid( item_uid ), f"Provided uid '{item_uid}' is not a valid dicom UID."
                new_item_uid = item_uid

            # Add the new row to the table, using the default columns and the extra columns if provided
            if extra_columns_values:    new_data = pd.DataFrame( [ [item_name, new_item_uid, self.now_datetime, self.accessor_uid] + list( extra_columns_values.values() ) ], columns=self.tables[table_name].columns)
            else:                       new_data = pd.DataFrame( [ [item_name, new_item_uid, self.now_datetime, self.accessor_uid] ], columns=self.tables[table_name].columns )
            
            self._tables[table_name] = pd.concat( [self.tables[table_name], new_data], ignore_index=True )
            self._update_metadata()
            success, out_str = True, f'\tSUCCESS! --- Added "{item_name}" to table "{table_name}".'
        if verbose:                     print( out_str )
        return success, out_str
    

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
        output = [f'\n-- ConfigTables --\n\tAccessed by:\t{self.accessor_username}']
        output.append( f'\t*Last Modified:\t{self.metadata["LAST_MODIFIED"]}')
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
    def __init__( self, reference_table: Opt[ConfigTables]=None, img: Opt[np.ndarray] = None ):
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
        self._ConfigTables, self._in_img_hash_metatable = reference_table, False
        self._convert_to_grayscale()
        self._normalize_and_convert_to_uint8()
        self._resize_image()
        self._compute_hash_str()
        if self.ConfigTables is not None and isinstance( self.ConfigTables, ConfigTables ):
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
    def ConfigTables( self )            -> Opt[Union[ConfigTables, list]]:  return self._ConfigTables
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
    

    def _check_img_hash_metatable( self ): # check if it exists in the config data
        assert self.processed_img.shape == self.required_img_size_for_hashing, f'Processed image must be of size {self.required_img_size_for_hashing} (is currently size {self.processed_img.shape}).'
        if isinstance( self.ConfigTables, ConfigTables ):   self._in_img_hash_metatable = self.ConfigTables.item_exists( table_name='IMAGE_HASHES', item_name=self.hash_str )
        else:                                               self._in_img_hash_metatable = False
    

    def __str__( self ) -> str:
        return f"-- ImageHash --\n\nShape:\t{self.processed_img.shape}\nDType:\t{self.processed_img.dtype}\t(min: {np.min(self.processed_img)}, max: {np.max(self.processed_img)})\nHash:\t{self.hash_str}\tIn ConfigTables:\t{self.in_img_hash_metatable}"


    def plot( self ):
        fig, ax = plt.subplots()
        ax.imshow( self.processed_img, cmap='gray' )
        ax.set_title( self.hash_str) 
        ax.axis('off')
        plt.show()

    def dummy_image( self ) -> np.ndarray:
        return np.full( self.required_img_size_for_hashing, np.nan )


#--------------------------------------------------------------------------------------------------------------------------
# Class for representing an imported batch upload spreadsheet.
class BatchUploadRepresentation( UIDandMetaInfo ):
    def __init__( self, xls_ffn: Path, config: ConfigTables, verbose: bool = True ):
        assert isinstance( xls_ffn, Path ), f"Inputted 'xls_ffn' must be a {type( Path() )} object; you provided a '{type( xls_ffn )}' object."
        assert xls_ffn.exists(), f"Inputted 'xls_ffn' path does not exist; you entered:\n\t'{xls_ffn}'"
        super().__init__()  # Call the __init__ method of the base class
        self._ffn, self._config = xls_ffn, config
        self._import_data_and_process_columns()
        self._process_mass_upload_data()
        if verbose:                     self.print_rows( rows='all' )
            
    @property
    def ffn( self )             -> Path:                    return self._ffn
    @property
    def config( self )          -> ConfigTables:            return self._config
    @property
    def df( self )              -> pd.DataFrame:            return self._df
    @property
    def warnings( self )        -> pd.DataFrame:            return self._warnings
    @property
    def errors( self )          -> pd.DataFrame:            return self._errors
    @property
    def summary_table( self )   -> pd.DataFrame:            return self._summary_table
    
    def _import_data_and_process_columns( self ) -> None:   # Load data from excel file and ignore the warning resulting from enforced dropdowns within the excel template that i made
        with open( self.ffn, 'rb' ) as f:
            warnings.simplefilter( action='ignore', category=UserWarning )
            self._df        = pd.read_excel( f, header=0 )
            self._df        = self._df.fillna( '' )
            self._df.columns= [col.replace('\n', ' ').strip() for col in self._df.columns]
            self._df        = self.df.loc[:, ~self.df.columns.str.contains('^Case Name [Optional]')] # This column is only there for the user's reference.
            self._df        = self.df.loc[:, ~self.df.columns.str.contains('^Unnamed')] # Remove any unnamed columns
            text_columns    = [k for k, v in self.required_batch_upload_columns.items() if 'text' in v.lower()]
            for col in text_columns: # Lowercase everything for now for more reliable substring searches (will re-caps first letter of sentences in comments at time of upload).
                self.df[col] = self.df[col].astype(str).str.lower().str.strip()
            self._warnings      = pd.DataFrame( data=np.empty( ( len( self.df ), len( self.df.columns ) ), dtype=list ), columns=self.df.columns )   
            self._errors        = self.warnings.copy()
            self._summary_table = self.warnings.copy()
            
        required_columns = [k for k, v in self.required_batch_upload_columns.items() if 'required' in v.lower()]
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        assert not missing_columns, f"The following required columns are missing from the imported file: {', '.join( missing_columns )}"

    def _process_mass_upload_data( self ) -> None:          # Iterate through each row, performing custom checks on all the columns
        all_tag_info = self.retrieve_server_config_tags()
        surgeon_hawkids = all_tag_info['surgeon_hawk_id_encodings']
        for idx, row in self.df.iterrows():
            self._check_required_columns( idx=idx, row=row, tag_info=all_tag_info )
            self._check_optional_columns( idx=idx, row=row, surgeon_hawkids=surgeon_hawkids )
            self._check_conditional_columns( idx=idx, row=row, surgeon_hawkids=surgeon_hawkids ) # conditional on either a required or optional column's value.
        
        # Assign 'E' then 'W' to cells in the summary table based on the presence of errors or warnings in the respective columns.
        for col in self.summary_table.columns:
            # If there are both errors and warnings for a given cell, assign 'EW' to the cell.
            self.summary_table[col] = self.summary_table.apply(
                lambda x: 'EW' if not self._col_is_empty( x[col] ) and not self._col_is_empty( self.errors.at[x.name, col] )
                    else 'E' if not self._col_is_empty( self.errors.at[x.name, col] )
                    else 'W' if not self._col_is_empty( self.warnings.at[x.name, col] )
                    else '', axis=1 )
    
    def _log_issue( self, idx, column, message, issue_type='error' ):
        if issue_type == 'error':
            if not isinstance(self._errors.at[idx, column], list):
                self._errors.at[idx, column] = [message]
            else:
                self._errors.at[idx, column].append(message)
        elif issue_type == 'warning':
            if not isinstance(self._warnings.at[idx, column], list):
                self._warnings.at[idx, column] = [message]
            else:
                self._warnings.at[idx, column].append(message)

    def _check_required_columns( self, idx: Hashable, row: pd.Series, tag_info: dict ) -> None:
        if self._col_is_empty( row['Filer HawkID'] ) or row['Filer HawkID'] not in tag_info['hawk_ids']:
            self._log_issue( idx=idx, column='Filer HawkID', message=f"'Filer HawkID' ('{row['Filer HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if not self._col_is_empty( row['Operation Date'] ):
            date_str = str(row['Operation Date']).split()[0]
            if datetime.strptime( date_str, '%Y-%m-%d' ) < datetime( 2000, 1, 1 ):
                self._log_issue( idx=idx, column='Operation Date', message=f"'Operation Date' ('{row['Operation Date']}') is before January 1, 2000; make sure this is intentional.", issue_type='warning' )
        if self._col_is_empty( row['Epic Start Time'] ):
            self._log_issue( idx=idx, column='Epic Start Time', message="'Epic Start Time' cannot be blank or empty.", issue_type='error' )
        if self._col_is_empty( row['Institution Name'] ) or row['Institution Name'] not in tag_info['institutions']:
            self._log_issue( idx=idx, column='Institution Name', message=f"'Institution Name' ('{row['Institution Name']}') not registered in the 'Acquisition_Sites' config table.", issue_type='error' )
        if self._col_is_empty( row['Procedure Name'] ) or row['Procedure Name'] not in tag_info['procedure_names']:
            self._log_issue( idx=idx, column='Procedure Name', message=f"'Procedure Name' ('{row['Procedure Name']}') not registered in the 'Groups' config table.", issue_type='error' )
        if self._col_is_empty( row['Full Path to Data'] ):
            self._log_issue( idx=idx, column='Full Path to Data', message="'Full Path to Data' cannot be blank.", issue_type='error' )
        elif not Path( row['Full Path to Data'] ).exists():
            self._log_issue( idx=idx, column='Full Path to Data', message=f"'Full Path to Data' '{row['Full Path to Data']}' is not found on your local machine.", issue_type='error' )

    def _check_conditional_columns( self, idx: Hashable, row: pd.Series, surgeon_hawkids: dict ) -> None:
        if not self._col_is_empty( row['Epic End Time'] ) and row['Epic End Time'] != 'unknown':
            if datetime.strptime( str( row['Epic End Time'] ), '%H:%M' ) < datetime.strptime( row['Epic Start Time'], '%H:%M' ): 
                self._log_issue( idx=idx, column='Epic End Time', message=f"'Epic End Time' '{str( row['Epic End Time'] )}' cannot be before provided 'Epic Start Time' '{ str( row['Epic Start Time'] )}'.", issue_type='error' )
                
        # Checks for Supervising Surgeon HawkID.
        if self._col_is_empty( row['Supervising Surgeon HawkID'] ):
            self._df.at[idx, 'Supervising Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message="'Supervising Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] != 'unknown':
            if row['Supervising Surgeon HawkID'] not in surgeon_hawkids:
                self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') not found in the 'Surgeons' config table.", issue_type='error' )

        # Checks for Performing Surgeon HawkID.
        if self._col_is_empty( row['Performing Surgeon HawkID'] ):
            self._df.at[idx, 'Performing Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message="'Performing Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] != 'unknown':
            if row['Performing Surgeon HawkID'] not in surgeon_hawkids:
                self._log_issue( idx=idx, column='Performing Surgeon HawkID', message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID']}') not found in the 'Surgeons' config table.", issue_type='error' )
        elif not self._col_is_empty( row['Performer HawkID-Task']) :
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message="'Performing Surgeon HawkID' cannot be empty if 'Performer HawkID-Task' is not empty.", issue_type='error' )
            self._log_issue( idx=idx, column='Performer HawkID-Task', message="'Performing Surgeon HawkID' cannot be empty if 'Performer HawkID-Task' is not empty.", issue_type='error' )
        
        # Checks for # of Participating Performing Surgeons
        if self._col_is_empty( row['# of Participating Performing Surgeons'] ):
            num_surgeons = None
            if not self._col_is_empty( row['Performer HawkID-Task'] ) and row['Performing Surgeon HawkID'] != 'unknown':
                self._log_issue( idx=idx, column='# of Participating Performing Surgeons', message="'# of Participating Performing Surgeons' cannot be empty if 'Performer HawkID-Task' or 'Performing Surgeon HawkID' is not empty.", issue_type='error' )
                self._log_issue( idx=idx, column='Performer HawkID-Task', message="'# of Participating Performing Surgeons' cannot be empty if 'Performer HawkID-Task' or 'Performing Surgeon HawkID' is not empty.", issue_type='error' )
        else:
            num_surgeons = int( row['# of Participating Performing Surgeons'] )
            
        # Checks for Performer HawkID-Task
        if not self._col_is_empty( row['Performer HawkID-Task'] ):
            try:
                performer_hawk_id_task = ast.literal_eval( self._validate_and_format_dict_string( row['Performer HawkID-Task'] ) )
                assert isinstance( performer_hawk_id_task, dict ), "The input string was not in a valid Python dictionary format."
                for key in performer_hawk_id_task.keys():
                    if key.lower() not in surgeon_hawkids:
                        self._log_issue( idx=idx, column='Performer HawkID-Task', message=f"'Performer HawkID-Task' key ('{key}') not found in the 'Surgeons' config table.", issue_type='error' )
                    else: # Replace the key with the encoding to protect identity information.
                        performer_hawk_id_task[key] = self.replace_hawk_ids_with_encodings( performer_hawk_id_task[key], surgeon_hawkids )
                if len( performer_hawk_id_task.keys() ) != num_surgeons:
                    self._log_issue( idx=idx, column='Performer HawkID-Task', message=f"Number of keys in 'Performer HawkID-Task' ({len(performer_hawk_id_task)}) does not match '# of Participating Performing Surgeons' ({num_surgeons}).", issue_type='error' )
                    self._log_issue( idx=idx, column='# of Participating Performing Surgeons', message=f"Number of keys in 'Performer HawkID-Task' ({len(performer_hawk_id_task)}) does not match '# of Participating Performing Surgeons' ({num_surgeons}).", issue_type='error' )
            except:
                self._log_issue( idx=idx, column='Performer HawkID-Task', message="'Performer HawkID-Task' must be structured in a valid Python dictionary format, e.g., '{surgeon1_hawkid: task performed, surgeon2_hawkid: task_performed, ...}'.", issue_type='error' )
        elif num_surgeons is not None and num_surgeons != 1:
            self._log_issue( idx=idx, column='Performer HawkID-Task', message="'Performer HawkID-Task' cannot be empty if '# of Participating Performing Surgeons' is not 1.", issue_type='error' )
            self._log_issue( idx=idx, column='# of Participating Performing Surgeons', message="'Performer HawkID-Task' cannot be empty if '# of Participating Performing Surgeons' is not 1.", issue_type='error' )

        # Additional checks for consistency between columns
        if not self._col_is_empty( row['Performing Surgeon HawkID'] ) and not self._col_is_empty( row['Performer HawkID-Task'] ):
            if row['Performing Surgeon HawkID'] not in row['Performer HawkID-Task'] and row['Performing Surgeon HawkID'] != 'unknown' and row['Performing Surgeon HawkID'] != 'not-applicable':
                self._log_issue( idx=idx, column='Performer HawkID-Task', message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID']}') specified but not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}'); double-check spelling.", issue_type='error' )
                self._log_issue( idx=idx, column='Performing Surgeon HawkID', message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID']}') specified but not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}'); double-check spelling.", issue_type='error' )
        if not self._col_is_empty( row['Supervising Surgeon HawkID'] ) and not self._col_is_empty( row['Performer HawkID-Task'] ):
            if row['Supervising Surgeon HawkID'] not in row['Performer HawkID-Task'] and row['Supervising Surgeon HawkID'] != 'unknown' and row['Supervising Surgeon HawkID'] != 'not-applicable':
                self._log_issue( idx=idx, column='Performer HawkID-Task', message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') specified but not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}'); double-check spelling.", issue_type='error' )
                self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') specified but not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}'); double-check spelling.", issue_type='error' )
        
        if not self._col_is_empty( row['Skills Assessment Requested'] ) and row['Skills Assessment Requested'] != 'unknown':
            if self._col_is_empty( row['Assessor HawkID'] ):            # Let the user state that they know an assessment was done but not who did it.
                self._df.at[idx, 'Assessor HawkID'] = 'unknown'
                self._log_issue( idx=idx, column='Assessor HawkID', message="'Assessor HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
            elif row['Assessor HawkID'] not in surgeon_hawkids:
                self._log_issue( idx=idx, column='Assessor HawkID', message=f"'Assessor HawkID' ('{row['Assessor HawkID']}') not found in the 'Surgeons' config table.", issue_type='error' )
            if not self._col_is_empty( row['Assessor HawkID'] ) and row['Skills Assessment Requested'] != 'y':
                self._log_issue( idx=idx, column='Skills Assessment Requested', message=f"'Assessor HawkID' provided but 'Skills Assessment Requested' ('{row['Skills Assessment Requested']}') not set to 'Y'.", issue_type='error' )
            if not self._col_is_empty( row['Additional Assessment Details'] ):
                self._issues_appending_helper( in_row=row, idx=idx, col_name='Additional Assessment Details', hawk_ids=surgeon_hawkids )
        if not self._col_is_empty( row['Was Radiology Contacted'] ) and row['Was Radiology Contacted'] != 'unknown':
            if self._col_is_empty( row['Radiology Contact Date'] ):
                self._log_issue( idx=idx, column='Radiology Contact Date', message="'Radiology Contact Date' cannot be blank when 'Was Radiology Contacted' is specified.", issue_type='error' )
            else:   # ensure that the text provided corresponds to a date                                                                
                try:
                    datetime.strptime( row['Radiology Contact Date'], '%Y-%m-%d' )
                except:     
                    self._log_issue( idx=idx, column='Radiology Contact Date', message=f"'Radiology Contact Date' ('{row['Radiology Contact Date']}') is not a valid date format.", issue_type='error' )    
         
    def _check_optional_columns( self, idx: Hashable, row: pd.Series, surgeon_hawkids: dict ) -> None:
        if self._col_is_empty( row['Quality'] ):                                                                                     
            self._df.at[idx, 'Quality'] = 'unknown'
            self._log_issue( idx=idx, column='Quality', message="'Quality' is blank, converting to 'unknown'.", issue_type='warning' )
        if self._col_is_empty( row['Supervising Surgeon HawkID'] ):
            self._df.at[idx, 'Supervising Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message="'Supervising Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Supervising Surgeon HawkID'] not in surgeon_hawkids:
            self._df.at[idx, 'Supervising Surgeon HawkID'] = self.config.get_uid( table_name='Surgeons', item_name=row['Supervising Surgeon'] )
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if self._col_is_empty( row['Performing Surgeon HawkID'] ):
            self._df.at[idx, 'Performing Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message="'Performing Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] not in surgeon_hawkids: 
            self._df.at[idx, 'Performing Surgeon HawkID'] = self.config.get_uid( table_name='Surgeons', item_name=row['Performing Surgeon HawkID'] )
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if not self._col_is_empty( row['Unusual Features'] ):                   self._issues_appending_helper( in_row=row, idx=idx, col_name='Unusual Features', hawk_ids=surgeon_hawkids )
        if not self._col_is_empty( row['Diagnostic Notes'] ):                   self._issues_appending_helper( in_row=row, idx=idx, col_name='Diagnostic Notes', hawk_ids=surgeon_hawkids )
        if not self._col_is_empty( row['Additional Comments'] ):                self._issues_appending_helper( in_row=row, idx=idx, col_name='Additional Comments', hawk_ids=surgeon_hawkids )
        if self._col_is_empty( row['Skills Assessment Requested'] ):                                                                 
            self._df.at[idx, 'Skills Assessment Requested'] = 'unknown'
            self._log_issue( idx=idx, column='Skills Assessment Requested', message="'Skills Assessment Requested' is blank, converting to 'unknown'.", issue_type='warning' )
        if self._col_is_empty( row['Was Radiology Contacted'] ):                                                                     
            self._df.at[idx, 'Was Radiology Contacted'] = 'unknown'
            self._log_issue( idx=idx, column='Was Radiology Contacted', message="'Was Radiology Contacted' is blank, converting to 'unknown'.", issue_type='warning' )

    # --------------- Helpers ----------------------------------------------------------------
    def _col_is_empty( self, col_name: str )  -> bool:      return col_name in ['', ' ', None]

    def _revise_string( self, in_str: str ) -> str:         return re.sub( r'\'\s', '\'', re.sub(r'\s\'', '\'', in_str ) )

    def _validate_and_format_dict_string( self, input_string: str ) -> str:
        # Strip all apostrophes and quotation marks from the string
        input_string = input_string.replace( "'", '' ).replace( '"', '' )

        # Check that the string begins with '{' and ends with '}'
        assert input_string.startswith( '{') and input_string.endswith('}' ), "The input string must begin with '{' and end with '}'."

        # Check for appropriate use of semicolons and colons
        assert re.match( r'^\{(?:\s*[^{}]+:\s*[^{}]+(?:;\s*[^{}]+:\s*[^{}]+)*\s*)?\}$', input_string ), "The input string must use semicolons and colons appropriately."

        # Add quotes around keys and values if they are not already quoted
        def add_quotes( match ):  
            key, value = match.groups()
            key, value = key.strip(), value.strip()
            if not (key.startswith('"') and key.endswith('"')):         key = f'"{key}"'
            if not (value.startswith('"') and value.endswith('"')):     value = f'"{value}"'
            return f'{key}: {value}'
        return re.sub( r'(\b\w+\b)\s*:\s*([^,{}]+)', add_quotes, input_string ).replace(';', ',')

    def retrieve_server_config_tags( self ) -> dict:
        surgeon_hawkids = self.config.list_of_all_items_in_table( table_name='Surgeons' )
        return {'hawk_ids': [id.lower() for id in self.config.list_of_all_items_in_table( table_name='Registered_Users' )],
            'institutions': [i.lower() for i in self.config.list_of_all_items_in_table( table_name='Acquisition_Sites' )],
            'procedure_names': [p.lower() for p in self.config.list_of_all_items_in_table( table_name='Groups' )],
            'surgeon_hawk_ids': surgeon_hawkids,
            'surgeon_hawk_id_encodings': {u.lower(): self.config.get_uid( table_name='Surgeons', item_name=u ) for u in surgeon_hawkids} }
     
    def _issues_appending_helper( self, in_row: pd.Series, idx: Hashable, col_name: str, hawk_ids: dict ) -> None:
        self._df.at[idx, col_name], issues = self.replace_hawk_ids_with_encodings( in_str=in_row[col_name], hawk_ids=hawk_ids )
        for iss in issues:
            self._log_issue( idx=idx, column=col_name, message=iss, issue_type='warning' )

    def replace_hawk_ids_with_encodings( self, in_str: str, hawk_ids: dict ) -> Tuple[str, List[str]]:
        """ Replace all instances of HawkIDs in a string with their respective encoding. """
        issues = []
        for k in hawk_ids:
            if k in in_str:
                in_str = in_str.replace( k, 'HAWKID='+hawk_ids[k] )
                issues.append( f"/nHawkID '{k}' was found unencoded within the string, output will overwrite this." )    
        return in_str, issues
    
    def print_rows( self, rows: Opt[str]='both' ) -> str:
        class_name = self.__class__.__name__;
        assert rows in ['errors', 'warnings', 'both', 'all'], f"Inputted 'rows' must be a list containing either 'errors', 'warnings', 'both', or 'all'; you provided '{rows}'."
        if ( self.summary_table == '' ).all().all():   return f"{class_name}\n\tFilename:\t{self.ffn.name}\n\tRows:\t{len(self.df)}\n\tCols:\t{len(self.df.columns)}\n\tIssues:\tNone"
        num_row_errs = self.summary_table.apply( lambda row: any(cell in ['E', 'EW'] for cell in row), axis=1 ).sum()
        num_row_warns = self.summary_table.apply( lambda row: any(cell in ['W', 'EW'] for cell in row), axis=1 ).sum()
        df_str = self._summary_table.copy()
        df_str['row'] = df_str.index + 2  # Add a column with the original indices
        if rows == 'errors':
            df_str = df_str.loc[df_str.apply(lambda row: any(cell in ['E', 'EW'] for cell in row), axis=1)]
        elif rows == 'warnings':
            df_str = df_str.loc[df_str.apply(lambda row: any(cell in ['W', 'EW'] for cell in row), axis=1)]
        elif rows == 'both':
            df_str = df_str.loc[df_str.apply(lambda row: any(cell in ['E', 'EW', 'W'] for cell in row), axis=1)]
        
        df_str.columns = ['\n'.join(col.split()) for col in df_str.columns]
        cols = df_str.columns.tolist()
        cols = [cols[-1]] + cols[:-1]
        df_str = df_str[cols]
        if len( df_str ) > 0:
            df_str = tabulate( df_str.values.tolist(), headers=df_str.columns.tolist(), tablefmt='pretty', showindex=False, stralign='center' )
        else:
            df_str = ''
        return f"{class_name}\n\tFile:\t{self.ffn.name}\n\tRows:\t{len(self.df)+1} (w header)\n\t\t/w Errors:\t{num_row_errs}\n\t\t/w Warnings:\t{num_row_warns}\n\tCols:\t{len(self.df.columns)}\n{df_str}"

    def build_issue_details( self ) -> List[Dict[str, Any]]:
        failed_details = []
        for idx, row in self._errors.iterrows():
            errors, warnings = [], []
            for column in self._errors.columns:
                if row[column]:                 errors.extend(row[column])
            if idx in self._warnings.index:
                warning_row = self._warnings.loc[idx]  # type: ignore .at removes the red line but leads to a runtime error
                for column in self._warnings.columns:
                    if warning_row[column]:     warnings.extend(warning_row[column])
            if errors or warnings:
                failed_details.append( { 'row': idx, 'errors': errors, 'warnings': warnings } )
        return failed_details
    

    def generate_summary( self, write_to_file: Opt[bool] = False ) -> str:
        # Compute the number of failed and successful rows
        failed_details = self.build_issue_details()
        failed_rows = len(failed_details)
        # successful_rows = len(self.successful_details)

        # Create the header
        header = textwrap.dedent( f"""\
        Summary of Mass Data Upload
        ===========================

        Filename: {self.ffn.name}
        Date: {datetime.now().strftime("%Y-%m-%d %H:%M")}

        Total Rows Processed:\t{len( self.df )}
        Successful Rows:\t{''}
        Failed Rows:\t\t{failed_rows}

        *Row corresponds to the header with column names.

        ------------------------
        Details for Failed Rows:
        ------------------------
        """ )

        # Create the details for failed rows
        failed_rows_details = ""
        for i, detail in enumerate( failed_details, 1 ):
            failed_rows_details += f"({i}/{failed_rows}) Row {detail['row']+1}:\n"
            for error in detail['errors']:
                failed_rows_details += f"- Error: {error}\n"
            for warning in detail.get('warnings', []):
                failed_rows_details += f"- Warning: {warning}\n"
            failed_rows_details += "\n"

        # Create the details for successful rows
        successful_rows_details = "\n----------------------------\nDetails for Successful Rows:\n----------------------------\n"
        # for i, detail in enumerate(self.successful_details, 1):
        #     successful_rows_details += f"({i}/{successful_rows})\tRow {detail['row']}: \tsuccessfully uploaded at {detail['time']} as '{detail['id']}'\n"
        #     successful_rows_details += f"\t- {detail['surgeons']} surgeon(s) operating on a {detail['case']} case on {detail['date']} in {detail['location']}.\n"

        # Create the footer & Combine all parts
        output = header + failed_rows_details + successful_rows_details + "\n===========================\nEnd of Summary"
        if write_to_file:
            with open( self._ffn.with_name( self._ffn.stem + '-summary.txt' ), 'w' ) as f:
                f.write( output )
        return output
    

    def upload_sessions( self, xnat_connection: XNATConnection, verbose: bool = True, write_to_file: Opt[bool]=False ) -> None:
        """ Upload the sessions to XNAT. """

        # Note TO SELF -- Reconvert all commentary to Uppercase for the first letter of each sentence. ********

        if write_to_file: # Write before printing because the header
            with open( self.ffn.with_suffix('.txt'), 'w' ) as f: f.write( output )
        assert 1 == 0, "Functionality not yet implemented."
        txt = self._init_mass_upload_summary_doc()
        ind = 0
        failed_rows = {}
        for idx, row in self.df.iterrows():
            # Iterate through the errors and append them to the failed_rows dictionary key for this row index. Use self.summary_table to get the exact error description
            if any( self.summary_table.loc[idx].notna() & self.summary_table.loc[idx] != '' ): # type: ignore
                failed_rows[ind] = [self.errors.loc[ind, col] for col in self.errors.columns if self.summary_table.loc[ind, col] == 'E']
                continue
        print( failed_rows)

    def __str__( self ) -> str:                             return self.print_rows( rows='all' )