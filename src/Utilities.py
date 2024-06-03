import json
import os

import cv2
import numpy as np

import pandas as pd

from datetime import datetime
from datetime import datetime
from dateutil import parser
from datetime import datetime
import pytz


from pyxnat import Interface
from pyxnat.core.resources import Project as pyxnatProject

from typing import Optional as Opt, Tuple, List as typehintList, Dict as typehintDict, AnyStr as typehintAnyStr

# potentially unused:
import pydicom
from pydicom.uid import UID as pydicomUID, generate_uid as generate_pydicomUID
from pathlib import Path, PurePosixPath


# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['LibrarianUtilities', 'XNATLogin', 'MetaTables', 'XNATConnection', 'USCentralDateTime']


#--------------------------------------------------------------------------------------------------------------------------
## Helper class for inserting all information that should only be used locally within the _local_variables class def below:
class _local_variables:
    def __init__( self ):
        self._img_sizes = ( 256, 256 )
        self.__dict__.update( self._set_local_variables() )

    def _read_template_image( self, template_ffn: str ) -> np.ndarray:
        return cv2.resize( cv2.imread( template_ffn, cv2.IMREAD_GRAYSCALE ), self._img_sizes ).astype( np.uint8 )

    def __getattr__( self, attr ):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            raise AttributeError( f"Attribute {attr} is not yet defined." )
    
    def __str__( self ) -> str:
        return '\n'.join([f'{k}:\t{v}' for k, v in self.__dict__.items()])

    def _set_local_variables( self ) -> dict:
        repo_dir = os.getcwd()
        doc_dir = os.path.join( repo_dir, 'doc' )
        # doc_dir = os.path.join(os.path.dirname(repo_dir), 'doc' )
        data_dir = doc_dir.replace( 'doc', 'data' )
        template_img_dir = os.path.join( data_dir, 'image_templates', 'unwanted_dcm_image_template.png' )
        local_vars =  { 'doc_dir': doc_dir,
                        'data_dir': data_dir,
                        'tmp_data_dir': os.path.join( data_dir, 'tmp' ),
                        'cataloged_resources_ffn': os.path.join( doc_dir, r'cataloged_resources.json' ),
                        'meta_tables_ffn': os.path.join( data_dir, 'meta_tables.json' ),
                        'required_login_keys': ['USERNAME', 'PASSWORD', 'URL'],
                        'xnat_project_name': 'domSandBox',
                        'xnat_project_url': 'https://rpacs.iibi.uiowa.edu/xnat/',
                        'default_meta_table_columns' : ['NAME', 'UID', 'CREATED', 'REGISTERED_USER'],
                        'template_img_dir' : template_img_dir,
                        # 'template_img_hash' : ImageHash( self._read_template_image( template_img_dir ) ).hashed_img
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
                                                'Approve','Reject']
                        }
        # local_vars.template_img_hash = ImageHash( local_vars.template_img_dir ).hashed_img
        return local_vars


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all utitlities to inherit from.
class LibrarianUtilities:
    def __init__( self ):
        self._local_variables = _local_variables()
    
    @property
    def local_variables( self ) -> _local_variables:    return self._local_variables
    @property
    def doc_dir( self ) -> str:                     return self.local_variables.doc_dir
    @property
    def data_dir( self ) -> str:                    return self.local_variables.data_dir
    @property
    def tmp_data_dir( self ) -> str:                return self.local_variables.tmp_data_dir
    @property
    def cataloged_resources_ffn( self ) -> str:     return self.local_variables.cataloged_resources_ffn
    @property
    def meta_tables_ffn( self ) -> str:             return self.local_variables.meta_tables_ffn
    @property
    def required_login_keys( self ) -> list:        return self.local_variables.required_login_keys
    @property
    def xnat_project_name( self ) -> str:           return self.local_variables.xnat_project_name
    @property
    def xnat_project_url( self ) -> str:            return self.local_variables.xnat_project_url
    @property
    def default_meta_table_columns( self ) -> list: return self.local_variables.default_meta_table_columns
    @property
    def template_img_dir( self ) -> str:            return self.local_variables.template_img_dir
    @property
    def template_img( self ) -> np.ndarray:         return self.local_variables.template_img
    @property
    def acceptable_img_dtypes( self ) -> list:      return self.local_variables.acceptable_img_dtypes
    @property
    def required_img_size_for_hashing( self ) -> tuple: return self.local_variables.required_img_size_for_hashing
    @property
    def mturk_batch_col_names( self ) -> list:      return self.local_variables.mturk_batch_col_names
    
    def convert_all_kwarg_strings_to_uppercase( **kwargs ):
        return {k: v.upper() if isinstance(v, str) else v for k, v in kwargs.items()}


#--------------------------------------------------------------------------------------------------------------------------
## Class for validating xnat login information
class XNATLogin( LibrarianUtilities ):
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

    def __init__( self, input_info: dict ):
        super().__init__()  # Call the __init__ method of the base class
        self._validated_username, self._validated_password = '', '', 
        self._is_valid, self._user_role = False, ''
        self._validate_login( input_info )

    def _validate_login( self, input_info ): # If all checks pass, set _is_valid to True and deal login info
        assert len( input_info ) == 3, f"Provided login info dictionary must only have the following three key-value pairs: {self.required_login_keys}"
        self._provided_info, validated_info = input_info, {k.upper(): v for k, v in input_info.items()}
        assert validated_info['URL'].lower() == self.local_variables.xnat_project_url, f"Provided server URL is invalid: {validated_info['URL']}"
        assert all( k in validated_info for k in self.required_login_keys ), f"Missing login info: {set( self.required_login_keys ) - set( validated_info.keys() )}"
        self._is_valid, self._validated_username, self._validated_password = True , validated_info['USERNAME'], validated_info['PASSWORD']

    @property
    def provided_info( self ) -> dict:      return self._provided_info
    @property
    def is_valid( self ) -> bool:           return self._is_valid
    @property
    def user_role( self ) -> str:           return self._user_role
    @property
    def validated_username( self ) -> str:  return self._validated_username
    @property
    def validated_password( self ) -> str:  return self._validated_password

    def __str__( self ) -> str:
        if self.is_valid:
            return f"-- Validated XNATLogin --\n\tUser: {self.validated_username}\n\tServer: {self.xnat_project_url}\n"
        return f"-- Invalid XNATLogin --\n\tUser: {self.validated_username}\n\tServer: {self.xnat_project_url}\n"

    # def doc( self ) -> str: return self.__doc__


#--------------------------------------------------------------------------------------------------------------------------
## Class for cataloging all seen data and user info.
class MetaTables( LibrarianUtilities ):
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
        - I (dom mattioli) will be the only one allowed to add anything and I will control the 'metatables.json' file via a protected branch in the github repository
        - In the future it might make more sense to use a simple relational database.

    # Example usage (if not building from scratch, try adding a new table other than those listed above):
    my_login_info = { 'USERNAME': '...', 'PASSWORD' : '...', 'URL' : 'https://rpacs.iibi.uiowa.edu/xnat/' }
    my_login = XNATLogin( my_login_info )
    mt = MetaTables( my_login )
    print( mt )

    # Create some new tables -- convention is for inputs to be plural. Will automatically caps everything
    mt.add_new_table( table_name='hello' )

    # Add new items to the tables - convention is for inputs to be plural
    mt.add_new_item( table_name='hello', item_name='world' )
    # mt.save()
    '''
    def __init__( self, login_info: XNATLogin, print_out: Opt[bool] = False ):
        assert login_info.is_valid, f"Provided login info must be validated before accessing metatables: {login_info}"
        super().__init__()  # Call the __init__ method of the base clas
        self._login_info = login_info
        if os.path.isfile( self.meta_tables_ffn ):  self._load( print_out )
        else:                                       self._instantiate_json_file() 


    @property
    def login_info( self ) -> XNATLogin:    return self._login_info
    @property
    def tables( self ) -> dict:             return self._tables
    @property
    def metadata( self ) -> dict:           return self._metadata
    @property
    def accessor_username( self ) -> str:   return str( self.login_info.validated_username ).upper() 
    @property
    def accessor_uid( self ) -> str:        return self.get_uid( 'REGISTERED_USERS', self.accessor_username )
    @property
    def now_datetime( self ) -> str:        return datetime.now( pytz.timezone( 'US/Central' ) ).isoformat()

    #==========================================================PRIVATE METHODS==========================================================
    def _instantiate_json_file( self ):
        '''#Instantiate with a registered users table.'''
        assert self.login_info.is_valid, f"Provided login info must be validated before loading metatables: {self.login_info}"
        assert self.accessor_uid is not None, f'BUG: shouldnt arrive to this point in the code without having an established username; receive: {self.accessor_uid}.'
        now_datetime = self.now_datetime
        default_users = { 'DMATTIOLI':              [self._generate_uid(), now_datetime, 'INIT'] }
        if self.accessor_username not in ( k.upper() for k in default_users.keys() ):
            default_users[self.accessor_username] = [self._generate_uid(), now_datetime, 'INIT']
        data = [[name] + info for name, info in default_users.items()]
        self._tables = {    'REGISTERED_USERS': pd.DataFrame( data, columns=self.default_meta_table_columns ) }
        self._metadata = {  'CREATED': now_datetime,
                            'LAST_MODIFIED': now_datetime,
                            'REGISTERED_USER': self.accessor_uid }
        
        # Fill initialized tables
        self.add_new_table( 'AcquisitioN_sites' )
        self.add_new_item( 'acquisitIon_sites', 'UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS' )
        self.add_new_item( 'acqUisition_sites', 'UNIVERSITY_OF_HOUSTON' )
        self.add_new_item( 'ACQUISITION_SItes', 'AMAZON_MECHANICAL_TURK' )
        self.add_new_table( 'gRouPs' )
        self.add_new_item( 'grOups', 'DYNAMIC_HIP_scrEW' )
        self.add_new_item( 'grOups', 'TROCHANTERIC_STABILIZATION_PLATE' )
        self.add_new_item( 'groups', 'KNEE_ARTHROSCOPY' )
        self.add_new_item( 'groups', 'INTERMEDULLARY_NAIL' )
        self.add_new_item( 'groups', 'TROCHANTERIC_STABILIZING_PLATE' )
        self.add_new_item( 'groUps', 'PEDIATRIC_SUPRACONDYLaR_HUMERUS_FRACTURE' )
        self.add_new_table( 'subjects', ['acquisition_site', 'group'] ) # need additional columns to reference uids from other tables
        self.add_new_table( 'IMAGE_HASHES', ['subject', 'INSTANCE_NUM'] ) # need additional columns to reference uids from other tables
        # self.save()
        
    def _load( self, print_out: Opt[bool] = False ) -> None:
        assert self.login_info.is_valid, f"Provided login info must be validated before loading metatables: {self.login_info}"
        with open( self.meta_tables_ffn, 'r' ) as f:
            data = json.load( f )
        self._tables = { name: pd.DataFrame.from_records( table ) for name, table in data['tables'].items()}
        self._metadata = data['metadata']
        if print_out:
            print( f'SUCCESS! -- Loaded metatables from: {self.meta_tables_ffn}' )
    
    def _update_metadata( self ) -> None:
        self.metadata.update( {'LAST_MODIFIED': self.now_datetime, 'REGISTERED_USER': self.accessor_uid} )
    
    def _init_table_w_default_cols( self ) -> pd.DataFrame:
        return pd.DataFrame( columns=self.default_meta_table_columns ).assign( CREATED=self.now_datetime, REGISTERED_USER=self.accessor_uid )
    
    def _validate_login_for_important_functions( self ) -> None:
        assert self.login_info.is_valid, f"Provided login info must be validated before accessing metatables: {self.login_info}"
        assert self.is_user_registered(), f'User {self.accessor_uid} must first be registed before saving metatables.'
        assert self.get_name( table_name='REGISTERED_USERS', item_uid=self.accessor_uid ) == 'DMATTIOLI', f'Invalid credentials for saving metatables data.'
    
    def _generate_uid( self ) -> str: return str( generate_pydicomUID() ).replace( '.', '_' )

    #==========================================================PUBLIC METHODS==========================================================
    def save( self, print_out: Opt[bool] = False ) -> None: # Convert all tables to JSON; Write the data to the file
        self._validate_login_for_important_functions()
        tables_json = {name: df.to_dict('records') for name, df in self.tables.items()}
        data = {'metadata': self.metadata, 'tables': tables_json }
        with open( self.meta_tables_ffn, 'w' ) as f:
            json.dump( data, f, indent=4 )
        if print_out:
            print( f'SUCCESS! --- saved metatables to: {self.meta_tables_ffn}' )

    def is_user_registered( self, user_name: Opt[str] = None ) -> bool:
        if user_name is None:   user_name = self.accessor_username
        return user_name in self.tables['REGISTERED_USERS']['NAME'].values

    def register_new_user( self, user_name: str, print_out: Opt[bool] = False ):
        self._validate_login_for_important_functions()
        if not self.is_user_registered( user_name ):
            self.add_new_item( 'REGISTERED_USERS', user_name )
        if print_out:
            print( f'SUCCESS! --- Registered new user: {user_name}' )

    def list_of_all_tables( self ) -> list:
        return list( self.tables.keys() )

    def list_of_all_items_in_table( self, table_name: str ) -> list:
        return list( self.tables[table_name.upper()]['NAME'] )

    def table_exists( self, table_name: str ) -> bool:
        return table_name.upper() in self.list_of_all_tables()

    def item_exists( self, table_name: str, item_name: str ) -> bool:
        return not self.tables[table_name.upper()].empty and item_name.upper() in self.tables[table_name.upper()].values

    def add_new_table( self, table_name: str, extra_column_names: Opt[typehintList[str]] = None, print_out: Opt[bool] = False ) -> None:
        assert self.is_user_registered(), f"User '{self.accessor_username}' must first be registed before adding new items."
        table_name = table_name.upper()
        assert not self.table_exists( table_name ), f'Cannot add table "{table_name}" because it already exists.'
        self._tables[table_name] = self._init_table_w_default_cols()
        if extra_column_names: # checks if it is not None and if the dict is not empty
            for c in extra_column_names: 
                self._tables[table_name][c.upper()] = pd.Series([None] * len(self._tables[table_name])) # don't forget to convert new column name to uppercase
        self._update_metadata()
        if print_out:
            print( f'SUCCESS! --- Added new "{table_name}" table' )

    def add_new_item( self, table_name: str, item_name: str, extra_columns_values: Opt[typehintDict[str, str]] = None, print_out: Opt[bool] = False ) -> None:
        assert self.is_user_registered(), f"User '{self.accessor_username}' must first be registed before adding new items."
        table_name, item_name = table_name.upper(), item_name.upper(),
        assert self.table_exists( table_name ), f"Cannot add item '{item_name}' to table '{table_name}' because that table does not yet exist.\n\tTry creating the new table before adding '{item_name}' as a new item."
        assert not self.item_exists( table_name, item_name ), f'Cannot add item "{item_name}" to Table "{table_name}" because it already exists.'

        new_item_uid = self._generate_uid()
        if extra_columns_values: # convert keys to uppercase, make sure all inputted keys were defined when the table was added as new.
            extra_columns_values = {k.upper(): v for k, v in extra_columns_values.items()}
            assert all( k in self.tables[table_name].columns for k in extra_columns_values.keys() ), f"Provided extra column names must exist in the table: {table_name}"
            all_cols = set( self.tables[table_name].columns )
            default_cols = list( self.default_meta_table_columns )
            missing_cols = all_cols.difference( default_cols + list( extra_columns_values.keys() ) )
            assert missing_cols == set(), f"All non-default columns in the table must be defined when adding a new item; missing value definition for: {missing_cols}"
            new_data = pd.DataFrame( [[item_name, new_item_uid, self.now_datetime, self.accessor_username, *extra_columns_values.values()]], columns=self.tables[table_name].columns )
        else: # No inserted data for extra columns
            new_data = pd.DataFrame( [[item_name, new_item_uid, self.now_datetime, self.accessor_username]], columns=self.tables[table_name].columns )


        # # For empty tables, we need a special approach
        # if self.tables[table_name].empty:
        #     self._tables[table_name] = self._init_table_w_default_cols()
        #     if extra_columns_values: # checks if it is not None and if the dict is not empty
        #         for k, v in extra_columns_values.items():
        #             self._tables[table_name][k.upper()] = None

        self._tables[table_name] = pd.concat( [self.tables[table_name], new_data], ignore_index=True )
        self._update_metadata()
        if print_out:
            print( f'\tSUCCESS! --- Added "{item_name}" to table "{table_name}"' )

    def get_uid( self, table_name: str, item_name: str ) -> str:
        table_name, item_name = table_name.upper(), item_name.upper()
        assert self.item_exists( table_name, item_name ), f"Item '{item_name}' does not exist in table '{table_name}'"
        return str( self.tables[table_name].loc[self.tables[table_name]['NAME'] == item_name, 'UID'].values[0] )

    def get_name( self, table_name: str, item_uid: str ) -> str:
        table_name, item_uid = table_name.upper(), item_uid.upper()
        assert self.item_exists( table_name, item_uid ), f"Item '{item_uid}' does not exist in table '{table_name}'"
        return str( self.tables[table_name].loc[self.tables[table_name]['UID'] == item_uid, 'NAME'].values[0] )

    def __str__( self ) -> str:
        output = [f'\n-- MetaTables -- Accessed by: {self.accessor_username}']
        for table_name, table_data in self.tables.items():
            if table_name == 'REGISTERED_USERS':
                continue  # Skip the 'REGISTERED_USERS' table
            output.append( f'\tTable: {table_name}')
            if table_data.empty:
                output.append( '\t--Empty--' )
            else:
                if len( table_data ) > 5: # If the table has more than 5 rows, print only the first and last two rows
                    for idx, row in table_data.head(2).iterrows():
                        output.append( f'\t{idx+1:<5}{row["NAME"]:<50}')
                    output.append( '\t...' )
                    for idx, row in table_data.tail(2).iterrows():
                        output.append( f'\t{idx+1:<5}{row["NAME"]:<50}')
                else: # If the table has 5 or fewer rows, print all rows
                    for idx, row in table_data.iterrows():
                        output.append( f'\t{idx+1:<5}{row["NAME"]:<50}')
            output.append('')  # Add a new line after each table
        return '\n'.join( output )

    # def doc( self ) -> str: return self.__doc__


#--------------------------------------------------------------------------------------------------------------------------
## Class for establishing a connection to the xnat server with specific credentials.
class XNATConnection( LibrarianUtilities ):
    '''
    to-do: document explanation of class

    # Example usage:
    my_login_info = {'URL': 'https://rpacs.iibi.uiowa.edu/xnat/', 'USERNAME': '...', 'PASSWORD': '...'}
    my_login = XNATLogin( login_info )
    mt = MetaTables( my_login )
    my_connection = XNATConnection( my_login, my ) 
    print( my_connection )
    # my_connection = XNATConnection( my_login, my, stay_connected=True ) 
    # print( my_connection )
    '''
    def __init__( self, login_info: XNATLogin, meta_tables: MetaTables, stay_connected: bool = False ):
        assert login_info.is_valid, f"Provided login info must be validated before accessing metatables: {login_info}"
        super().__init__()  # Call the __init__ method of the base clas
        self._login_info, self.meta_tables, self._open, self._project_handle = login_info, meta_tables, stay_connected, None
        self._verify_login()
        if stay_connected:
            self.server.disconnect()
            # self.server = None

    @property
    def login_info( self ) -> XNATLogin:    return self._login_info
    @property
    def server( self ) -> Interface:        return self._server
    @property
    def project_query_str( self ) -> str:   return self._project_query_str
    @property
    def project_handle( self ) -> pyxnatProject:  return self._project_handle # type: ignore
    @property
    def is_verified( self ) -> bool:        return self._is_verified
    @property
    def open( self ) -> bool:               return self._open
    @property
    def get_user( self ) -> Opt[str]:       return self.login_info.validated_username
    @property
    def get_password( self ) -> Opt[str]:   return self.login_info.validated_password

    def _verify_login( self, stay_connected: bool = False ):
        self._server = self._establish_connection()
        self._grab_project_handle() # If more tests in the future, separate as its own function.
        self._is_verified = True

    def _establish_connection( self ) -> Interface:
        return Interface( server=self.xnat_project_url, user=self.get_user, password=self.get_password )

    def _grab_project_handle( self ):
        self._project_query_str = '/project/' + self.xnat_project_name
        project_handle = self.server.select( self.project_query_str )
        if project_handle.exists(): # type: ignore
            self._project_handle = project_handle

    def close( self ):
        if hasattr( self, '_server' ):
            self._server.disconnect()
        self._open = False
        print( f'!!!'*3 + '\n\tConnection to XNAT server has been closed -- any unsaved metatable data will be lost!\n' + '!!!'*3 )

    def __del__( self ):    self.close()

    def __enter__( self ):  return self

    def __exit__( self, exc_type, exc_value, traceback ): self.close()

    def __str__( self ) -> str:
        connection_status = "Open" if self.open else "Closed"
        return (f"-- XNAT Connection --\n"
                f"Status:\t\t{connection_status}\n"
                f"Signed-in:\t{self.get_user}\n"
                # f"UID:\t\t{self.uid}\n"
                f"Project:\t{self.project_handle}\n" )


#--------------------------------------------------------------------------------------------------------------------------
## Class for ensuring common formatting of date-time strings.
class USCentralDateTime():
    '''
    # Example usage:
    tst1 = USCentralDateTime( '2022-01-01 11:00:00 PST' )
    print( tst1 )
    print( 'USCentral Date: ' + tst1.date + ', time: ' + tst1.time )
    print( USCentralDateTime( 'nonsense time o'clock' ) )
    '''
    def __init__( self, dt_str: str ):
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
    def date( self ) -> str:    return self.dt.strftime( '%Y%m%d' )
    @property
    def time( self ) -> str:    return self.dt.strftime( '%H%M%S' )
    @property
    def dt( self ) -> datetime: return self._dt # type: ignore

    def __str__( self ) -> str: return f'{self._dt} US-Central\t({self._raw_dt_str})'