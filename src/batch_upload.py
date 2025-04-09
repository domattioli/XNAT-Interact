from pathlib import Path
from typing import Any, Dict, Hashable, List, Optional as Opt, Tuple
import pandas as pd
import numpy as np
import warnings
import re
import ast
from datetime import datetime
from tabulate import tabulate
import textwrap
import ast
from typing import List, Dict, Any, Tuple, AnyStr as typehintAnyStr
from collections.abc import Hashable
from tabulate import tabulate
import requests
from requests.adapters import HTTPAdapter
import difflib

from src.utilities import ConfigTables, XNATConnection, UIDandMetaInfo
from src.xnat_resource_data import *
from src.xnat_experiment_data import *


# Define list for allowable imports from this module -- do not want to import _local_variables. As more classes are added you will need to update this list.
__all__ = ['BatchUploadRepresentation']

#--------------------------------------------------------------------------------------------------------------------------
# Class for representing an imported batch upload spreadsheet.
class BatchUploadRepresentation( UIDandMetaInfo ):
    '''
    NOTE: As of April 2025, this class is written to assume that the surgical procedures are either TRAUMA or ARTHRO. If another class emerges, we will need to revisit this.
        - The upload_sessions() method checks for the presence of the 'ARTHROSCOPY' keyword in the 'Procedure' column of the batch upload file. If it isn't found, it assumes trauma and will try to upload using the SourceRFSession class from xnat_experiment_data.py.

    # Example usage:
    data = BatchUploadRepresentation( xls_ffn=Path( 'path/to/file.xlsx' ), config=ConfigTables( XNatLogin( {...} ) ) )
    use the 'print_rows' method to print out the rows of the batch upload file with the errors and warnings.
        - e.g., data.print_rows( rows='errors' ) # shows all rows with at least one error.
            - use data.print_errors_list() to print out the errors in a more readable format.
        - Each W/E cell in the summary table corresponds to a warning or error associated with that row in the excel file.
        - To see specific errors or warnings, print ( data.errors)
        - Warnings are not fatal to the upload process, but errors are.

    ## Common Errors.
    - Not using semicolons to separate the key-value pairs in Performer HawkID-Task. For example {surgeon1hawkid: task1; surgeon2hawkid: task2}
    - Misspelling hawkids. use config.list_of_all_items_in_table( table_name='SURGEONS' ) to see all valid hawkids, or ask the librarian to add a missing/new surgeon's info.
    - Using the wrong drive letter for data on the RDSS (some people will assign different letters for their hawkid).
    '''
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
            self._df        = self.df.loc[:, ~self.df.columns.str.contains( '^Case Name [Optional]' )] # This column is only there for the user's reference.
            self._df        = self.df.loc[:, ~self.df.columns.str.contains( '^Unnamed' )] # Remove any unnamed columns
            # text_columns    = [k for k, v in self.required_batch_upload_columns.items() if 'text' in v.lower()]
            # for col in text_columns: # Lowercase everything for now for more reliable substring searches (will re-caps first letter of sentences in comments at time of upload).
            #     self.df[col] = self.df[col].astype(str).str.lower().str.strip()
            self._warnings      = pd.DataFrame( data=np.empty( ( len( self.df ), len( self.df.columns ) ), dtype=list ), columns=self.df.columns )   
            self._errors        = self.warnings.copy()
            self._summary_table = self.warnings.copy()
            
        required_columns = [k for k, v in self.required_batch_upload_columns.items() if 'required' in v.lower()]
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        assert not missing_columns, f"The following required columns are missing from the imported file: {', '.join( missing_columns )}"

    def _process_mass_upload_data( self ) -> None:          # Iterate through each row, performing custom checks on all the columns
        for idx, row in self.df.iterrows():
            self._check_required_columns( idx=idx, row=row )
            self._check_optional_columns( idx=idx, row=row )
            self._check_conditional_columns( idx=idx, row=row ) # conditional on either a required or optional column's value.
        
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
            if not isinstance( self._errors.at[idx, column], list ):    self._errors.at[idx, column] = [message]
            else:                                                       self._errors.at[idx, column].append( message )
        elif issue_type == 'warning':
            if not isinstance( self._warnings.at[idx, column], list ):  self._warnings.at[idx, column] = [message]
            else:                                                       self._warnings.at[idx, column].append( message )

    def _check_required_columns( self, idx: Hashable, row: pd.Series ) -> None:
        if self._col_is_empty( row['Filer HawkID'] ) or not self.config.item_exists( table_name='Registered_Users', item_name=row['Filer HawkID'] ):
            self._log_issue( idx=idx, column='Filer HawkID', message=f"'Filer HawkID' ('{row['Filer HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if not self._col_is_empty( row['Operation Date'] ):
            date_str = str( row['Operation Date'] ).split()[0]
            if datetime.strptime( date_str, '%Y-%m-%d' ) < datetime( 2000, 1, 1 ):
                self._log_issue( idx=idx, column='Operation Date', message=f"'Operation Date' ('{row['Operation Date']}') is before January 1, 2000; make sure this is intentional.", issue_type='warning' )
        if self._col_is_empty( row['Epic Start Time'] ):
            self._log_issue( idx=idx, column='Epic Start Time', message="'Epic Start Time' cannot be blank or empty.", issue_type='error' )
        if self._col_is_empty( row['Institution Name'] ) or not self.config.item_exists( table_name='Acquisition_Sites', item_name=row['Institution Name'] ):
            self._log_issue( idx=idx, column='Institution Name', message=f"'Institution Name' ('{row['Institution Name']}') not registered in the 'Acquisition_Sites' config table.", issue_type='error' )
        if self._col_is_empty( row['Procedure Name'] ) or not self.config.item_exists( table_name='Groups', item_name=row['Procedure Name'] ):
            self._log_issue( idx=idx, column='Procedure Name', message=f"'Procedure Name' ('{row['Procedure Name']}') not registered in the 'Groups' config table.", issue_type='error' )
        if self._col_is_empty( row['Full Path to Data'] ):
            self._log_issue( idx=idx, column='Full Path to Data', message="'Full Path to Data' cannot be blank.", issue_type='error' )
        elif not Path( row['Full Path to Data'] ).exists():
            self._log_issue( idx=idx, column='Full Path to Data', message=f"'Full Path to Data' '{row['Full Path to Data']}' is not found on your local machine.", issue_type='error' )
            
    def _check_conditional_columns( self, idx: Hashable, row: pd.Series ) -> None:
        # Assign some variables
        surgeon_hawkids = self.config.list_of_all_items_in_table( table_name='Surgeons' )
        surgeon_hawkids_dict = {u.lower(): self.config.get_uid( table_name='Surgeons', item_name=u ) for u in surgeon_hawkids}
        performing_surgeon_hawkid = row['Performing Surgeon HawkID'].lower() #lower to standardize user input
        supervising_surgeon_hawkid = row['Supervising Surgeon HawkID'].lower()
        assessing_surgeon_hawkid = row['Assessor HawkID'].lower()
        performing_surgeon_suggestion = difflib.get_close_matches( performing_surgeon_hawkid.upper(), surgeon_hawkids, n=1, cutoff=0.6 )
        supervising_surgeon_suggestion = difflib.get_close_matches( supervising_surgeon_hawkid.upper(), surgeon_hawkids, n=1, cutoff=0.6 )
        assessing_surgeon_suggestion = difflib.get_close_matches( assessing_surgeon_hawkid.upper(), surgeon_hawkids, n=1, cutoff=0.6 )
        desired_format = r'{surgeon1_hawkid: task performed; ...; surgeonN_hawkid: task_performed, ...}'

        if not self._col_is_empty( row['Epic End Time'] ) and row['Epic End Time'] != 'unknown':
            if datetime.strptime(':'.join(str(row['Epic End Time']).split(':')[:2]), '%H:%M') < datetime.strptime(':'.join(str(row['Epic Start Time']).split(':')[:2]), '%H:%M'): # Drop the seconds from the time strings
                self._log_issue( idx=idx, column='Epic End Time', message=f"'Epic End Time' '{str( row['Epic End Time'] )}' cannot be before provided 'Epic Start Time' '{ str( row['Epic Start Time'] )}'.", issue_type='error' )
                
        # Checks for Supervising Surgeon HawkID.
        if self._col_is_empty( row['Supervising Surgeon HawkID'] ):
            self._df.at[idx, 'Supervising Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message="'Supervising Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] != 'unknown':
            if not self.config.item_exists( table_name='surgeons', item_name=row['Supervising Surgeon HawkID'] ):
                self._log_issue(
                    idx=idx,
                    column='Supervising Surgeon HawkID',
                    message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID'].upper()}') not found in the 'Surgeons' config table;\n\t--\tDid you mean '{supervising_surgeon_suggestion[0]}'?",
                    issue_type='error' )

        # Checks for Performing Surgeon HawkID.
        if self._col_is_empty( row['Performing Surgeon HawkID'] ):
            self._df.at[idx, 'Performing Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message="'Performing Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] != 'unknown':
            if not self.config.item_exists( table_name='surgeons', item_name=row['Performing Surgeon HawkID'] ):
                self._log_issue(
                    idx=idx,
                    column='Performing Surgeon HawkID',
                    message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID'].upper()}') not found in the 'Surgeons' config table;\n\t--\tDid you mean '{performing_surgeon_suggestion[0]}'?",
                    issue_type='error' )
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
        if num_surgeons is not None and num_surgeons != 1 and not self._col_is_empty( row['Performer HawkID-Task'] ):
            try: # First, we need to validate that the formatting is valid.
                formatted_str, notices = self._validate_and_format_dict_string( row['Performer HawkID-Task'] )
                for notice in notices:
                    self._log_issue( idx=idx, column='Performer HawkID-Task', message=notice, issue_type='warning' )
                performer_hawk_id_task = ast.literal_eval( formatted_str )
                performer_hawk_id_task = {k.lower(): v for k, v in performer_hawk_id_task.items()}
                assert isinstance( performer_hawk_id_task, dict ), "The input string was not in a valid format, e.g., {k1: v1; ...; kn: vn}."
            except:
                self._log_issue(
                    idx=idx,
                    column='Performer HawkID-Task',
                    message=f"'Performer HawkID-Task' must follow this format {desired_format}\n\t--\tYou entered: {row['Performer HawkID-Task']}" )
                return

        # Now proceed to checks.
        if not self._col_is_empty( row['Performer HawkID-Task'] ): # Performer tasks are described -- validate the string and encode any hawkids found; making sure any pre-specified hawkids match those found in the task descriptions
            # At least one of the keys needs to be the performing surgeon hawkid
            if performing_surgeon_hawkid not in performer_hawk_id_task.keys() or performing_surgeon_hawkid == 'unknown':
                self._log_issue(
                    idx=idx,
                    column='Performer HawkID-Task',
                    message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID'].upper()}') not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}').",
                    issue_type='error' )
            
            # Need to make sure the number of keys matches the number of participating surgeons.
            if len(performer_hawk_id_task.keys()) != num_surgeons:
                self._log_issue(
                    idx=idx,
                    column='Performer HawkID-Task',
                    message=f"Number of keys in 'Performer HawkID-Task' ({len(performer_hawk_id_task)}) does not match '# of Participating Performing Surgeons' ({num_surgeons}).\n\t--\tMake sure you're using proper formatting, e.g., {desired_format}\n\t--\tYou entered: {row['Performer HawkID-Task']}",
                    issue_type='error' )
            else:
                for key in performer_hawk_id_task.keys():
                    key_suggestion = difflib.get_close_matches( key.upper(), surgeon_hawkids, n=1, cutoff=0.6 )
                    if not self.config.item_exists( table_name='surgeons', item_name=key ):
                        self._log_issue(
                            idx=idx,
                            column='Performer HawkID-Task',
                            message=f"'Performer HawkID-Task' key ('{key.upper()}') not found in the 'Surgeons' config table;\n\t--\tDid you mean '{key_suggestion[0]}'?",
                            issue_type='error' )
                    else: # Replace the key with the encoding to protect identity information.
                        performer_hawk_id_task[key] = self.replace_hawk_ids_with_encodings( performer_hawk_id_task[key], surgeon_hawkids_dict, original_col_name='Performer HawkID-Task' )
                if len( performer_hawk_id_task.keys() ) != num_surgeons:
                    self._log_issue(
                        idx=idx,
                        column='Performer HawkID-Task',
                        message=f"Number of keys in 'Performer HawkID-Task' ({len(performer_hawk_id_task)}) does not match '# of Participating Performing Surgeons' ({num_surgeons}).\n\t--\tMake sure you're using proper formatting, e.g., {desired_format}\n\t--\tYou entered: {row['Performer HawkID-Task']}",
                        issue_type='error' )
        elif num_surgeons is not None and num_surgeons != 1:
            self._log_issue( idx=idx, column='Performer HawkID-Task', message="'Performer HawkID-Task' cannot be empty if '# of Participating Performing Surgeons' is not 1.", issue_type='error' )
            self._log_issue( idx=idx, column='# of Participating Performing Surgeons', message="'Performer HawkID-Task' cannot be empty if '# of Participating Performing Surgeons' is not 1.", issue_type='error' )

        # Additional checks for consistency between columns
        if not self._col_is_empty(performing_surgeon_hawkid) and not self._col_is_empty( row['Performer HawkID-Task'] ):
            if performing_surgeon_hawkid not in performer_hawk_id_task.keys() and performing_surgeon_hawkid != 'unknown' and performing_surgeon_hawkid != 'not-applicable':
                self._log_issue(
                    idx=idx,
                    column='Performer HawkID-Task',
                    message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID'].upper()}') specified but not found in 'Performer HawkID-Task' ('{row['Performer HawkID-Task']}'); double-check spelling.",
                    issue_type='error'
                )

        if not self._col_is_empty( supervising_surgeon_hawkid ) and not self._col_is_empty( row['Performer HawkID-Task'] ):
            performer_tasks_lower = [str(key).lower() for key in performer_hawk_id_task.keys()]
            if supervising_surgeon_hawkid not in performer_tasks_lower and supervising_surgeon_hawkid != 'unknown' and supervising_surgeon_hawkid != 'not-applicable':
                self._log_issue(
                    idx=idx,
                    column='Performer HawkID-Task',
                    message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') specified but not found in 'Performer HawkID-Task'; you entered: ('{row['Performer HawkID-Task']}').\n\t--\tMake sure you specify any role they had outside of supervising.",
                    issue_type='warning'
                )

        if not self._col_is_empty( row['Skills Assessment Requested'] ) and row['Skills Assessment Requested'] != 'unknown':
            if self._col_is_empty( assessing_surgeon_hawkid ):            # Let the user state that they know an assessment was done but not who did it.
                self._df.at[idx, 'Assessor HawkID'] = 'unknown'
                self._log_issue( idx=idx, column='Assessor HawkID', message="'Assessor HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
            elif not self.config.item_exists( table_name='surgeons', item_name=assessing_surgeon_hawkid ):
                self._log_issue(
                    idx=idx,
                    column='Assessor HawkID',
                    message=f"'Assessor HawkID' ('{row['Assessor HawkID'].upper()}') not found in the 'Surgeons' config table;\n\t--\tDid you mean '{assessing_surgeon_suggestion[0]}'?",
                    issue_type='error' )
            if not self._col_is_empty( assessing_surgeon_hawkid ) and row['Skills Assessment Requested'].lower() != 'y':
                self._log_issue( idx=idx, column='Skills Assessment Requested', message=f"'Assessor HawkID' provided but 'Skills Assessment Requested' ('{row['Skills Assessment Requested']}') not set to 'Y'.", issue_type='error' )
            if not self._col_is_empty( row['Additional Assessment Details'] ):
                self._issues_appending_helper( in_row=row, idx=idx, col_name='Additional Assessment Details', hawk_ids=surgeon_hawkids_dict )
        if self._col_is_empty( row['Radiology Contact Date'] ) and not row['Was Radiology Contacted'].lower() != 'y':
            if self._col_is_empty( row['Radiology Contact Date'] ):
                self._log_issue( idx=idx, column='Radiology Contact Date', message="'Radiology Contact Date' is blank but 'Was Radiology Contacted' is specified; if you have the date, please input it.", issue_type='warning' )
            else:   # ensure that the text provided corresponds to a date                                                                
                try:    datetime.strptime( row['Radiology Contact Date'], '%Y-%m-%d' )
                except: self._log_issue( idx=idx, column='Radiology Contact Date', message=f"'Radiology Contact Date' ('{row['Radiology Contact Date']}') is not a valid date format.", issue_type='error' )    
         
    def _check_optional_columns( self, idx: Hashable, row: pd.Series ) -> None:
        surgeon_hawkids = self.config.list_of_all_items_in_table( table_name='Surgeons' )
        surgeon_hawkids_dict = {u.lower(): self.config.get_uid( table_name='Surgeons', item_name=u ) for u in surgeon_hawkids}
        if self._col_is_empty( row['Quality'] ):                                                                                     
            self._df.at[idx, 'Quality'] = 'unknown'
            self._log_issue( idx=idx, column='Quality', message="'Quality' is blank, converting to 'unknown'.", issue_type='warning' )
        if self._col_is_empty( row['Supervising Surgeon HawkID'] ):
            self._df.at[idx, 'Supervising Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message="'Supervising Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif not self.config.item_exists( table_name='surgeons', item_name=row['Supervising Surgeon HawkID'] ):
            # self._df.at[idx, 'Supervising Surgeon HawkID'] = self.config.get_uid( table_name='Surgeons', item_name=row['Supervising Surgeon'] )
            self._log_issue( idx=idx, column='Supervising Surgeon HawkID', message=f"'Supervising Surgeon HawkID' ('{row['Supervising Surgeon HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if self._col_is_empty( row['Performing Surgeon HawkID'] ):
            self._df.at[idx, 'Performing Surgeon HawkID'] = 'unknown'
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message="'Performing Surgeon HawkID' is blank, converting to 'unknown'.", issue_type='warning' )
        elif row['Performing Surgeon HawkID'] not in surgeon_hawkids: 
            # self._df.at[idx, 'Performing Surgeon HawkID'] = self.config.get_uid( table_name='Surgeons', item_name=row['Performing Surgeon HawkID'] )
            self._log_issue( idx=idx, column='Performing Surgeon HawkID', message=f"'Performing Surgeon HawkID' ('{row['Performing Surgeon HawkID']}') not registered in the 'Registered_Users' config table.", issue_type='error' )
        if not self._col_is_empty( row['Unusual Features'] ):                   self._issues_appending_helper( in_row=row, idx=idx, col_name='Unusual Features', hawk_ids=surgeon_hawkids_dict )
        if not self._col_is_empty( row['Diagnostic Notes'] ):                   self._issues_appending_helper( in_row=row, idx=idx, col_name='Diagnostic Notes', hawk_ids=surgeon_hawkids_dict )
        if not self._col_is_empty( row['Additional Comments'] ):                self._issues_appending_helper( in_row=row, idx=idx, col_name='Additional Comments', hawk_ids=surgeon_hawkids_dict )
        if self._col_is_empty( row['Skills Assessment Requested'] ):                                                                 
            self._df.at[idx, 'Skills Assessment Requested'] = 'unknown'
            self._log_issue( idx=idx, column='Skills Assessment Requested', message="'Skills Assessment Requested' is blank, converting to 'unknown'.", issue_type='warning' )
        if self._col_is_empty( row['Was Radiology Contacted'] ):                                                                     
            self._df.at[idx, 'Was Radiology Contacted'] = 'unknown'
            self._log_issue( idx=idx, column='Was Radiology Contacted', message="'Was Radiology Contacted' is blank, converting to 'unknown'.", issue_type='warning' )

    # --------------- Helpers ----------------------------------------------------------------
    def _col_is_empty( self, col_name: str )  -> bool:      return col_name in ['', ' ', None]

    def _revise_string( self, in_str: str ) -> str:         return re.sub( r'\'\s', '\'', re.sub(r'\s\'', '\'', in_str ) )

    def _validate_and_format_dict_string( self, input_string: str ) -> Tuple[str, list]:
      # Strip all apostrophes and quotation marks from the string
        input_string = input_string.replace("'", '').replace('"', '')

        # Check that the string begins with '{' and ends with '}'
        notices = []
        if not input_string.startswith('{') or not input_string.endswith('}'):
            notices.append("The input string must begin with '{' and end with '}'.")

        # Check for appropriate use of semicolons and colons
        if not re.match(r'^\{(?:\s*[^{}]+:\s*[^{}]+(?:;\s*[^{}]+:\s*[^{}]+)*\s*)?\}$', input_string):
            notices.append("The input string must use semicolons and colons appropriately.")

        # Count the number of colons and semicolons (-1) in the string, they should be equal.
        num_colons, num_semicolons = input_string.count(':'), input_string.count(';')
        if num_colons != ( num_semicolons + 1 ):
            notices.append(f"The number of colons ({num_colons}) do not equal the number of semicolons+1 ({num_semicolons+1}), indicating invalid key:value; formatting.")

        # Add quotes around keys and values if they are not already quoted
        def add_quotes(match):
            key, value = match.groups()
            key, value = key.strip(), value.strip()
            if not (key.startswith('"') and key.endswith('"')):
                key = f'"{key}"'
            if not (value.startswith('"') and value.endswith('"')):
                value = f'"{value}"'
            return f'{key}: {value}'
        return re.sub(r'(\b\w+\b)\s*:\s*([^;{}]+)', add_quotes, input_string).replace(';', ','), notices
     
    def _issues_appending_helper( self, in_row: pd.Series, idx: Hashable, col_name: str, hawk_ids: Dict[str, str] ) -> None:
        self._df.at[idx, col_name], issues = self.replace_hawk_ids_with_encodings( in_str=in_row[col_name], hawk_ids=hawk_ids, original_col_name=col_name )
        for iss in issues:
            self._log_issue( idx=idx, column=col_name, message=iss, issue_type='warning' )

    def replace_hawk_ids_with_encodings( self, in_str: str, hawk_ids: Dict[str, str], original_col_name: str ) -> Tuple[str, List[str]]:
        """ Replace all instances of HawkIDs in a string with their respective encoding. """
        issues = []
        for k in hawk_ids:
            if k in in_str:
                in_str = in_str.replace( k, 'HAWKID='+hawk_ids[k] )
                issues.append( f"HawkID '{k}' was found unencoded within '{original_col_name}', output will automatically encode this." )    
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

    def print_errors_list( self ) -> str:
        '''Walk through each error in the table, printing out a new line in the following format: row # -- column name: error message'''
        error_str, ind = "", 2 # Start at 2 to account for the header row
        num_errors_total, num_rows_w_errors = 0, 0
        for _, row in self._errors.iterrows():
            start = num_errors_total
            for col in self._errors.columns:
                if row[col]:
                    num_errors_total += 1
                    if isinstance( row[col], list ):
                        for item in row[col]:
                            error_str += f"Row {ind}\t{item}\n"
                    else:                                                           
                        error_str += f"Row {ind}\t{row[col]}\n"
            if num_errors_total > start:    num_rows_w_errors += 1
            ind += 1

        # Append to the top of the string a summary of the number of errors in total and a breakdown of the number of rows with an error.
        str_header = f"{'='*50}\nError Summary for '{self.ffn.name}'\n{'='*50}\n"
        error_str = str_header + f"Input # of Rows (with header): {len(self.df)+1}\nTotal # of Errors: {num_errors_total}\nRows w/ Errors: {num_rows_w_errors}/{len(self.df)}\n\n" + error_str
        return error_str
    
    def print_warnings_list( self ) -> str:
        '''Walk through each warning in the table, printing out a new line in the following format: row # -- column name: warning message'''
        warning_str, ind = "", 2 # Start at 2 to account for the header row
        num_warns_total, num_rows_w_warns = 0, 0
        for _, row in self._warnings.iterrows():
            start = num_warns_total
            for col in self._warnings.columns:
                if row[col]:
                    num_warns_total += 1
                    if isinstance( row[col], list ):
                        for item in row[col]:
                            warning_str += f"Row {ind}\t{item}\n"
                    else:
                        warning_str += f"Row {ind}\t{row[col]}\n"
            if num_warns_total > start:    num_rows_w_warns += 1
            ind += 1

        # Append to the top of the string a summary of the number of errors in total and a breakdown of the number of rows with an error.
        str_header = f"{'='*50}\nWarning Summary for '{self.ffn.name}'\n{'='*50}\n"
        warning_str = str_header + f"Input # of Rows (with header): {len(self.df)+1}\nTotal # of Warnings: {num_warns_total}\nRows w/ Warnings: {num_rows_w_warns}/{len(self.df)}\n\n" + warning_str
        return warning_str
    

    def _build_issue_details( self ) -> Tuple[List[Dict[str, Any]],List[int]]:
        failed_details, counts = [], [0, 0]  # counts[0] = total errors, counts[1] = total warnings
        ind = 2  # Start at 2 to account for the header row

        for idx, row in self._errors.iterrows():
            errors, warnings = [], []
            row_has_errors = False  # Track if the row has errors

            # Process errors
            for col in self._errors.columns:
                if row[col]:
                    counts[0] += 1  # Increment total errors
                    row_has_errors = True  # Mark that this row has errors
                    if isinstance(row[col], list):
                        errors.extend(row[col])
                    else:
                        errors.append(row[col])

            # Process warnings if the row exists in the warnings table
            if idx in self._warnings.index:
                warning_row = self._warnings.loc[idx] # type: ignore
                for col in self._warnings.columns:
                    if warning_row[col]:
                        counts[1] += 1  # Increment total warnings
                        if isinstance(warning_row[col], list):
                            warnings.extend(warning_row[col])
                        else:
                            warnings.append(warning_row[col])

            # Append details if there are errors or warnings
            if errors or warnings:
                failed_details.append({
                    'row': ind,  # Use the adjusted row index (starting at 2)
                    'errors': errors,
                    'warnings': warnings
                })

            # Increment the "rows with errors" count only if the row has errors
            if row_has_errors:
                counts[0] += 1

            ind += 1  # Increment the row index for the next iteration
        return failed_details, counts
    

    def generate_summary( self, write_to_file: Opt[bool] = False ) -> Tuple[str, Opt[bool], Opt[str], Opt[str], Opt[str]]:
        '''
        Generate a summary of the mass data upload process, including a detailed report of errors and warnings.
        The summary includes the filename, date, total rows processed, rows with fatal errors, total errors, rows with warnings, and total warnings.
        It also provides details for each failed row, including the row number, errors, and warnings.
        The summary can be written to a file if specified.
        Outputs include:
        - output: The summary string.
        - permission_to_upload: A boolean indicating whether the upload can proceed (True if no errors, False otherwise).
        - table_summary: A string containing the details of the inputted rows' errors and/or warnings.
        - errors: A string containing the error details.
        - warnings: A string containing the warning details.
        '''
        # Create the details for failed rows
        failed_details, counts = self._build_issue_details()
        failed_rows_details, num_failed = "", len( failed_details )
        for i, detail in enumerate( failed_details, 1 ):
            failed_rows_details += f"({i}/{num_failed}) Row {detail['row']+2}:\n"
            for error in detail['errors']:              failed_rows_details += f"! Error: {error}\n"
            for warning in detail.get( 'warnings', []): failed_rows_details += f"\t- Warning: {warning}\n"
            failed_rows_details += "\n"
        num_errors, rows_w_errors = 0, 0
        for item in failed_details:
            rows_w_errors += 1
            num_errors += len( item['errors'] )

        # Create the header
        header = textwrap.dedent( f"""\
        Summary of Mass Data Upload
        ===========================

        Filename: {self.ffn.name}
        Date: {datetime.now().strftime("%Y-%m-%d %H:%M")}

        Total Rows Processed:\t{len( self.df )}
        Rows with Fatal Errors:\t{num_errors}
        Total # of Errors:\t{num_errors}
        Rows with Warnings:\t{sum( 1 for item in failed_details if item['warnings'] )}
        Total # of Warnings:\t{counts[1]}

        ------------------------
        Details for Failed Rows:
        ------------------------
        *Row 1 corresponds to the excel file header containing the column names.

        """ )


        # Create the footer & Combine all parts
        output = header + failed_rows_details  + "\n===========================\nEnd of Summary\n"
        if write_to_file:
            with open( self._ffn.with_name( self._ffn.stem + '-summary.txt' ), 'w' ) as f:
                f.write( output )
        return output, num_errors == 0, self.print_rows(), self.print_errors_list(), self.print_warnings_list()
    

    def upload_sessions( self, config:ConfigTables, validated_login: XNATLogin, xnat_connection: XNATConnection, write_to_file: Opt[bool]=False, verbose: bool = True ) -> None:
        """ Upload the sessions to XNAT. """
        # Verify an open and valid XNAT connection is provided.
        assert xnat_connection.is_verified, "XNAT connection must be open and valid.\n\t--\tPlease check your connection and try again."

        # Verify the data is contains only valid rows that are ready for upload.
        out_str, permitted_to_upload, _, _, _ = self.generate_summary( write_to_file=True )
        assert permitted_to_upload, "Cannot upload sessions if there are any errors with the imported data\n\t--\tRemove or fix the problematic rows from the spreadsheet; see generate_summary()\nSummary is:\n{out_str}"
        assert 1==0, "This function is not yet implemented."
        
        # Change the column names of the dataframe such that spaces are replaced with \n
        df_copy = self.df.copy()
        df_copy.columns = [ col.replace( ' ', '\n' ) for col in df_copy.columns ]

        # Walk through each row, check what type of data it is, instantiate appropriately (eg rfSession, esvSession, etc), then write and publish.
        ind, failed_rows = 0, {}
        for idx, row in df_copy.iterrows():
            if ind > 0: break
            # Create the digital form for uploading.
            procedure_name = row['Procedure\nName']
            if 'ARTHROSCOPY' in procedure_name.upper(): # Get the procedure name, if it contains 'Arthroscopy' then use ESVSession, otherwise use RFSession.
                intake_form = ORDataIntakeForm( validated_login=validated_login, config=config, input_data=row, verbose=verbose )
                session = SourceESVSession( intake_form=intake_form, config=config  )
                session.write( config=config, verbose=True )
                session.write_publish_catalog_subroutine( config=config, xnat_connection=xnat_connection, validated_login=validated_login, verbose=True )
            else:
                print( f'!!!! Assuming that {procedure_name} is a trauma case -- attempting to upload as a SourceRFsession\n\t--\tIf this is incorrect, hit Ctrl-C now and consult the Data Librarian with this note.' )
                # session = SourceRFSession( xnat_connection=xnat_connection, config=config, row=row, verbose=True )

    def __str__( self ) -> str:                             return self.print_rows( rows='all' )
