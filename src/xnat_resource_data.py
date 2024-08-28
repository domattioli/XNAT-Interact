import os
import sys
from pathlib import Path, WindowsPath
import shutil

from typing import List, Dict, Any, Tuple, Optional as Opt, Union

import re
from datetime import datetime, date as dtdate, time as dttime
from dateutil import parser

import json
from collections import OrderedDict

from src.utilities import UIDandMetaInfo, MetaTables, USCentralDateTime, XNATLogin, USCentralDateTime

import pytz

import string

ordered_keys_of_intake_text_file = ['FORM_LAST_MODIFIED', 'OPERATION_DATE', 'SUBJECT_UID', 'FILER_HAWKID', 'FORM_AVAILABLE_FOR_PERFORMANCE', 'SCAN_QUALITY',
                                    'SURGICAL_PROCEDURE_INFO', 'SKILLS_ASSESSMENT_INFO', 'STORAGE_DEVICE_INFO', 'INFO_DERIVED_FROM_ORIGINAL_FILE_METADATA']

indent_str = f'\n\t\t-- '

class ResourceFile( UIDandMetaInfo ):
    """This can represent a resource at any level, e.g., project, subject, experiment, scan, etc."""
    def __init__( self, metatables: MetaTables, login: XNATLogin ):
        assert metatables.is_user_registered( login.validated_username ), f'User with HAWKID {login.validated_username} is not registered in the system!'
        super().__init__() # Call the __init__ method of the base class to create a uid for this instance
        

    def _init_all_fields( self )    -> None:        raise NotImplementedError( 'This method must be implemented in the subclass.' ) # This is a placeholder for the subclass to implement
        
    def __str__( self )             -> str:         return '-----'*5 + f'\nOR Data Intake Form\n' + '-----'*5 + '\n\n'

    def _construct_dict_of_ortho_procedure_names( self, metatables: MetaTables ) -> Dict[str, str]:
        """This method is intended to be used to create a dictionary of all ortho procedure names that are in the metatables."""
        # Separate items into arthroscopy and trauma items
        items = metatables.list_of_all_items_in_table( table_name='Groups' )
        arthroscopy_items = [item for item in items if 'arthroscopy' in item.lower()]
        trauma_items = [item for item in items if 'arthroscopy' not in item.lower()]

        # Create keys for arthroscopy items and for trauma items, combine them into a dictionary
        arthroscopy_keys    = [f"1{letter.upper()}" for letter      in string.ascii_lowercase[:len( arthroscopy_items )]]
        other_keys          = [f"2{letter.upper()}" for letter      in string.ascii_lowercase[:len( trauma_items )]]
        arthroscopy_dict    = {key: item            for key, item   in zip( arthroscopy_keys, arthroscopy_items )}
        other_dict          = {key: item            for key, item   in zip( other_keys, trauma_items )}
        return {**arthroscopy_dict, **other_dict}
        

class InvalidInputError( Exception ):
    """Exception raised for errors in the input after multiple attempts."""
    def __init__(self, message="Invalid input despite multiple attempts to correct it. Exiting..."):
        self.message = message
        super().__init__(self.message)


class ORDataIntakeForm( ResourceFile ):
    """
    This class is for describing a subject-experiment intake, i.e., a subject's source data experiment.
    It is intended to be used for the OR Data Intake Form, which is a paper form that is filled out by someone immediately after a surgical procedure.
    The form is then used to create a text file that is used to populate the XNAT database with the relevant information.
    The UID generated represents the subject and the source data experiment.
    """
    def __init__( self, metatables: MetaTables, login: XNATLogin, parent_folder: Opt[str]=None, verbose: Opt[bool]=False, write_tmp_file: Opt[bool]=True ):
        super().__init__( metatables=metatables, login=login ) # Call the __init__ method of the base class -- bug:? goes all the way to our utility class and generates a uid.

        # Init dict (and future json-formatted text file) with required keys.
        self._init_all_fields( metatables=metatables )
        
        # Either read in the inputted text file and distribute that data, or prompt the user for the data.
        if parent_folder:
            self._read_from_file( parent_folder, verbose=verbose )
        else:
            if verbose:     print( f'\n...Processing OR Data Intake Form...' )
            self._prompt_user_for_filer_name_and_operation_date( metatables=metatables )
            self._prompt_user_for_scan_quality()
            self._prompt_user_for_surgical_procedure_info( metatables=metatables )
            self._prompt_user_for_skills_assessment_info( metatables=metatables )
            self._prompt_user_for_storage_device_info()

        # Need to identify the save-to location for the json file; if successfully read from file, use that, else, use the generated uid.
        self._saved_ffn = metatables.tmp_data_dir / Path( self.uid ) / self.filename
        if not os.path.exists( self.saved_ffn.parent ):     os.makedirs( self.saved_ffn.parent )
        if write_tmp_file:                                  self.construct_digital_file( verbose=verbose )
        # self._create_text_file_reconstruction( verbose=verbose ) # commenting out bc we want it saved to a temp folder corresponding to this subject


    def _read_from_file( self, parent_folder: str, verbose: Opt[bool]=False ) -> None:
        ffn = os.path.join( parent_folder, self.filename_str )
        if verbose:     print( f'\n\t...Initializing OR Intake From from "{ffn}"...' )
        with open( ffn, 'r', encoding='cp1252' ) as jf:     self._running_text_file = json.loads( jf.read() )

        # Minimally Required information (if paper form was available when processed)
        self._uid = self.running_text_file['SUBJECT_UID'] # Overwrites generated uid in base class
        self._filer_name = self.running_text_file['FILER_HAWKID']
        self._form_available = self.running_text_file['FORM_AVAILABLE_FOR_PERFORMANCE']
        self._operation_date = self.running_text_file['OPERATION_DATE']
        self._scan_quality = self.running_text_file['SCAN_QUALITY']
        self._institution_name = self.running_text_file['SURGICAL_PROCEDURE_INFO']['INSTITUTION_NAME']
        self._ortho_procedure_type = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PROCEDURE_TYPE']
        self._ortho_procedure_name = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PROCEDURE_NAME']
        self._epic_start_time = self.running_text_file['SURGICAL_PROCEDURE_INFO']['EPIC_START_TIME']
        self._side_of_patient_body = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PATIENT_SIDE']
        self._storage_device_name_and_type = self.running_text_file['STORAGE_DEVICE_INFO']['STORAGE_DEVICE_NAME_AND_TYPE']
        self._radiology_contact_date = self.running_text_file['STORAGE_DEVICE_INFO']['RADIOLOGY_CONTACT_DATE']
        self._radiology_contact_time = self.running_text_file['STORAGE_DEVICE_INFO']['RADIOLOGY_CONTACT_TIME']
        self._relevant_folder = Path( self.running_text_file['STORAGE_DEVICE_INFO']['RELEVANT_FOLDER'] )
        # self._saved_ffn = Path( ffn )

        try: # Optional fields that may not be present in the inputted form (if the filer did not have this info when originally creating the digitized version)
            self._epic_end_time = self.running_text_file['SURGICAL_PROCEDURE_INFO']['EPIC_END_TIME']
            self._OR_location = self.running_text_file['SURGICAL_PROCEDURE_INFO']['OR_LOCATION']
            self._supervising_surgeon_hawk_id = self.running_text_file['SURGICAL_PROCEDURE_INFO']['SUPERVISING_SURGEON_UID']
            self._supervising_surgeon_presence = self.running_text_file['SURGICAL_PROCEDURE_INFO']['SUPERVISING_SURGEON_PRESENCE']
            self._performing_surgeon_hawk_id = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMING_SURGEON_UID']
            self._performer_year_in_residency = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_YEAR_IN_RESIDENCY']
            self._performer_was_assisted = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_WAS_ASSISTED']
            self._performer_num_of_similar_logged_cases = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_NUM_OF_SIMILAR_LOGGED_CASES']
            self._performance_enumerated_task_performer = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMANCE_ENUMERATED_TASK_PER_PERFORMER']
            self._list_unusual_features_of_performance = self.running_text_file['SURGICAL_PROCEDURE_INFO']['LIST_UNUSUAL_FEATURES']
            self._diagnostic_notes = self.running_text_file['SURGICAL_PROCEDURE_INFO']['DIAGNOSTIC_NOTES']
            self._misc_surgical_performance_comments = self.running_text_file['SURGICAL_PROCEDURE_INFO']['MISC_PROCEDURE_COMMENTS']
            self._assessment_title = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSMENT_TITLE']
            self._assessor_hawk_id = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSOR_UID']
            self._assessment_details = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSMENT_DETAILS']
        except Exception as e:
            if verbose:     print( f'\t--- Only minimally required fields were found in the inputted form.' )


    def _init_all_fields( self, metatables: MetaTables ) -> None:
        # Required inputs -- user must at the very least acknowledge that they do not have the information
        self._filer_name, self._operation_date, self._form_available = '', '', False
        self._institution_name, self._ortho_procedure_type, self._ortho_procedure_namem, self._epic_start_time = '', '', '', ''
        self._storage_device_name_and_type, self._radiology_contact_date, self._radiology_contact_time, self._relevant_folder = None, None, None, Path('') # While required, it would be good to show a "null" value in the json file if this is truly unknown.
        self._scan_quality = ''

        # Information that may not be knowable if the data is older, so we will allow for None values
        self._epic_end_time, self._side_of_patient_body, self._OR_location = None, None, None
        self._supervising_surgeon_hawk_id, self._supervising_surgeon_presence, self._performing_surgeon_hawk_id, self._performer_year_in_residency = None, None, None, None
        self._performer_was_assisted, self._performer_num_of_similar_logged_cases, self._performance_enumerated_task_performer = None, None, None
        self._list_unusual_features_of_performance, self._diagnostic_notes, self._misc_surgical_performance_comments = None, None, None
        self._assessment_title, self._assessor_hawk_id, self._assessment_details = None, None, None

        # Create a dict to represent all imported ortho procedure names
        acceptable_ortho_procedure_names = self._construct_dict_of_ortho_procedure_names( metatables=metatables )
        self._running_text_file = OrderedDict( ( k, acceptable_ortho_procedure_names[k]) for k in ordered_keys_of_intake_text_file if k in acceptable_ortho_procedure_names )
        self._running_text_file['FORM_LAST_MODIFIED'] = datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()


    @staticmethod
    def prompt_until_valid_answer_given( selection_name: str, acceptable_options: list, max_num_attempts: int=2 ) -> str:
        num_attempts = 0
        while True and num_attempts < max_num_attempts:
            user_input, num_attempts = input( f'\tAnswer:\t' ), num_attempts + 1
            if user_input.upper() in acceptable_options:    return user_input.upper()
            else:                                           print( f'\t--- Invalid entry for {selection_name}! Please enter one of the options listed above.' )
        raise InvalidInputError( f'Failed to provide a valid entry for {selection_name} after {max_num_attempts} attempts.' )
    

    def _prompt_user_for_filer_name_and_operation_date( self, metatables: MetaTables ) -> None:
        acceptable_registered_users_options_encoded = {str(i+1): reg_user for i, reg_user in enumerate( metatables.list_of_all_items_in_table( table_name='REGISTERED_USERS' ) )}
        options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_registered_users_options_encoded.items()] )
        print( f'\t(1/34)\tWhat is your HAWKID (the Form Filer)?{indent_str}Please select from the following registered users:\n{options_str}' )
        filer_hawkid_key = self.prompt_until_valid_answer_given( 'Institution Name', acceptable_options=list( acceptable_registered_users_options_encoded ) )
        self._filer_name = acceptable_registered_users_options_encoded[filer_hawkid_key].upper()

        print( f'\n\t(2/34)\tDo you have a *PAPER* Intake Form available filled-out for this procedure?{indent_str}Please enter "1" for Yes or "2" for No' )
        form_available = self.prompt_until_valid_answer_given( 'Form Availability', acceptable_options=['1', '2'] ) # to-do: Automate acceptable_options based on the type of input expected bc we may change the metatables values for this and then these prompts wont reflect those changes.

        num_attempts, max_num_attempts = 0, 2
        while True and num_attempts < max_num_attempts:
            try:
                date_str = input( '\n\t(3/34)\tPlease enter the Operation Date (YYYY-MM-DD):\t' )
                self._operation_date = parser.parse( date_str ).date().strftime('%Y-%m-%d')
                break
            except KeyboardInterrupt:
                print( f'\n\n...User cancelled task via Ctrl+C...' )
                sys.exit( 0 )
            except:
                num_attempts += 1
                print( "Invalid date format. Please enter the date in YYYY-MM-DD format." )
        if num_attempts == max_num_attempts:
            raise ValueError( f"Maximum number of attempts, {max_num_attempts}, reached. Exiting." )

        if form_available == '1':       self._form_available = True
        elif form_available == '2':     self._form_available = False
        
        self._running_text_file['FILER_HAWKID'] = str( self.filer_name )
        self._running_text_file['FORM_AVAILABLE_FOR_PERFORMANCE'] = str( self.form_is_available )
        self._running_text_file['OPERATION_DATE'] = str( self.operation_date )
        self._running_text_file['SUBJECT_UID'] = str( self.uid )
    
    
    def get_time_input( self, prompt ) -> str:
        for _ in range(2):  # Gives the user 1 opportunity to try again
            # Replace hyphens and spaces with colons as applicable
            user_input = input( prompt )
            user_input = re.sub( r'[-\s]', ':', user_input )
            
            try: return parser.parse( user_input ).time().strftime( '%H:%M' )
            except ValueError: print( f"\t\t --- Invalid time provided; you entered: {user_input}{indent_str}Please use the HH:MM format.\n" )
        raise ValueError( "Failed to provide a valid time after 2 attempts." )


    def _prompt_user_for_scan_quality( self ):
        print( f'\n\n--- Quality/Usability of the OR Image Data ---' )
        print( f'\t(4a/34)\tDo you know the quality of the OR image data?{indent_str}-- Please enter "1" for Yes or "2" for No.' )
        #"1" for Usable, "2" for Un-usable, "3" for Questionable, or "4" for Unknown.' )
        known_scan_quality = self.prompt_until_valid_answer_given( 'Quality of the Scan', acceptable_options=['1', '2'] )
        if known_scan_quality == '1':
            print( f'\n\t(4b/34)\tPlease enter "1" for Usable, "2" for Un-usable, or "3" for Questionable.' )
            print( f'\t\t--- Usable:\t\tThe image data is of sufficient quality to be used for research purposes.' )
            print( f'\t\t--- Un-usable:\tThe image data is of insufficient quality to be used for research purposes.' )
            print( f'\t\t--- Questionable:\tThe image data is of questionable quality and may or may not be usable for research purposes.' )
            scan_quality = self.prompt_until_valid_answer_given( 'Quality of the Image/Video Data', acceptable_options=['1', '2', '3'] )
            if scan_quality == '1':     self._scan_quality = 'usable'
            elif scan_quality == '2':   self._scan_quality = 'unusable'
            elif scan_quality == '3':   self._scan_quality = 'questionable'
        else: scan_quality = '' # Unknown, but xnat doesnt except that as an input.
        self._running_text_file['SCAN_QUALITY'] = self.scan_quality # type: ignore -- not sure why this is giving a type error. runs fine in spite of it.


    def _prompt_user_for_surgical_procedure_info( self, metatables: MetaTables ): # Make sure fields that might be stored in the metatables are all completely capitalized
        print( f'\n--- Surgical Procedure Information ---' )
        local_dict = {}

        #Encode the options for acceptable institions as a list of integer strings
        acceptable_institution_options_encoded = {str(i+1): institution for i, institution in enumerate( metatables.list_of_all_items_in_table( table_name='ACQUISITION_SITES' ) )}
        options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_institution_options_encoded.items()] )
        print( f'\t(5/34)\tAt which institution did this performance occur?{indent_str}Please select from the following options:\n{options_str}' )
        institution_name_key = self.prompt_until_valid_answer_given( 'Institution Name', acceptable_options=list( acceptable_institution_options_encoded ) )
        self._institution_name = acceptable_institution_options_encoded[institution_name_key]
        local_dict['INSTITUTION_NAME'] = self.institution_name

        print( f'\n\t(6/34)\tWhat was the Ortho Procedure Type?{indent_str}Please enter "1" for Arthro or "2" for Trauma.' )
        ortho_procedure_type = self.prompt_until_valid_answer_given( 'Type of Orthro Procedure', acceptable_options=['1', '2'] )
        if ortho_procedure_type == '1':     self._ortho_procedure_type = 'Arthroscopy'.upper()
        elif ortho_procedure_type == '2':   self._ortho_procedure_type = 'Trauma'.upper()
        local_dict['PROCEDURE_TYPE'] = self.ortho_procedure_type

        # Given the ortho procedure type, select the keys from the acceptable_ortho_procedure_names dictionary that begin with the ortho_procedure_type
        acceptable_ortho_procedure_names = self._construct_dict_of_ortho_procedure_names( metatables=metatables )
        acceptable_ortho_procedure_name_options_encoded = {key: value for key, value in acceptable_ortho_procedure_names.items() if key.startswith( ortho_procedure_type )}
        options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_ortho_procedure_name_options_encoded.items()] )
        print( f'\n\t(7/34)\tWhat is the name of Ortho Procedure?{indent_str}Please select from the following options:\n{options_str}' )
        procedure_name_key = self.prompt_until_valid_answer_given( 'Ortho Procedure Name', acceptable_options = list( acceptable_ortho_procedure_name_options_encoded ) )
        self._ortho_procedure_name = acceptable_ortho_procedure_name_options_encoded[procedure_name_key]
        local_dict['PROCEDURE_NAME'] = str( self.ortho_procedure_name ) # type: ignore

        if self.form_is_available:
            print( f'\n\t(8a/34)\tDo You know the Epic End Time?{indent_str}Please enter "1" for Yes of "2" for No.' )
            known_end_time = self.prompt_until_valid_answer_given( 'Known EPIC End Time', acceptable_options=['1', '2'] )

            valid_times, num_attempts, max_attempts = False, 0, 2
            while not valid_times and num_attempts < max_attempts:
                epic_start_time = self.get_time_input( '\n\t(8b/34)\tEpic Start Time (HH:MM in 24hr format): ' )

                if known_end_time == '1':
                    epic_end_time = self.get_time_input( '\n\t(9/34)\tEpic End Time (HH:MM in 24hr format): ' )
                    if epic_start_time and epic_end_time:  # If both are not None
                        if epic_start_time < epic_end_time: valid_times = True
                        else:
                            print( f'\tAttempt {num_attempts + 1} failed:\n\t\tStart Time must be before End Time. You entered {epic_start_time} and {epic_end_time}.' )
                            num_attempts += 1
                    else:
                        print( f'\tAttempt {num_attempts + 1} failed:\n\t\tInvalid times provided.{indent_str}Please use the HH:MM format.' )
                        num_attempts += 1
                else:
                    valid_times, num_attempts = True, max_attempts # Exit the while loop
                    epic_end_time = None
            if not valid_times:
                print( '\n\t--- Failed to provide valid times after 2 attempts. Keeping last-entered start time and ignoring end-time (if this is unacceptable, press Ctrl-C to restart)!' )
                self._epic_start_time, self._epic_end_time = epic_start_time, None
            else:   self._epic_start_time, self._epic_end_time = epic_start_time, epic_end_time
            local_dict['EPIC_START_TIME'], local_dict['EPIC_END_TIME'] = self.epic_start_time, self.epic_end_time
        else:
            print( f'\n\t(10a/34) Do you know the Operation or EPIC Start Time?{indent_str}Please enter "1" for Yes or "2" for No.' )
            known_start_time = self.prompt_until_valid_answer_given( 'Known EPIC Start Time', acceptable_options = list( ['1', '2'] ) )
            if known_start_time == '1':
                epic_start_time = self.get_time_input( f'\t(10b/34) Known Epic Start Time (HH:MM in 24hr format):\t' )
            else: epic_start_time = datetime.now().replace( hour=0, minute=0, second=0, microsecond=0 ).strftime( '%H:%M:%S' )  # Assign midnight-today as the default start time
            local_dict['EPIC_START_TIME'] = epic_start_time
            self._epic_start_time = epic_start_time


        print( f'\n\t(11a/34) Do you know which Side of Patient\'s Body that the surgery was performed?{indent_str}Please enter "1" for Yes or "2" for No.' )
        #Please enter "1" for Right, "2" for Left, "3" for Unknown, or "4" for N/A or not relevant.' )
        known_patient_side = self.prompt_until_valid_answer_given( 'Known Side of Patient\'s Body', acceptable_options=['1', '2'] )
        if known_patient_side == '1':
            print( f'\n\t(11b/34) Which side of the patient\'s body was the surgery performed on?{indent_str}Please enter "1" for Right or "2" for Left.' )
            patient_side = self.prompt_until_valid_answer_given( 'Side of Patient\'s Body', acceptable_options=['1', '2'] )
            if patient_side == '1':     self._side_of_patient_body = 'Right'.upper()
            elif patient_side == '2':   self._side_of_patient_body = 'Left'.upper()
        else:                           self._side_of_patient_body = 'Unknown'.upper()
        local_dict['PATIENT_SIDE'] = self.side_of_patient_body

        print( f'\n\t(12/34) Operating Room Name/Location (press Enter if Unknown).' )
        OR_location = input( '\n\tAnswer:\t' ).upper().replace( '"', "'" )
        if len( OR_location ) == 0: self._OR_location = 'Unknown'.upper()
        else:                       self._OR_location = OR_location
        local_dict['OR_LOCATION'] = self.OR_location

        print( f'\n\t(13a/34) Do you know the Supervising Surgeon\'s HawkID?{indent_str}Please enter "1" for Yes or "2" for No.' )
        known_supervising_hawkid = self.prompt_until_valid_answer_given( 'Known Supervising Surgeon HawkID', acceptable_options=['1', '2'] )
        if known_supervising_hawkid == '1':
            # create an encoding of the acceptable options for the supervising surgeon
            acceptable_supervising_surgeon_options_encoded = {str(i+1): surgeon for i, surgeon in enumerate( metatables.list_of_all_items_in_table( table_name='Surgeons' ) )}
            options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_supervising_surgeon_options_encoded.items()] )

            print( f'\n\t(13b/34) Supervising Surgeon HawkID{indent_str}Please select from the following list:\n{options_str}')
            supervising_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'Supervising Surgeon\'s HAWKID', acceptable_options = list( acceptable_supervising_surgeon_options_encoded ) )
            supervising_surgeon_hawk_id = acceptable_supervising_surgeon_options_encoded[supervising_surgeon_hawk_id]
        else:   supervising_surgeon_hawk_id = 'Unknown'.upper()
        self._supervising_surgeon_hawk_id = metatables.get_uid( 'Surgeons', supervising_surgeon_hawk_id )

        print( f'\n\t(14a/34) Do you know the Supervising Surgeon\'s Presence?{indent_str}Please enter "1" for Yes or "2" for No.' )
        known_supervising_surgeon_presence = self.prompt_until_valid_answer_given( 'Known Supervising Surgeon Presence', acceptable_options=['1', '2'] )
        if known_supervising_surgeon_presence == '1':
            print( f'\n\t(14b/34) To indicate the supervising surgeon\'s presence{indent_str}Please enter "1" for Present or "2" for Retrospective Review.' )
            supervising_surgeon_presence = self.prompt_until_valid_answer_given( 'Supervising Surgeon Presence', acceptable_options=['1', '2'] )
            if supervising_surgeon_presence == '1':     supervising_surgeon_presence = 'Present'.upper()
            elif supervising_surgeon_presence == '2':   supervising_surgeon_presence = 'Retrospective Review'.upper()
        else:   supervising_surgeon_presence = 'Unknown'.upper()
        self._supervising_surgeon_presence = supervising_surgeon_presence
        local_dict['SUPERVISING_SURGEON_UID'], local_dict['SUPERVISING_SURGEON_PRESENCE'] = self.supervising_surgeon_hawk_id, self.supervising_surgeon_presence

        print( f'\n\t(15a/34) Do you know the Performing Surgeon\'s HawkID?{indent_str}Please enter "1" for Yes or "2" for No.' )
        known_performer_hawk_id = self.prompt_until_valid_answer_given( 'Known Performing Surgeon HawkID', acceptable_options=['1', '2'] )
        if known_performer_hawk_id == '1':
            # create an encoding of the acceptable options for the performing surgeon
            acceptable_performing_surgeon_options_encoded = {str(i+1): surgeon for i, surgeon in enumerate( metatables.list_of_all_items_in_table( table_name='Surgeons' ) )}
            options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_performing_surgeon_options_encoded.items()] )
            
            print( f'\n\t(15b/34) Performing Surgeon HawkID{indent_str}Please select the from the following list:\n{options_str}' )
            performing_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'Performing Surgeon\'s HAWKID', acceptable_options=list( acceptable_performing_surgeon_options_encoded ) )
            performing_surgeon_hawk_id = acceptable_performing_surgeon_options_encoded[performing_surgeon_hawk_id]
        else:   performing_surgeon_hawk_id = 'Unknown'.upper()

        print( f'\n\t(16a/34) Do you know the Performing Surgeon\'s # of Years in Residency/Experience?{indent_str}Please enter "1" for Yes or "2" for No.' )
        known_years = self.prompt_until_valid_answer_given( 'Known Performing Surgeon\'s # of Years in Residency/Experience', acceptable_options=['1','2'] )
        if known_years == '1':
            performer_year_in_residency = self._prompt_user_for_integer_input( '\n\t(16b/34) Performing Surgeon\'s # of Years in Residency/Experience:\t', acceptable_range=( 1, 50 ) )
        else:                   performer_year_in_residency = 'Unknown'.upper()
        
        print( f'\n\t(17a/34) Do you know how many Similar Prior Cases have been logged for the performing surgeon?{indent_str}Please enter "1" for Yes or "2" for No.')
        known_number_of_similar_logged_cases = self.prompt_until_valid_answer_given( 'Known # of Similar Cases Logged', acceptable_options=['1', '2'] )
        if known_number_of_similar_logged_cases == '1':
            self._performer_num_of_similar_logged_cases     = self._prompt_user_for_integer_input( '\n\t(17b/34) Performing Surgeon\'s # of Similar Cases Logged (if none, enter "0").', acceptable_range=( 0, 500 ) )
        else: self._performer_num_of_similar_logged_cases   = None
        self._performing_surgeon_hawk_id, self._performer_year_in_residency = metatables.get_uid( 'Surgeons', performing_surgeon_hawk_id ), performer_year_in_residency
        local_dict['PERFORMING_SURGEON_UID'], local_dict['PERFORMER_YEAR_IN_RESIDENCY/EXPERIENCE'], local_dict['PERFORMER_NUM_OF_SIMILAR_LOGGED_CASES'] = self.performing_surgeon_hawk_id, self.performer_year_in_residency, self.performer_num_of_similar_logged_cases

        print( f'\n\t(18/34) Was the Performing Surgeon Assisted?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown.' )
        performer_was_assisted = self.prompt_until_valid_answer_given( 'Performing Surgeon Assistance', acceptable_options=['1', '2', '3'] )
        if performer_was_assisted == '1':
            self._performer_was_assisted,   dict_performance_enumerated_tasks       = True, self._prompt_user_for_n_surgical_tasks_and_hawkids( metatables=metatables ) # Prompt 19
            for key, value in dict_performance_enumerated_tasks.items():    # If any of the values in the dict are empty, replace them with None
                if len( value ) == 0:       dict_performance_enumerated_tasks[key]  = None
            self._performance_enumerated_task_performer = dict_performance_enumerated_tasks
        elif performer_was_assisted == '2': self._performer_was_assisted            = False
        else:                               self._performer_was_assisted, self._performance_enumerated_task_performer = 'Unknown'.upper(), {'Unknown'.upper()}
        local_dict['PERFORMER_WAS_ASSISTED'], local_dict['PERFORMANCE_ENUMERATED_TASK_PER_PERFORMER'] = self.performer_was_assisted, self.performance_enumerated_task_performer

        print( f'\n\t(20/34) Were there any unusual features of the performance?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown.')
        any_unusual_features_of_performance = self.prompt_until_valid_answer_given( 'Unusual Features of Performance', acceptable_options=['1', '2', '3'] )
        if any_unusual_features_of_performance == '1':
            list_of_performance_features = input( f'\n\t(21/34) Please detail any/all unusual features of the performance:\n\tAnswer: ' ).replace( '"', "'" )
            if len( list_of_performance_features ) > 0:     self._list_unusual_features_of_performance = list_of_performance_features
        elif any_unusual_features_of_performance == '2':    self._list_unusual_features_of_performance = None
        else:                                               self._list_unusual_features_of_performance = 'Unknown'.upper()
        local_dict['LIST_UNUSUAL_FEATURES'] = self.list_unusual_features_of_performance

        print( f'\n\t(22/34) Were there any diagnostic notes about the surgical procedure?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown.')
        any_diagnostic_notes = self.prompt_until_valid_answer_given( 'Performing Surgeon Assistance', acceptable_options=['1', '2', '3'] )
        # if self.ortho_procedure_type == 'Arthroscopy' or ortho_procedure_type == '2':
        if any_diagnostic_notes == '1':
            diagnostic_notes = input( f'\n\t(23/34) Please enter any diagnostic notes about the surgical procedure:\n\tAnswer: ' ).replace( '"', "'" )
            if len( diagnostic_notes ) > 0:                 self._diagnostic_notes = diagnostic_notes
        elif any_diagnostic_notes == '2':                   self._diagnostic_notes = None
        else:                                               self._diagnostic_notes = 'Unknown'.upper()
        local_dict['DIAGNOSTIC_NOTES'] = self.diagnostic_notes

        print( f'\n\t(24/34) Do you have any additional comments or notes regarding BMI, pre-existing conditions, etc.?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown' )
        any_misc_comments = self.prompt_until_valid_answer_given( ' Miscellaneous Procedure Comments', acceptable_options=['1', '2', '3'])
        if any_misc_comments == '1':
            misc_comments = input( f'\n\t(25/34) Please enter any additional comments or notes:\n\tAnswer: ' ).replace( '"', "'" )
            if len( misc_comments ) > 0:                    self._misc_surgical_performance_comments = misc_comments
        elif any_diagnostic_notes == '2':                   self._misc_surgical_performance_comments = None
        else:                                               self._misc_surgical_performance_comments = 'Unknown'.upper()
        local_dict['MISC_PROCEDURE_COMMENTS'] = self.misc_surgical_performance_comments
        
        # Need to save info to the running text file regardless of if the form is available
        self._running_text_file['SURGICAL_PROCEDURE_INFO'] = local_dict # type: ignore
    
    def _prompt_user_for_integer_input( self, prompt: str, acceptable_range: Tuple[int, int], max_num_attempts: int=3 ) -> int:
        num_attempts = 0
        help_str = f'\tPlease enter an integer between {acceptable_range[0]} and {acceptable_range[1]}.'
        while num_attempts < max_num_attempts:
            user_input, num_attempts = input( f'\t{prompt}\n{help_str}\n\tAnswer (Attempt {num_attempts+1}/3): ' ), num_attempts + 1
            try:
                user_input = int( user_input )
                if acceptable_range[0] <= user_input <= acceptable_range[1]: return user_input
                else: print( f'\t--- Invalid entry! Please enter an integer between {acceptable_range[0]} and {acceptable_range[1]}.' )
            except ValueError: print( f'\t--- Invalid entry! Please enter an integer between {acceptable_range[0]} and {acceptable_range[1]}.' )
        raise ValueError( f'Failed to provide a valid integer input after {num_attempts} attempts.' )

    def _prompt_user_for_n_surgical_tasks_and_hawkids( self, metatables: MetaTables, max_num_attempts: int=3 ) -> dict:
        # Extract answer from user for the number of participating surgeons
        print( f'\n\t(19a/34) How many surgeons participated in the procedure?{indent_str}Please enter an integer (must be non-zero and positive).' )
        num_tasks = self._prompt_user_for_integer_input( '\n(19a/34) # of Participating Surgeons', acceptable_range=( 1, 1000 ), max_num_attempts=max_num_attempts )

        # Extract the HAWKIDs for each of the participating surgeons and prompt the user with an unstructured opportunity to detail the tasks that they performed.
        acceptable_performing_surgeon_options_encoded = {str(i+1): surgeon for i, surgeon in enumerate( metatables.list_of_all_items_in_table( table_name='Surgeons' ) )}
        options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_performing_surgeon_options_encoded.items()] )
        print( f'\n\t(19b/34) To denote each of the participating surgeons, please select from the following list of HawkIDs:\n{options_str}\n')
        task_performers = {}
        for i in range( num_tasks ):
            if i == 0:      hawkid_encoding = input( f'\t\t1st Surgeon: ' )
            elif i == 1:    hawkid_encoding = input( f'\t\t2nd Surgeon: ' )
            elif i == 2:    hawkid_encoding = input( f'\t\t3rd Surgeon: ' )
            else:           hawkid_encoding = input( f'\t\t{i+1}th Surgeon: ' )
            hawkid = acceptable_performing_surgeon_options_encoded[hawkid_encoding]
            task_performers[metatables.get_uid( table_name='SURGEONS', item_name=hawkid )] = input( f"\t\t\tPlease detail the task(s) performed by '{hawkid_encoding}'', i.e., {hawkid.upper()}: " ).replace( '"', "'" )
        return task_performers


    def _prompt_user_for_skills_assessment_info( self, metatables: MetaTables ):
        print( f'\n\n--- Skills Assessment Information ---' )
    
        print( f'\t(26a/34) Was a Skills Assessment requested for this procedure?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown.')
        assessment_requested = self.prompt_until_valid_answer_given( 'Skills Assessment Request', acceptable_options=['1', '2', '3'] )
        if assessment_requested == '1':
            assessment_requested, assessment_title = True, input( '\n\t(26b/34) Please enter the full name of the requested assessment:\n\tAnswer: ' ).upper().replace( '"', "'" )

            print( f'\n\t(27a/34) Do you know the HAWKID of the assessor?{indent_str}Please enter "1" for Yes or "2" for No.' )
            assessor_known = self.prompt_until_valid_answer_given( 'Assessor HAWKID', acceptable_options=['1', '2'] )
            if assessor_known == '1':
                # create an encoding of the acceptable options for the assessor
                acceptable_assessor_options_encoded = {str(i+1): surgeon for i, surgeon in enumerate( metatables.list_of_all_items_in_table( table_name='Surgeons' ) )}
                options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_assessor_options_encoded.items()] )
                print( f'\n\t(27b/34) Assessing Surgeon\'s HawkID{indent_str}Please select the from the following list:\n{options_str}' )
                assessor_hawkid = self.prompt_until_valid_answer_given( 'Performing Surgeon\'s HAWKID', acceptable_options=list( acceptable_assessor_options_encoded ) )
                assessor_hawkid = acceptable_assessor_options_encoded[assessor_hawkid]
            else: assessor_hawkid = 'Unknown'.upper()
            
            print( f'\n\t(28/34) Do you have any additional details about the assessment (e.g., date of assessment, score, etc.)?{indent_str}Please enter "1" for Yes or "2" for No.')
            known_details = self.prompt_until_valid_answer_given( 'Additional Assessment Details', acceptable_options=['1', '2'])
            if known_details == '1':    self._assessment_details = input( '\n\t(29/34) Please enter any additional details about the assessment:\n\tAnswer: ' ).replace( '"', "'" )
            else:                       self._assessment_details = None
            self._assessment_title, self._assessor_hawk_id = assessment_title, assessor_hawkid
        elif assessment_requested == '2':
            assessment_requested, self._assessment_title, self._assessor_hawk_id, self._assessment_details = False, None, None, None
        else:   assessment_requested = None,
        self._running_text_file['SKILLS_ASSESSMENT_INFO'] = {   'ASSESSMENT_REQUESTED': assessment_requested, # type: ignore
                                                                'ASSESSMENT_TITLE': self.assessment_title,
                                                                'ASSESSOR_UID': self.assessor_hawk_id,
                                                                'ASSESSMENT_DETAILS': self.assessment_details}


    def _prompt_user_for_storage_device_info( self ):
        print( f'\n\n--- Storage Device Information ---' )
        
        self._storage_device_name_and_type = input( '\t(30/34) Please enter the name and type of the storage device:\n\tAnswer: ' ).replace( '"', "'" )

        while True:
            full_path_name = input( '\n\t(31/34) Please enter the full directory name of the *local* folder containing the case data:\n\tAnswer: ' )
            if os.path.exists( full_path_name ):    break
            else:                                   print( f'!!!!!Input directory path is not accessible on this system!!!!!\n\tPlease double-check the validity of that directory and try again.' )
        self._relevant_folder = Path( full_path_name )

        print( f'\n\t(32/34) Was radiology contacted for this procedure?{indent_str}Please enter "1" for Yes, "2" for No, or "3" for Unknown.' )
        radiology_contacted = self.prompt_until_valid_answer_given( 'Radiology Contact Information', acceptable_options=['1', '2', '3'] )
        if radiology_contacted == '1':
            radiology_contact_date = parser.parse( input( '\n\t(33/34) Radiology Contact Date (YYYY-MM-DD):\t' ) ).date().strftime( '%Y-%m-%d' )
            radiology_contact_time = self.get_time_input( '\n\t(34/34) Radiology Contact Time (HH:MM in 24hr format):\t' )
            self._radiology_contact_date,   self._radiology_contact_time = radiology_contact_date, radiology_contact_time
        elif radiology_contacted in ['2', '3']:    self._radiology_contact_date, self._radiology_contact_time = None, None
        else:   raise ValueError( f'Invalid entry for Radiology Contacted! Please only enter "1" for Yes or "2" for No. You entered {radiology_contacted}' )
        self._running_text_file['STORAGE_DEVICE_INFO'] = {  'STORAGE_DEVICE_NAME_AND_TYPE': self.name_of_storage_device, # type: ignore
                                                            'RADIOLOGY_CONTACT_DATE': self.radiology_contact_date,
                                                            'RADIOLOGY_CONTACT_TIME': self.radiology_contact_time,
                                                            'RELEVANT_FOLDER': self.relevant_folder}
        

    def construct_digital_file( self, verbose: Opt[bool]=False ) -> None:
        assert self._uid, 'UID must be set before calling the IntakeForm saved full file name.'
        self._running_text_file['FORM_LAST_MODIFIED'] = datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()
        json_str = json.dumps( self.running_text_file, indent=4, default=ORDataIntakeForm._custom_serializer )
        with open( self.saved_ffn, 'w' ) as f:
            f.write( json_str )
            if verbose:     print( f'\t-- SUCCESS -- OR Data Intake Form saved to:\t{self.saved_ffn}\n' )
        
        # Copy the file to the inputted parent folder of the data.
        dest_ffn = self.relevant_folder / self.filename
        shutil.copy( self.saved_ffn, dest_ffn )


    def push_to_xnat( self, subj_inst, verbose: Opt[bool] = False ):
        if verbose:     print( f'\t\t...Uploading resource files...' )
        with open( self.saved_ffn, 'r' ) as f:
            subj_inst.resource( 'INTAKE_FORM' ).file( self.filename_str ).insert( f.read(), content='TEXT', format='JSON', tags='DOC' ) # type: ignore


    @staticmethod
    def _custom_serializer( obj ) -> str:
        if isinstance( obj, WindowsPath ):  return str( obj )  # Convert WindowsPath to string
        elif isinstance( obj, set ):        return str( list( obj ) ) # Convert set to list    
        raise TypeError( f"Object of type {obj.__class__.__name__} is not JSON serializable ")


    @property
    def running_text_file( self )                       -> dict:                    return self._running_text_file
    @property
    def saved_ffn( self )                               -> Path:                    return self._saved_ffn
    @property
    def saved_ffn_str( self )                           -> str:                     return str( self.saved_ffn )
    @property
    def filename( self )                                -> Path:                    return Path( 'RECONSTRUCTED_OR_DATA_INTAKE_FORM.json' )
    @property
    def filename_str( self )                            -> str:                     return str( self.filename )
    @property
    def form_is_available( self )                       -> bool:                    return self._form_available
    @property
    def filer_name( self )                              -> str:                     return self._filer_name
    @property
    def operation_date( self )                          -> str:                     return self._operation_date
    @property
    def institution_name( self )                        -> str:                     return self._institution_name
    @property
    def acquisition_site( self )                        -> str:                     return self._institution_name
    @property
    def ortho_procedure_type( self )                    -> str:                     return self._ortho_procedure_type # Trauma or arthro
    @property
    def ortho_procedure_name( self )                    -> str:                     return self._ortho_procedure_name # type: ignore
    @property
    def group( self )                                   -> str:                     return self._ortho_procedure_name # type: ignore
    @property
    def scan_quality( self )                            -> str:                     return self._scan_quality
    @property
    def epic_start_time( self )                         -> str:                     return self._epic_start_time
    @property
    def datetime( self )                                -> USCentralDateTime:
        assert self.operation_date, 'Operation Date must be set before calling the IntakeForm datetime property.'
        assert self.epic_start_time, 'Epic Start Time must be set before calling the IntakeForm datetime property.'
        return USCentralDateTime( f'{self.operation_date} {self.epic_start_time}' )
    @property   
    def epic_end_time( self )                           -> Opt[str]:                return self._epic_end_time
    @property
    def side_of_patient_body( self )                    -> Opt[str]:                return self._side_of_patient_body
    @property
    def OR_location( self )                             -> Opt[str]:                return self._OR_location
    @property
    def supervising_surgeon_hawk_id( self )             -> Opt[str]:                return self._supervising_surgeon_hawk_id
    @property
    def supervising_surgeon_presence( self )            -> Opt[str]:                return self._supervising_surgeon_presence # Present, retrospective, other
    @property
    def performing_surgeon_hawk_id( self )              -> Opt[str]:                return self._performing_surgeon_hawk_id
    @property
    def performer_year_in_residency( self )             -> Opt[Union[int, str]]:    return self._performer_year_in_residency
    @property
    def performer_was_assisted( self )                  -> Opt[Union[bool, str]]:   return self._performer_was_assisted
    @property
    def performer_num_of_similar_logged_cases( self )   -> Opt[int]:                return self._performer_num_of_similar_logged_cases
    @property
    def performance_enumerated_task_performer( self )   -> Opt[Union[dict, set]]:   return self._performance_enumerated_task_performer
    @property
    def list_unusual_features_of_performance( self )    -> Opt[str]:                return self._list_unusual_features_of_performance
    @property
    def diagnostic_notes( self )                        -> Opt[str]:                return self._diagnostic_notes
    @property
    def misc_surgical_performance_comments( self )      -> Opt[str]:                return self._misc_surgical_performance_comments # body habitus, pre-existing conditions, specific technical struggles, damage to tissue, non-technical issues, anything that happened before/after the procedure began 
    
    @property
    def assessment_title( self )                        -> Opt[str]:                return self._assessment_title
    @property
    def assessor_hawk_id( self )                        -> Opt[str]:                return self._assessor_hawk_id
    @property
    def assessment_details( self )                      -> Opt[str]:                return self._assessment_details

    @property
    def name_of_storage_device( self )                  -> Opt[str]:                return self._storage_device_name_and_type
    @property
    def radiology_contact_date( self )                  -> Opt[str]:                return self._radiology_contact_date
    @property
    def radiology_contact_time( self )                  -> Opt[str]:                return self._radiology_contact_time
    @property
    def relevant_folder( self )                         -> Path:                    return self._relevant_folder
    

    def __str__( self ) -> str:
        if self.saved_ffn is not None:
            with open( self.saved_ffn, 'r' ) as f:  return ''.join('\t' + line for line in f)
        else:                                       return f'No file found at {self.saved_ffn_str}'