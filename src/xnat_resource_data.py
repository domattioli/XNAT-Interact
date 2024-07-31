import os
from pathlib import Path, WindowsPath

from typing import List, Dict, Any, Tuple, Optional as Opt, Union

from datetime import datetime, date as dtdate, time as dttime
from dateutil import parser

import json
from collections import OrderedDict

from src.utilities import UIDandMetaInfo, MetaTables, USCentralDateTime, XNATLogin, USCentralDateTime

import pytz

acceptable_ortho_procedure_names = {'1A': 'OPEN_REDUCTION_HIP_FRACTURE–DYNAMIC_HIP_SCREW',
                                    '1B': 'OPEN_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW',
                                    '1C': 'CLOSED_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW',
                                    '1D': 'PERCUTANEOUS_SACROLIAC_FIXATION',
                                    '1E': 'OPEN_AND_PERCUTANEOUS_PILON_FRACTURES',
                                    '1F': 'INTRAMEDULLARY_NAIL-CMN',
                                    '1G': 'INTRAMEDULLARY_NAIL-ANTEGRADE_FEMORAL',
                                    '1H': 'INTRAMEDULLARY_NAIL-RETROGRADE_FEMORAL',
                                    '1I': 'INTRAMEDULLARY_NAIL-TIBIA',
                                    '1J': 'SCAPHOID_FRACTURE',
                                    '1I': 'PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE_REDUCTION_AND_PINNING',
                                    '2A': 'SHOULDER_ARTHROSCOPY',
                                    '2B': 'KNEE_ARTHROSCOPY',
                                    '2C': 'HIP_ARTHROSCOPY', 
                                    '2D': 'ANKLE_ARTHROSCOPY',
                                    '3A': 'OTHER'
}


ordered_keys_of_intake_text_file = ['FORM_LAST_MODIFIED', 'OPERATION_DATE', 'SUBJECT_UID', 'FILER_HAWKID', 'FORM_AVAILABLE_FOR_PERFORMANCE', 'SCAN_QUALITY',
                                    'SURGICAL_PROCEDURE_INFO', 'SKILLS_ASSESSMENT_INFO', 'STORAGE_DEVICE_INFO', 'INFO_DERIVED_FROM_ORIGINAL_FILE_METADATA']


class ResourceFile( UIDandMetaInfo ):
    """This can represent a resource at any level, e.g., project, subject, experiment, scan, etc."""
    def __init__( self, metatables: MetaTables, login: XNATLogin ):
        assert metatables.is_user_registered( login.validated_username ), f'User with HAWKID {login.validated_username} is not registered in the system!'
        super().__init__() # Call the __init__ method of the base class to create a uid for this instance
        

    def _init_all_fields( self )    -> None:        raise NotImplementedError( 'This method must be implemented in the subclass.' ) # This is a placeholder for the subclass to implement
        
    def __str__( self )             -> str:         return '-----'*5 + f'\nOR Data Intake Form\n' + '-----'*5 + '\n\n'


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
    def __init__( self, metatables: MetaTables, login: XNATLogin, ffn: Opt[str]=None, verbose: Opt[bool]=False, write_to_file: Opt[bool]=False ):
        super().__init__( metatables=metatables, login=login ) # Call the __init__ method of the base class -- bug:? goes all the way to our utility class and generates a uid.

        # Init dict (and future json-formatted text file) with required keys.
        self._init_all_fields()
        
        # Either read in the inputted text file and distribute that data, or prompt the user for the data.
        if ffn:
            self._read_from_file( ffn, verbose=verbose )
            return
        else:
            self._prompt_user_for_filer_name_and_operation_date( metatables=metatables )
            self._prompt_user_for_scan_quality()
            self._prompt_user_for_surgical_procedure_info( metatables=metatables )
            self._prompt_user_for_skills_assessment_info( metatables=metatables )
            self._prompt_user_for_storage_device_info()

            # Need to identify the save-to location for the json file; if successfully read from file, use that, else, use the generated uid.
            self._saved_ffn = metatables.tmp_data_dir / Path( self.uid ) / self.filename
            if not os.path.exists( self.saved_ffn.parent ): os.makedirs( self.saved_ffn.parent )
            if write_to_file: self.construct_digital_file( verbose=verbose )
            # self._create_text_file_reconstruction( verbose=verbose ) # commenting out bc we want it saved to a temp folder corresponding to this subject
        


    def _read_from_file( self, ffn: str, verbose: Opt[bool]=False ) -> None:
        if verbose: print( f'\n\t--- Initializing IntakeFrom from {ffn} ---' )
        with open( ffn, 'r', encoding='cp1252' ) as jf:
            self._running_text_file = json.loads( jf.read() )

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
        self._saved_ffn = Path( ffn )

        try: # Optional fields that may not be present in the inputted form (if the filer did not have this info when originally creating the digitized version)
            self._epic_end_time = self.running_text_file['SURGICAL_PROCEDURE_INFO']['EPIC_END_TIME']
            self._OR_location = self.running_text_file['SURGICAL_PROCEDURE_INFO']['OR_LOCATION']
            self._supervising_surgeon_hawk_id = self.running_text_file['SURGICAL_PROCEDURE_INFO']['SUPERVISING_SURGEON_UID']
            self._supervising_surgeon_presence = self.running_text_file['SURGICAL_PROCEDURE_INFO']['SUPERVISING_SURGEON_PRESENCE']
            self._performing_surgeon_hawk_id = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMING_SURGEON_UID']
            self._performer_year_in_residency = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_YEAR_IN_RESIDENCY']
            self._performer_was_assisted = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_WAS_ASSISTED']
            self._performer_num_of_similar_logged_cases = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMER_NUM_OF_SIMILAR_LOGGED_CASES']
            self._performance_enumerated_task_performer = self.running_text_file['SURGICAL_PROCEDURE_INFO']['PERFORMANCE_ENUMERATED_TASK_PERFORMER']
            self._list_unusual_features_of_performance = self.running_text_file['SURGICAL_PROCEDURE_INFO']['LIST_UNUSUAL_FEATURES']
            self._diagnostic_notes = self.running_text_file['SURGICAL_PROCEDURE_INFO']['DIAGNOSTIC_NOTES']
            self._misc_surgical_performance_comments = self.running_text_file['SURGICAL_PROCEDURE_INFO']['MISC_PROCEDURE_COMMENTS']
            self._assessment_title = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSMENT_TITLE']
            self._assessor_hawk_id = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSOR_UID']
            self._assessment_details = self.running_text_file['SKILLS_ASSESSMENT_INFO']['ASSESSMENT_DETAILS']
        except Exception as e:
            if verbose: print( f'\t--- Only minimally required fields were found in the inputted form.' )


    def _init_all_fields( self ):
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
        self._running_text_file = OrderedDict( ( k, acceptable_ortho_procedure_names[k]) for k in ordered_keys_of_intake_text_file if k in acceptable_ortho_procedure_names )
        self._running_text_file['FORM_LAST_MODIFIED'] = datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()


    @staticmethod
    def prompt_until_valid_answer_given( selection_name: str, acceptable_options: list ) -> str:
        while True:
            user_input = input( f'\tAnswer:\t' )
            if user_input.upper() in acceptable_options: return user_input.upper()
            else:
                print( f'\t--- Invalid entry for {selection_name}! Please enter one of the options listed above.' )
    

    def _prompt_user_for_filer_name_and_operation_date( self, metatables: MetaTables ) -> None: 
        possible_user_hawkids = metatables.list_of_all_items_in_table( 'REGISTERED_USERS' )
        print( f'\t(1/35)\tHAWKID of the Form Filer\t--\tPlease enter a HawkID from the following list:\t{possible_user_hawkids}' )
        filer_hawkid = self.prompt_until_valid_answer_given( 'HawkID of the Form Filer', acceptable_options=possible_user_hawkids ) # to-do: allow user to just input an integer instead of type out hawkid?
        self._filer_name = filer_hawkid.upper()

        print( '\n\t(2/35)\tIs there an OR Data Intake Form available for this procedure?\n\tEnter "1" for Yes or "2" for No' )
        form_available = self.prompt_until_valid_answer_given( 'Form Availability', acceptable_options=['1', '2'] ) # to-do: Automate acceptable_options based on the type of input expected bc we may change the metatables values for this and then these prompts wont reflect those changes.

        self._operation_date = parser.parse( input( '\n\t(3/35)\tPlease enter the Operation Date (YYYY-MM-DD):\t' ) ).date().strftime( '%Y-%m-%d' )
        if form_available == '1':       self._form_available = True
        elif form_available == '2':     self._form_available = False
        
        self._running_text_file['FILER_HAWKID'] = str( self.filer_name )
        self._running_text_file['FORM_AVAILABLE_FOR_PERFORMANCE'] = str( self.form_is_available )
        self._running_text_file['OPERATION_DATE'] = str( self.operation_date )
        self._running_text_file['SUBJECT_UID'] = str( self.uid )
    
    
    def get_time_input( self, prompt ) -> str:
        for _ in range(2):  # Gives the user 1 opportunity to try again
            user_input = input( prompt )
            try: return parser.parse( user_input ).time().strftime( '%H:%M' )
            except ValueError: print( f"\t---Invalid time provided; you entered: {user_input}.\n\t\tPlease use the HH:MM format." )
        raise ValueError( "Failed to provide a valid time after 2 attempts." )


    def _prompt_user_for_scan_quality( self ):
        print( f'\n\n--- Quality/Usability of the OR Image Data ---' )
        print( f'\t(4/35)\tWhat is the quality of the OR image data?\n\tEnter "1" for Usable, "2" for Un-usable, "3" for Questionable, or "4" for Unknown.' )
        scan_quality = self.prompt_until_valid_answer_given( 'Quality of the Scan', acceptable_options=['1', '2', '3', '4'] )
        if scan_quality == '1':     self._scan_quality = 'usable'
        elif scan_quality == '2':   self._scan_quality = 'unusable'
        elif scan_quality == '3':   self._scan_quality = 'questionable'
        else:                       self._scan_quality = ''
        self._running_text_file['SCAN_QUALITY'] = self.scan_quality # type: ignore -- not sure why this is giving a type error. runs fine in spite of it.


    def _prompt_user_for_surgical_procedure_info( self, metatables: MetaTables ): # Make sure fields that might be stored in the metatables are all completely capitalized
        print( f'\n--- Surgical Procedure Information ---' )
        local_dict = {}

        acceptable_institutions = metatables.list_of_all_items_in_table( table_name='ACQUISITION_SITES' )
        print( f'\t(5/35)\tInstitution Name\t--\tPlease Copy-and-Paste from the following list:\t{acceptable_institutions}' )
        self._institution_name = self.prompt_until_valid_answer_given( 'Institution Name', acceptable_options=acceptable_institutions )
        local_dict['INSTITUTION_NAME'] = self.institution_name

        print( f'\n\t(6/35)\tType of Orthro Procedure\t--\tPlease enter "1" for Trauma or "2" for Arthro' )
        ortho_procedure_type = self.prompt_until_valid_answer_given( 'Type of Orthro Procedure', acceptable_options=['1', '2'] )
        if ortho_procedure_type == '1':     self._ortho_procedure_type = 'Trauma'.upper()
        elif ortho_procedure_type == '2':   self._ortho_procedure_type = 'Arthroscopy'.upper()
        local_dict['PROCEDURE_TYPE'] = self.ortho_procedure_type

        # Given the ortho procedure type, select the keys from the acceptable_ortho_procedure_names dictionary that begin with the ortho_procedure_type
        acceptable_ortho_procedure_name_options = {key: value for key, value in acceptable_ortho_procedure_names.items() if key.startswith( ortho_procedure_type )}
        options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_ortho_procedure_name_options.items()] )

        print( f'\n\t(7/35)\tOrtho Procedure Name\t--\tPlease select from the following options:\n{options_str}')
        procedure_name_key = self.prompt_until_valid_answer_given( 'Ortho Procedure Name', acceptable_options = list( acceptable_ortho_procedure_name_options ) )
        self._ortho_procedure_name = acceptable_ortho_procedure_name_options[procedure_name_key]
        local_dict['PROCEDURE_NAME'] = str( self.ortho_procedure_name ) # type: ignore

        if self.form_is_available:
            valid_times, num_attempts, max_attempts = False, 0, 2
            while not valid_times and num_attempts < max_attempts:
                epic_start_time = self.get_time_input( '\n\t(8/35)\tEpic Start Time (HH:MM): ' )

                epic_end_time = self.get_time_input( '\t(9/35)\tEpic End Time (HH:MM): ' )
                if epic_start_time and epic_end_time:  # If both are not None
                    if epic_start_time < epic_end_time: valid_times = True
                    else:
                        print( f'\tAttempt {num_attempts + 1} failed:\n\t\tStart Time must be before End Time. You entered {epic_start_time} and {epic_end_time}.' )
                        num_attempts += 1
                else:
                    print( f'\tAttempt {num_attempts + 1} failed:\n\t\tInvalid times provided. Please use the HH:MM format.' )
                    num_attempts += 1
            if      not valid_times:    raise InvalidInputError('Failed to provide valid start and end times after multiple attempts.')
            else:   self._epic_start_time, self._epic_end_time = epic_start_time, epic_end_time
            local_dict['EPIC_START_TIME'], local_dict['EPIC_END_TIME'] = self.epic_start_time, self.epic_end_time
        else:
            print( f'\n\t(10/35)\tDo you know the Operation or EPIC Start Time\t--\tPlease enter "1" for Yes or "2" for No' )
            known_start_time = self.prompt_until_valid_answer_given( 'Known Start Time', acceptable_options = list( ['1', '2'] ) )
            if known_start_time == '1':
                epic_start_time = self.get_time_input( '\t(10/35)\tEpic Start Time (HH:MM):\t' )
            else: epic_start_time = datetime.now().replace( hour=0, minute=0, second=0, microsecond=0 ).strftime( '%H:%M:%S')  # Assign midnight-today as the default start time
            local_dict['EPIC_START_TIME'] = epic_start_time
            self._epic_start_time = epic_start_time


        print( f'\n\t(11/35)\tSide of Patient\'s Body\t--\tEnter "1" for Right, "2" for Left, "3" for Unknown, or "4" for N/A or not relevant.' )
        patient_side = self.prompt_until_valid_answer_given( 'Side of Patient\'s Body', acceptable_options=['1', '2', '3'] )
        if patient_side == '1':     self._side_of_patient_body = 'Right'.upper()
        elif patient_side == '2':   self._side_of_patient_body = 'Left'.upper()
        elif patient_side == '3':  self._side_of_patient_body = 'Unknown'.upper()
        else: raise ValueError( 'You indicated N/A for the side of the patient body; please contact the data librarian to clarify this before proceding!' )
        local_dict['PATIENT_SIDE'] = self.side_of_patient_body

        if self.form_is_available:
            self._OR_location = input( '\n\t(12/35)\tOperating Room Name/Location:\t' ).upper()
            local_dict['OR_LOCATION'] = self.OR_location

            print( f'\n\t(13/35)\tSupervising Surgeon HawkID\t--\tPlease select from the following list:\t{metatables.list_of_all_items_in_table( "Surgeons" )}' )
            supervising_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'Supervising Surgeon\'s HAWKID', acceptable_options=metatables.list_of_all_items_in_table( 'Surgeons' ) )

            print( f"\n\t(14/35)\tSupervising Surgeon Presence\t--\tEnter '1' for Present, '2' for Retrospective Review, or '3' for Other:" )
            supervising_surgeon_presence = self.prompt_until_valid_answer_given( 'Supervising Surgeon Presence', acceptable_options=['1', '2', '3'] )
            if supervising_surgeon_presence == '1':     self._supervising_surgeon_presence = 'Present'.upper()
            elif supervising_surgeon_presence == '2':   self._supervising_surgeon_presence = 'Retrospective Review'.upper()
            else: raise ValueError( 'You indicated other for the supervision surgeon presence; please contact the data librarian to clarify this before proceding!' )
            self._supervising_surgeon_hawk_id, self._supervising_surgeon_presence = metatables.get_uid( 'Surgeons', supervising_surgeon_hawk_id ), supervising_surgeon_presence
            local_dict['SUPERVISING_SURGEON_UID'], local_dict['SUPERVISING_SURGEON_PRESENCE'] = self.supervising_surgeon_hawk_id, self.supervising_surgeon_presence

            print( f'\n\t(15/35)\tPerforming Surgeon HawkID\t--\tPlease select from the following list:\t{metatables.list_of_all_items_in_table( "Surgeons" )}' )
            performing_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'tPerforming Surgeon\'s HAWKID', acceptable_options=metatables.list_of_all_items_in_table( 'Surgeons' ) )

            performer_year_in_residency = input( f'\n\t(16/35)\tPerforming Surgeon\'s Years in Residency: ' )
            assert performer_year_in_residency.isdigit(), 'Invalid entry for Performing Surgeon\'s Years in Residency! Must be an integer.'
            
            print( f'\n\t(17/35)\tDo you know how many similar prior cases have been logged by the performing surgeon?\n\tEnter "1" for Yes or "2" for No \t--\tNOTE: 0 prior cases ***is NOT the same thing*** as unknown!! Please Enter "1" for Yes and then declare 0 known cases in the following prompt.')
            known_number_of_similar_logged_cases = self.prompt_until_valid_answer_given( '# of Similar Cases Logged', acceptable_options=['1', '2'] )
            if known_number_of_similar_logged_cases == '1':

                performer_num_of_similar_logged_cases = input( f'\n\t(18/35)\tPerforming Surgeon\'s # of Similar Cases Logged (if none, enter "0"):\t' )
                self._performer_num_of_similar_logged_cases     = int( performer_num_of_similar_logged_cases )
            else: self._performer_num_of_similar_logged_cases   = None
            self._performing_surgeon_hawk_id, self._performer_year_in_residency = metatables.get_uid( 'Surgeons', performing_surgeon_hawk_id ), int( performer_year_in_residency )
            local_dict['PERFORMING_SURGEON_UID'], local_dict['PERFORMER_YEAR_IN_RESIDENCY'], local_dict['PERFORMER_NUM_OF_SIMILAR_LOGGED_CASES'] = self.performing_surgeon_hawk_id, self.performer_year_in_residency, self.performer_num_of_similar_logged_cases

            print( f'\n\t(19/35)\tWas the Performing Surgeon Assisted?\tEnter "1" for Yes or "2" for No.' )
            performer_was_assisted = self.prompt_until_valid_answer_given( 'Performing Surgeon Assistance', acceptable_options=['1', '2'] )
            if performer_was_assisted == '1':
                self._performer_was_assisted = True
                dict_performance_enumerated_tasks = self._prompt_user_for_n_surgical_tasks_and_hawkids( metatables=metatables )

                # If any of the values in the dict are empty, replace them with None
                for key, value in dict_performance_enumerated_tasks.items():
                    if len( value ) == 0: dict_performance_enumerated_tasks[key] = None
                self._performance_enumerated_task_performer = dict_performance_enumerated_tasks
            local_dict['PERFORMER_WAS_ASSISTED'], local_dict['PERFORMANCE_ENUMERATED_TASK_PERFORMER'] = self.performer_was_assisted, self.performance_enumerated_task_performer

            print( f'\n\t(21/35)\tWere there any unusual features of the performance?\n\tEnter "1" for Yes or "2" for No.')
            any_unusual_features_of_performance = self.prompt_until_valid_answer_given( 'Unusual Features of Performance', acceptable_options=['1', '2'] )
            if any_unusual_features_of_performance == '1':
                list_of_performance_features = input( f'\t(22/35)\tPlease detail any/all unusual features of the performance:\n\tAnswer: ' )
                if len( list_of_performance_features ) > 0:     self._list_unusual_features_of_performance = list_of_performance_features
            local_dict['LIST_UNUSUAL_FEATURES'] = self.list_unusual_features_of_performance

            print( f'\n\t(23/35)\tWere there any diagnostic notes about the surgical procedure?\n\tEnter "1" for Yes or "2" for No.')
            any_diagnostic_notes = self.prompt_until_valid_answer_given( 'Performing Surgeon Assistance', acceptable_options=['1', '2'] )
            # if self.ortho_procedure_type == 'Arthroscopy' or ortho_procedure_type == '2':
            if any_diagnostic_notes == '1':
                diagnostic_notes = input( f'\t(24/35)\tPlease enter any diagnostic notes about the surgical procedure:\n\tAnswer: ' )
                if len( diagnostic_notes ) > 0:                 self._diagnostic_notes = diagnostic_notes
            local_dict['DIAGNOSTIC_NOTES'] = self.diagnostic_notes

            print( f'\n\t(24/35)\tDo you have any additional comments or notes regarding BMI, pre-existing conditions, etc.?\n\tEnter "1" for Yes or "2" for No.' )
            any_misc_comments = self.prompt_until_valid_answer_given( ' Miscellaneous Procedure Comments', acceptable_options=['1', '2'])
            if any_misc_comments == '1':
                misc_comments = input( f'\t(25/35)\tPlease enter any additional comments or notes:\n\t\t' )
                if len( misc_comments ) > 0:     self._misc_surgical_performance_comments = misc_comments
            local_dict['MISC_PROCEDURE_COMMENTS'] = self.misc_surgical_performance_comments
        
        # Need to save info to the running text file regardless of if the form is available
        self._running_text_file['SURGICAL_PROCEDURE_INFO'] = local_dict # type: ignore
    

    def _prompt_user_for_n_surgical_tasks_and_hawkids( self, metatables: MetaTables ) -> dict:
        num_tasks = int( input( '\t(20/35)\tHow many surgeons participated in the procedure?\n\tEnter an integer:\t' ) )
        print( f'\tSelect from the following list of hawkIDs:\n\t\t{metatables.list_of_all_items_in_table( "Surgeons" )}')
        assert num_tasks > 0, 'Invalid number of surgeons! Must be a positive integer.'
        task_performers = {}
        for i in range( num_tasks ):
            if i == 0:      hawkid = input( f'\t\t1st HAWKID:\t' )
            elif i == 1:    hawkid = input( f'\t\t2nd HAWKID:\t' )
            elif i == 2:    hawkid = input( f'\t\t3rd HAWKID:\t' )
            else:           hawkid = input( f'\t\t{i+1}th HAWKID:\t' )
            assert metatables.item_exists( table_name='Surgeons', item_name=hawkid ), f'HAWKID {hawkid} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{metatables.list_of_all_items_in_table( "Surgeons" )}'
            task_performers[metatables.get_uid(table_name='SURGEONS', item_name=hawkid)] = input( f'\t\t\tEnter the task(s) performed by "{hawkid.upper()}":\t' )
        return task_performers


    def _prompt_user_for_skills_assessment_info( self, metatables: MetaTables ):
        if self.form_is_available:
            print( f'\n\n--- Skills Assessment Information ---' )
        
            print( f'\t(26/35)\tWas a Skills Assessment requested for this procedure?\n\tEnter "1" for Yes or "2" for No.')
            assessment_requested = self.prompt_until_valid_answer_given( 'Skills Assessment Request', acceptable_options=['1', '2'] )
            if assessment_requested == '1':

                assessment_requested, assessment_title = True, input( '\n\t(25/32)\tPlease enter the full name of the requested assessment:\t' ).upper()


                print( f'\n\t(27/35)\tAssessing Surgeon\'s HawkID --\tPlease select from the following list:\n\t\t{metatables.list_of_all_items_in_table( "Surgeons" )}' )
                assessor_hawkid = self.prompt_until_valid_answer_given( 'Assessing Surgeon\'s HAWKID', acceptable_options=metatables.list_of_all_items_in_table( 'Surgeons' ) )
                assessor_hawkid = metatables.get_uid( table_name='SURGEONS', item_name=assessor_hawkid )

                print( f'\n\t(28/35)\tDo you have any additional details about the assessment (e.g., date of assessment, score, etc.)?\n\tEnter "1" for Yes or "2" for No.')
                known_details = self.prompt_until_valid_answer_given( 'Additional Assessment Details', acceptable_options=['1', '2'])

                if known_details == '1':    self._assessment_details = input( '\n\t(29/35)\tPlease enter any additional details about the assessment:\n\t\t' )
                else:                       self._assessment_details = None
                self._assessment_title, self._assessor_hawk_id = assessment_title, assessor_hawkid
            elif assessment_requested == '2':
                assessment_requested, self._assessment_title, self._assessor_hawk_id, self._assessment_details = False, None, None, None
            else:   raise ValueError( f'Invalid entry for Skills Assessment! Please only enter "1" for Yes or "2" for No. You entered {assessment_requested}' )
            self._running_text_file['SKILLS_ASSESSMENT_INFO'] = {   'ASSESSMENT_REQUESTED': assessment_requested, # type: ignore
                                                                    'ASSESSMENT_TITLE': self.assessment_title,
                                                                    'ASSESSOR_UID': self.assessor_hawk_id,
                                                                    'ASSESSMENT_DETAILS': self.assessment_details}


    def _prompt_user_for_storage_device_info( self ):
        print( f'\n\n--- Storage Device Information ---' )
        
        self._storage_device_name_and_type = input( '\t(30/35)\tPlease enter the name and type of the storage device:\t' )

        full_path_name = input( '\t(31/35)\tPlease enter the full directory name of the folder containing the case data:\t\t' )
        while not os.path.exists( full_path_name ):#or not full_path_name.lower() in ['', 'escape', 'exit', 'stop', 'quit', 'n/a', 'na', 'unknown', 'none', 'not applicable']:
            print( f'!!!!!Input directory path is not accessible on this system!!!!!\n\tPlease double-check the validity of that directory and try again.' )
            full_path_name = input( '\t(31/35)\tPlease enter the full directory name of the folder containing the case data:\n\t' )
        self._relevant_folder = Path( full_path_name )

        print( f'\n\t(32/35)\tWas radiology contacted for this procedure?\n\tEnter "1" for Yes, "2" for No, or "3" for Unknown.' )
        radiology_contacted = self.prompt_until_valid_answer_given( 'Radiology Contact Information', acceptable_options=['1', '2', '3'] )
        if radiology_contacted == '1':
            radiology_contact_date = parser.parse( input( '\t(33/35)\tRadiology Contact Date (YYYY-MM-DD):\t' ) ).date().strftime( '%Y-%m-%d' )
            radiology_contact_time = self.get_time_input( '\t(34/35)\tRadiology Contact Time (HH:MM):\t' )
            self._radiology_contact_date,   self._radiology_contact_time = radiology_contact_date, radiology_contact_time
        elif radiology_contacted in ['2', '3']:    self._radiology_contact_date, self._radiology_contact_time = None, None
        else:   raise ValueError( f'Invalid entry for Radiology Contacted! Please only enter "1" for Yes or "2" for No. You entered {radiology_contacted}' )
        self._running_text_file['STORAGE_DEVICE_INFO'] = {  'STORAGE_DEVICE_NAME_AND_TYPE': self.name_of_storage_device, # type: ignore
                                                            'RADIOLOGY_CONTACT_DATE': self.radiology_contact_date,
                                                            'RADIOLOGY_CONTACT_TIME': self.radiology_contact_time,
                                                            'RELEVANT_FOLDER': self.relevant_folder}
        

    def construct_digital_file( self, verbose: Opt[bool]=False ) -> None:
        assert self._uid, 'UID must be set before calleding the IntakeForm saved full file name.'
        self._running_text_file['FORM_LAST_MODIFIED'] = datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat()
        json_str = json.dumps( self.running_text_file, indent=4, default=ORDataIntakeForm._custom_serializer )
        with open( self.saved_ffn, 'w' ) as f:
            f.write( json_str )
            if verbose:     print( f' -- SUCCESS -- OR Data Intake Form saved to:\t{self.saved_ffn}' )


    def push_to_xnat( self, subj_inst, verbose: Opt[bool] = False ):
        if verbose:     print( f'\t\t...Uploading resource files...' )
        with open( self.saved_ffn, 'r' ) as f:
            subj_inst.resource( 'INTAKE_FORM' ).file( self.filename_str ).insert( f.read(), content='TEXT', format='JSON', tags='DOC' ) # type: ignore


    @staticmethod
    def _custom_serializer( obj ) -> str:
        if isinstance( obj, WindowsPath ):
            return str( obj )  # Convert WindowsPath to string
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
    def performer_year_in_residency( self )             -> Opt[int]:                return self._performer_year_in_residency
    @property
    def performer_was_assisted( self )                  -> Opt[bool]:               return self._performer_was_assisted
    @property
    def performer_num_of_similar_logged_cases( self )   -> Opt[int]:                return self._performer_num_of_similar_logged_cases
    @property
    def performance_enumerated_task_performer( self )   -> Opt[dict]:               return self._performance_enumerated_task_performer
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
        # Print out the json formatted information as it would be shown in a text file.
        json_str = json.dumps( self.running_text_file, indent=4 )
        lines = json_str.split('\n')
        out_str = f'\t-- OR Data Intake Form --\n'
        for line in lines:
            if line.strip() not in ['{', '}', '},']:
                out_str += line + '\n'
        return out_str