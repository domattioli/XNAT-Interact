import os
import json
from typing import List, Dict, Any, Tuple, Optional as Opt, Union

from datetime import datetime, date as dtdate, time as dttime
from dateutil import parser

from src.utilities import LibrarianUtilities, MetaTables, USCentralDateTime, XNATLogin, XNATConnection, USCentralDateTime

import pytz

acceptable_ortho_procedure_names = {'1A': 'SHOULDER_ARTHROSCOPY',
                                    '1B': 'KNEE_ARTHROSCOPY',
                                    '1C': 'HIP_ARTHROSCOPY', 
                                    '1D': 'ANKLE_ARTHROSCOPY',
                                    '2A': 'OPEN_REDUCTION_HIP_FRACTURE–DYNAMIC_HIP_SCREW',
                                    '2B': 'OPEN_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW',
                                    '2C': 'CLOSED_REDUCTION_HIP_FRACTURE–CANNULATED_HIP_SCREW',
                                    '2D': 'PERCUTANEOUS_SACROLIAC_FIXATION',
                                    '2E': 'OPEN_AND_PERCUTANEOUS_PILON_FRACTURES',
                                    '2F': 'INTRAMEDULLARY_NAIL-CMN',
                                    '2G': 'INTRAMEDULLARY_NAIL-ANTEGRADE_FEMORAL',
                                    '2H': 'INTRAMEDULLARY_NAIL-RETROGRADE_FEMORAL',
                                    '2I': 'INTRAMEDULLARY_NAIL-TIBIA',
                                    '2J': 'SCAPHOID_FRACTURE',
                                    '3A': 'PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE_REDUCTION_AND_PINNING',
                                    '4A': 'OTHER'}
options_str = "\n".join( [f"\t\tEnter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_ortho_procedure_names.items()] )


class ResourceFile( LibrarianUtilities ):
    def __init__( self, metatables: MetaTables, login: XNATLogin ):
        self._running_text_file = ''
        self._metatables = metatables
        assert self.metatables.is_user_registered( login.validated_username ), f'User with HAWKID {login.validated_username} is not registered in the system!'
        self._login = login

    def _welcome_message( self ):
        self._running_text_file = str( self )  # Directly use the string representation

        
    def __str__( self )             -> str:         return '-----'*5 + f'\nOR Data Intake Form\n' + '-----'*5 + '\n\n'

    @property
    def running_text_file( self )   -> str:         return self._running_text_file
    @property
    def metatables( self )          -> MetaTables:  return self._metatables


class InvalidInputError( Exception ):
    """Exception raised for errors in the input after multiple attempts."""
    def __init__(self, message="Invalid input despite multiple attempts to correct it. Exiting..."):
        self.message = message
        super().__init__(self.message)


class ORDataIntakeForm( ResourceFile ):
    def __init__( self, metatables: MetaTables, login: XNATLogin ):
        super().__init__( metatables=metatables, login=login ) # Call the __init__ method of the base class
        self._init_all_fields()
        self._filer_name_and_performance_date_date_user_prompts()
        if self.form_is_available:
            self._surgical_procedure_info_user_prompts()
            self._skills_assessment_user_prompts()
            self._storage_device_user_prompts()
        self._create_text_file_reconstruction()


    def _init_all_fields( self ):
        # Need to just assign all the fields to None and populate the text file anyway.
        self._filer_name, self._performance_date, self._institution_name, self._ortho_procedure_type, self._ortho_procedure_name = None, None, None, None, None
        self._epic_start_time, self._epic_end_time, self._side_of_patient_body, self._OR_location = None, None, None, None
        self._supervising_surgeon_hawk_id, self._supervising_surgeon_presence, self._performing_surgeon_hawk_id, self._performer_year_in_residency = None, None, None, None
        self. _performer_was_assisted, self._performer_num_of_similar_logged_cases, self._performance_enumerated_task_performer = None, None, None
        self._unusual_features_present, self._list_unusual_features_of_performance, self._diagnostic_notes, self._misc_surgical_performance_comments = None, None, None, None
        self._assessment_title, self._assessor_hawk_id, self._assessment_details = None, None, None
        self._storage_device_name_and_type, self._radiology_contact_date, self._radiology_contact_time, self._relevant_folder_and_file_names = None, None, None, None
        self._running_text_file = {'UPLOAD_DATE': datetime.now( pytz.timezone( 'America/Chicago' ) ).isoformat() }


    def prompt_until_valid_answer_given( self, selection_name: str, acceptable_options: list ) -> str:
        while True:
            user_input = input( f'\tAnswer:\t' )
            if user_input.upper() in acceptable_options: return user_input.upper()
            else:
                print( f'Invalid entry for {selection_name}! Please enter one of the options listed above' )
    

    def _filer_name_and_performance_date_date_user_prompts( self ):
        filer_hawkid = input( 'Please enter the HAWKID of the Form Filer:\t' )
        assert self.metatables.is_user_registered( filer_hawkid ), f'User with HAWKID {filer_hawkid} is not registered in the system!'
        self._filer_name = filer_hawkid.upper()
        print( 'Is there an OR Data Intake Form available for this procedure?\n\tEnter "1" for Yes or "2" for No' )
        form_available = self.prompt_until_valid_answer_given( 'Form Availability', acceptable_options=['1', '2'] )
        self._performance_date = parser.parse( input( 'Please enter the Operation Date (YYYY-MM-DD):\t' ) ).date().strftime( '%Y-%m-%d' )
        if form_available == '1':   self._form_available = True
        elif form_available == '2': self._form_available = False
        
        self._running_text_file['FILER_NAME'] = str( self.filer_name )
        self._running_text_file['FORM_AVAILABLE_FOR_PERFORMANCE'] = str( self.form_is_available )
        self._running_text_file['PERFORMANCE_DATE'] = str( self.performance_date )
    
    
    def get_time_input( self, prompt ) -> str:
        for _ in range(2):  # Gives the user 1 opportunity to try again
            user_input = input( prompt )
            try: return parser.parse( user_input ).time().strftime( '%H:%M' )
            except ValueError: print( f"\t---Invalid time provided; you entered: {user_input}.\n\t\tPlease use the HH:MM format." )
        raise ValueError( "Failed to provide a valid time after 2 attempts." )


    def _surgical_procedure_info_user_prompts( self ): # Make sure fields that might be stored in the metatables are all completely capitalized
        print( f'\nSurgical Procedure Information ---\n' )
        local_dict = {}

        acceptable_institutions = self.metatables.list_of_all_items_in_table( table_name='ACQUISITION_SITES' )
        print( f'\n\t(1/29)\tInstitution Name --\t\tPlease Copy-and-Paste from the following list:\t{acceptable_institutions}' )
        self._institution_name = self.prompt_until_valid_answer_given( 'Institution Name', acceptable_options=acceptable_institutions )
        local_dict['INSTITUTION_NAME'] = self.institution_name

        print( f'\n\t(2/29)\tType of Orthro Procedure --\tPlease enter "1" for Trauma or "2" for Arthro' )
        ortho_procedure_type = self.prompt_until_valid_answer_given( 'Type of Orthro Procedure', acceptable_options=['1', '2'] )
        if ortho_procedure_type == '1':     self._ortho_procedure_type = 'Trauma'.upper()
        elif ortho_procedure_type == '2':   self._ortho_procedure_type = 'Arthroscopy'.upper()
        local_dict['PROCEDURE_TYPE'] = self.ortho_procedure_type

        print( f'\n\t(3/29)\tOrtho Procedure Name --\tPlease select from the following options:\n{options_str}')
        self._ortho_procedure_name = self.prompt_until_valid_answer_given( 'Ortho Procedure Name', acceptable_options=list( acceptable_ortho_procedure_names.keys() ) )
        local_dict['PROCEDURE_NAME'] = str( acceptable_ortho_procedure_names[self.ortho_procedure_name] ) # type: ignore

        valid_times, num_attempts, max_attempts = False, 0, 2
        while not valid_times and num_attempts < max_attempts:
            epic_start_time, epic_end_time = self.get_time_input( '\t(4/29)\tEpic Start Time (HH:MM):\t' ), self.get_time_input( '\t(5/29)\tEpic End Time (HH:MM):\t' )
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


        print( f'\n\t(6/29)\tSide of Patient\'s Body --\tEnter "1" for Right, "2" for Left, or "3" for N/A' )
        patient_side = self.prompt_until_valid_answer_given( 'Side of Patient\'s Body', acceptable_options=['1', '2', '3'] )
        if patient_side == '1':     self._side_of_patient_body = 'Right-side'.upper()
        elif patient_side == '2':   self._side_of_patient_body = 'Left-side'.upper()
        else: raise ValueError( 'You indicated N/A for the side of the patient body; please contact the data librarian to clarify this before proceding!' )
        local_dict['PATIENT_SIDE'] = self.side_of_patient_body


        self._OR_location = input( '\n\t(7/29)\tOperating Room Name/Location:\t' ).upper()
        local_dict['OR_LOCATION'] = self.OR_location


        print( f'\n\t(8/29)\tSupervising Surgeon HawkID --\tPlease select from the following list:\n\t\t{self.metatables.list_of_all_items_in_table( "Surgeons" )}' )
        supervising_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'Supervising Surgeon\'s HAWKID', acceptable_options=self.metatables.list_of_all_items_in_table( 'Surgeons' ) )
        print( f"\n\t(9/29)\tSupervising Surgeon Presence --\tEnter '1' for Present, '2' for Retrospective Review, or '3' for Other:" )
        supervising_surgeon_presence = self.prompt_until_valid_answer_given( 'Supervising Surgeon Presence', acceptable_options=['1', '2', '3'] )
        if supervising_surgeon_presence == '1':     self._supervising_surgeon_presence = 'Present'.upper()
        elif supervising_surgeon_presence == '2':   self._supervising_surgeon_presence = 'Retrospective Review'.upper()
        else: raise ValueError( 'You indicated other for the supervision surgeon presence; please contact the data librarian to clarify this before proceding!' )
        self._supervising_surgeon_hawk_id, self._supervising_surgeon_presence = self.metatables.get_uid( 'Surgeons', supervising_surgeon_hawk_id ), supervising_surgeon_presence
        local_dict['SUPERVISING_SURGEON_UID'], local_dict['SUPERVISING_SURGEON_PRESENCE'] = self.supervising_surgeon_hawk_id, self.supervising_surgeon_presence


        print( f'\n\t(10/29)\tPerforming Surgeon HawkID --\tPlease select from the following list:\n\t\t{self.metatables.list_of_all_items_in_table( "Surgeons" )}' )
        performing_surgeon_hawk_id = self.prompt_until_valid_answer_given( 'tPerforming Surgeon\'s HAWKID', acceptable_options=self.metatables.list_of_all_items_in_table( 'Surgeons' ) )
        performer_year_in_residency = input( '\tPerforming Surgeon\'s Years in Residency:\t' )
        assert performer_year_in_residency.isdigit(), 'Invalid entry for Performing Surgeon\'s Years in Residency! Must be an integer.'
        print( f'\n\t(11/29)\tDo you know how many similar prior cases have been logged by the performing surgeon?\n\tEnter "1" for Yes or "2" for No \t--\t***0 prior cases is NOT the same thing as unknown!!')
        known_number_of_similar_logged_cases = self.prompt_until_valid_answer_given( '# of Similar Cases Logged', acceptable_options=['1', '2'] )
        if known_number_of_similar_logged_cases == '1':
            performer_num_of_similar_logged_cases = input( '\n\t(12/29)\tPerforming Surgeon\'s # of Similar Cases Logged (if none, enter "0"):\t' )
            self._performer_num_of_similar_logged_cases     = int( performer_num_of_similar_logged_cases )
        else: self._performer_num_of_similar_logged_cases   = None
        self._performing_surgeon_hawk_id, self._performer_year_in_residency = self.metatables.get_uid( 'Surgeons', performing_surgeon_hawk_id ), int( performer_year_in_residency )
        local_dict['PERFORMING_SURGEON_UID'], local_dict['PERFORMER_YEAR_IN_RESIDENCY'], local_dict['PERFORMER_NUM_OF_SIMILAR_LOGGED_CASES'] = self.performing_surgeon_hawk_id, self.performer_year_in_residency, self.performer_num_of_similar_logged_cases


        print( '\n\t(13/29)\tWas the Performing Surgeon Assisted?\tEnter "1" for Yes or "2" for No.' )
        performer_was_assisted = self.prompt_until_valid_answer_given( 'Performing Surgeon Assistance', acceptable_options=['1', '2'] )
        if performer_was_assisted == '1':   self._performer_was_assisted,   self._performance_enumerated_task_performer = True,     self._prompt_user_for_n_surgical_tasks_and_hawkids()
        elif performer_was_assisted == '2': self._performer_was_assisted,   self._performance_enumerated_task_performer = False,    None
        else:   raise ValueError( f'Invalid entry for Performing Surgeon Assisted! Please only enter "1" for Yes or "2" for No. You entered {performer_was_assisted}' )
        local_dict['PERFORMER_WAS_ASSISTED'], local_dict['PERFORMANCE_ENUMERATED_TASK_PERFORMER'] = self.performer_was_assisted, self.performance_enumerated_task_performer

        print( '\n\t(14/29)\tWere there any unusual features of the performance?\n\tEnter "1" for Yes or "2" for No.')
        any_unusual_features_of_performance = self.prompt_until_valid_answer_given( 'Unusual Features of Performance', acceptable_options=['1', '2'] )
        if any_unusual_features_of_performance == '1':
            self._unusual_features_present, self._list_unusual_features_of_performance = True,  input( '\t(15/29)\tPlease detail any/all unusual features of the performance:\n\t\t' ).upper()
        elif any_unusual_features_of_performance == '2':
            self._unusual_features_present, self._list_unusual_features_of_performance = False, None
        else:   raise ValueError( f'Invalid entry for Unusual Features of Performance! Please only enter "1" for Yes or "2" for No. You entered {any_unusual_features_of_performance}' )
        local_dict['UNUSUAL_FEATURES_PRESENT'], local_dict['LIST_UNUSUAL_FEATURES'] = self.unusual_features_present, self.list_unusual_features_of_performance

        if self.ortho_procedure_type == 'Arthroscopy' or ortho_procedure_type == '2':
            diagnostic_notes = input( '\t(16/29)\tPlease enter any diagnostic notes about the surgical procedure (if none, just press Enter):\n\t' ).upper()
            self._diagnostic_notes = diagnostic_notes
        else: self._diagnostic_notes = None
        local_dict['DIAGNOSTIC_NOTES'] = self.diagnostic_notes

        print( '\n\t(17/29)\tDo you have any additional comments or notes regarding BMI, pre-existing conditions, etc.?\n\tEnter "1" for Yes or "2" for No.' )
        misc_comments = self.prompt_until_valid_answer_given( ' Miscellaneous Procedure Comments', acceptable_options=['1', '2'])
        if misc_comments == '1':    self._misc_surgical_performance_comments = input( '\t(18/29)\tPlease enter any additional comments or notes:\n\t\t' ).upper()
        elif misc_comments == '2':  self._misc_surgical_performance_comments = None
        else:   raise ValueError( f'Invalid entry for Additional Comments! Please only enter "1" for Yes or "2" for No. You entered {misc_comments}' )
        local_dict['MISC_PROCEDURE_COMMENTS'] = self.misc_surgical_performance_comments
        self._running_text_file['SURGICAL_PROCEDURE_INFO'] = local_dict # type: ignore
    

    def _prompt_user_for_n_surgical_tasks_and_hawkids( self ) -> dict:
        num_tasks = int( input( '\t(19/29)\tHow many surgeons participated in the procedure?\n\tEnter an integer:\t' ) )
        print( f'\tSelect from the following list of hawkIDs:\n\t\t{self.metatables.list_of_all_items_in_table( "Surgeons" )}')
        assert num_tasks > 0, 'Invalid number of surgeons! Must be a positive integer.'
        task_performers = {}
        for i in range( num_tasks ):
            if i == 0:      hawkid = input( f'\t\t1st HAWKID:\t' )
            elif i == 1:    hawkid = input( f'\t\t2nd HAWKID:\t' )
            elif i == 2:    hawkid = input( f'\t\t3rd HAWKID:\t' )
            else:           hawkid = input( f'\t\t{i+1}th HAWKID:\t' )
            assert self.metatables.item_exists( table_name='Surgeons', item_name=hawkid ), f'HAWKID {hawkid} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{self.metatables.list_of_all_items_in_table( "Surgeons" )}'
            task_performers[self.metatables.get_uid(table_name='SURGEONS', item_name=hawkid)] = input( f'\t\t\tEnter the task(s) performed by "{hawkid.upper()}":\t' )
        return task_performers


    def _skills_assessment_user_prompts( self ):
        print( f'\n\nSkills Assessment Information ---' )
        
        print( f'\n\t(19/29)\tWas a Skills Assessment requested for this procedure?\n\tEnter "1" for Yes or "2" for No.')
        assessment_requested = self.prompt_until_valid_answer_given( 'Skills Assessment Request', acceptable_options=['1', '2'] )
        if assessment_requested == '1':
            assessment_requested, assessment_title = True, input( '\n\t(20/29)\tPlease enter the full name of the requested assessment:\t' ).upper()
            print( '\n\t(21/29)\tDo you have any additional comments or notes regarding BMI, pre-existing conditions, etc.?\n\tEnter "1" for Yes or "2" for No.' )
            print( f'\n\t(22/29)\tAssessing Surgeon\'s HawkID --\tPlease select from the following list:\n\t\t{self.metatables.list_of_all_items_in_table( "Surgeons" )}' )
            assessor_hawkid = self.prompt_until_valid_answer_given( 'Assessing Surgeon\'s HAWKID', acceptable_options=self.metatables.list_of_all_items_in_table( 'Surgeons' ) )
            assessor_hawkid = self.metatables.get_uid( table_name='SURGEONS', item_name=assessor_hawkid )
            print( f'\n\t(23/29)\tDo you have any additional details about the assessment?\n\tEnter "1" for Yes or "2" for No.')
            known_details = self.prompt_until_valid_answer_given( 'Additional Assessment Details', acceptable_options=['1', '2'])
            if known_details == '1':    self._assessment_details = input( '\n\t(24/29)\tPlease enter any additional details about the assessment:\t' ).upper()
            else:                       self._assessment_details = None
            self._assessment_title, self._assessor_hawk_id = assessment_title, assessor_hawkid
        elif assessment_requested == '2':
            assessment_requested, self._assessment_title, self._assessor_hawk_id, self._assessment_details = False, None, None, None
        else:   raise ValueError( f'Invalid entry for Skills Assessment! Please only enter "1" for Yes or "2" for No. You entered {assessment_requested}' )
        self._running_text_file['SKILLS_ASSESSMENT_INFO'] = {   'ASSESSMENT_REQUESTED': assessment_requested, # type: ignore
                                                                'ASSESSMENT_TITLE': self.assessment_title,
                                                                'ASSESSOR_UID': self.assessor_hawk_id,
                                                                'ASSESSMENT_DETAILS': self.assessment_details}

    def _storage_device_user_prompts( self ):
        print( f'\n\nStorage Device Information ---\n' )
        
        self._storage_device_name_and_type = input( '\t(25/29)\tPlease enter the name and type of the storage device:\t' ).upper()
        self._relevant_folder = input( '\t(26/29)\tPlease enter the name of the relevant folder containing the case data:\t' ).upper()

        print( f'\n\t(27/29)\tWas radiology contacted for this procedure?\n\tEnter "1" for Yes or "2" for No.' )
        radiology_contacted = self.prompt_until_valid_answer_given( 'Radiology Contact Information', acceptable_options=['1', '2'] )
        if radiology_contacted == '1':
            radiology_contact_date = parser.parse( input( '\t(28/29)\tRadiology Contact Date (YYYY-MM-DD):\t' ) ).date().strftime( '%Y-%m-%d' )
            radiology_contact_time = self.get_time_input( '\t(29/29)\tRadiology Contact Time (HH:MM):\t' )
            self._radiology_contact_date,   self._radiology_contact_time = radiology_contact_date, radiology_contact_time
        elif radiology_contacted == '2':    self._radiology_contact_date, self._radiology_contact_time = None, None
        else:   raise ValueError( f'Invalid entry for Radiology Contacted! Please only enter "1" for Yes or "2" for No. You entered {radiology_contacted}' )
        self._running_text_file['STORAGE_DEVICE_INFO'] = {  'STORAGE_DEVICE_NAME_AND_TYPE': self.name_of_storage_device, # type: ignore
                                                            'RADIOLOGY_CONTACT_DATE': self.radiology_contact_date,
                                                            'RADIOLOGY_CONTACT_TIME': self.radiology_contact_time,
                                                            'RELEVANT_FOLDER': self.relevant_folder}
        

    def _create_text_file_reconstruction( self ):
        save_ffn = os.path.join( self.metatables.tmp_data_dir, 'OR_DATA_INTAKE_FORM-tmp.txt' )
        json_str = json.dumps( self.running_text_file, indent=4 )
        with open( save_ffn, 'w' ) as f:
            f.write( json_str )
            print( f' -- SUCCESS -- OR Data Intake Form saved to:\t{save_ffn}' )


    @property
    def running_text_file( self )                       -> dict:        return self._running_text_file
    @property
    def form_is_available( self )                       -> bool:        return self._form_available
    @property
    def filer_name( self )                              -> Opt[str]:    return self._filer_name
    @property
    def performance_date( self )                        -> Opt[str]:    return self._performance_date
    @property
    def institution_name( self )                        -> Opt[str]:    return self._institution_name
    @property
    def ortho_procedure_type( self )                    -> Opt[str]:    return self._ortho_procedure_type # Trauma or arthro
    @property
    def ortho_procedure_name( self )                    -> Opt[str]:    return self._ortho_procedure_name
    @property
    def epic_start_time( self )                         -> Opt[str]:    return self._epic_start_time
    @property   
    def epic_end_time( self )                           -> Opt[str]:    return self._epic_end_time
    @property
    def side_of_patient_body( self )                    -> Opt[str]:    return self._side_of_patient_body
    @property
    def OR_location( self )                             -> Opt[str]:    return self._OR_location
    @property
    def supervising_surgeon_hawk_id( self )             -> Opt[str]:    return self._supervising_surgeon_hawk_id
    @property
    def supervising_surgeon_presence( self )            -> Opt[str]:    return self._supervising_surgeon_presence # Present, retrospective, other
    @property
    def performing_surgeon_hawk_id( self )              -> Opt[str]:    return self._performing_surgeon_hawk_id
    @property
    def performer_year_in_residency( self )             -> Opt[int]:    return self._performer_year_in_residency
    @property
    def performer_was_assisted( self )                  -> Opt[bool]:   return self._performer_was_assisted
    @property
    def performer_num_of_similar_logged_cases( self )   -> Opt[int]:    return self._performer_num_of_similar_logged_cases
    @property
    def performance_enumerated_task_performer( self )   -> Opt[dict]:   return self._performance_enumerated_task_performer
    @property
    def unusual_features_present( self )                -> Opt[bool]:   return self._unusual_features_present
    @property
    def list_unusual_features_of_performance( self )    -> Opt[str]:    return self._list_unusual_features_of_performance
    @property
    def diagnostic_notes( self )                        -> Opt[str]:    return self._diagnostic_notes
    @property
    def misc_surgical_performance_comments( self )      -> Opt[str]:    return self._misc_surgical_performance_comments # body habitus, pre-existing conditions, specific technical struggles, damage to tissue, non-technical issues, anything that happened before/after the procedure began 
    
    @property
    def assessment_title( self )    -> Opt[str]:    return self._assessment_title
    @property
    def assessor_hawk_id( self )    -> Opt[str]:    return self._assessor_hawk_id
    @property
    def assessment_details( self )  -> Opt[str]:    return self._assessment_details

    @property
    def name_of_storage_device( self )          -> Opt[str]:    return self._storage_device_name_and_type
    @property
    def radiology_contact_date( self )          -> Opt[str]:    return self._radiology_contact_date
    @property
    def radiology_contact_time( self )          -> Opt[str]:    return self._radiology_contact_time
    @property
    def relevant_folder( self )                 -> Opt[str]:    return self._relevant_folder
    