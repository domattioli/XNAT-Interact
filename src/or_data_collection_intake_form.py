import json
import os
import glob
import re
from typing import Optional as Opt, Tuple, Union
import cv2
import io
import base64
import numpy as np
import requests
import hashlib
import pandas as pd

from datetime import datetime, date as dtdate, time as dttime
from dateutil import parser
import pytz

import uuid

from pydicom.dataset import FileDataset as pydicomFileDataset
from pydicom import Dataset, Sequence, dcmread, dcmwrite, uid as dcmUID


from typing import List, Dict, Any, Tuple, Optional as Opt, Union

import shutil
import tempfile

from src.utilities import LibrarianUtilities, MetaTables, USCentralDateTime, XNATLogin, XNATConnection

acceptable_ortho_procedure_names = { '1a': 'Shoulder_Arthroscopy', '1b': 'KNEE_ARTHROSCOPY', '1c': 'Hip_ARTHROSCOPY', '1d': 'Ankle_ARTHROSCOPY',
                                    '2a': 'Open_reduction_hip_fracture–Dynamic_hip_screw', '2b': 'Open_reduction_hip_fracture–Cannulated_hip_screw',
                                    '2c': 'Closed_reduction_hip_fracture–Cannulated_hip_screw', '2d': 'Percutaneous_sacroliac_fixation',
                                    '2e': 'Open_and_percutaneous_pilon_fractures', '2f': 'Intramedullary_nail-CMN',
                                    '2g': 'Intramedullary_nail-Antegrade_femoral', '2h': 'Intramedullary_nail-Retrograde_femoral',
                                    '2i': 'Intramedullary_nail-Tibia', '2j': 'Scaphoid_Fracture',
                                    '3a': 'PEDIATRIC_SUPRACONDYLaR_HUMERUS_FRACTURE_reduction_and_pinning', '4a': 'Other' }
options_str = "\n".join( [f"Enter '{code}' for {name.replace('_', ' ')}" for code, name in acceptable_ortho_procedure_names.items()] )

                    
class ORDataIntakeForm():
    def __init__( self, metatables: MetaTables, login: XNATLogin ):
        self._metatables = metatables
        assert self.metatables.is_user_registered( login.validated_username ), f'User with HAWKID {login.validated_username} is not registered in the system!'

        self._welcome_message()
        self._filer_name_and_operation_date_user_prompts()
        self._surgical_procedure_info_user_prompts()
        self._skills_assessment_user_prompts()
        self._storage_device_user_prompts()
        self._create_text_file_reconstruction()

    def _welcome_message( self ):
        self._running_text_file = '-----'*5 + f'\nOR Data Intake Form\n' + '-----'*5 + '\n\n'
        print( self.running_text_file )


    def _filer_name_and_operation_date_user_prompts( self ):
        filer_hawkid = input( 'Please enter the HAWKID of the Form Filer:\t' )
        assert self.metatables.is_user_registered( filer_hawkid ), f'User with HAWKID {filer_hawkid} is not registered in the system!'
        self._filer_name = filer_hawkid.upper()
        operation_date = parser.parse( input( 'Please enter the Operation Date (YYYY-MM-DD):\t' ) ).date()
        assert isinstance( operation_date, dtdate ), 'Invalid Operation Date! Must enter a valid date in the format YYYY-MM-DD.'
        self._performance_date = operation_date

        self._running_text_file += f'Filer Name: {self.filer_name}\nOperation Date: {self.performance_date}\n\n'
    

    def _surgical_procedure_info_user_prompts( self ): # Make sure fields that might be stored in the metatables are all completely capitalized
        header_str = f'\nSurgical Procedure Information ---\n'
        print( header_str )
        self._running_text_file += header_str

        acceptable_institutions = self.metatables.list_of_all_items_in_table( table_name='ACQUISITION_SITES' )
        institution_name = input( f'\tInstitution Name (Please Copy-and-Paste from the list below):\n\t{acceptable_institutions}\n\tAnswer:\t' )
        assert self.metatables.item_exists( 'Acquisition_Sites', institution_name ), f'Institution {institution_name} is invalid/not registered in the system. Here are a list of valid institutions:\n{self.metatables.list_of_all_items_in_table( "Acquisition_Sites" )}'
        self._institution_name = institution_name.upper()
        self._running_text_file += f'Institution Name:\t{self.institution_name}\n'

        ortho_procedure_type = input( '\t-- Procedure Type --\n\tEnter "1" for Trauma or "2" for Arthro:\t' )
        assert ortho_procedure_type in ['1', '2'], 'Invalid Procedure Type! Please only enter "1" for Trauma or "2" for Arthro.'
        if ortho_procedure_type == '1':     self._ortho_procedure_type = 'Trauma'.upper()
        elif ortho_procedure_type == '2':   self._ortho_procedure_type = 'Arthroscopy'.upper()
        self._running_text_file += f'Procedure Type:\t{self.ortho_procedure_type}\n'

        ortho_procedure_name = input( f'\t-- Procedure Name --\n{options_str}\n\t' )
        assert ortho_procedure_name in acceptable_ortho_procedure_names.keys(), 'Invalid entry for Procedure Name!'
        self._ortho_procedure_name = ortho_procedure_name.upper()
        self._running_text_file += f'Procedure Name:\t{acceptable_ortho_procedure_names[ortho_procedure_name]}\n'

        epic_start_time = parser.parse( input( '\tEpic Start Time (HH:MM):\t' ) ).time()
        epic_end_time = parser.parse( input( '\tEpic End Time (HH:MM):\t' ) ).time()
        assert epic_start_time < epic_end_time, f'Invalid Epic Start and End Times! Start Time must be before End Time. You entered {epic_start_time} and {epic_end_time}.'
        self._epic_start_time, self._epic_end_time = epic_start_time, epic_end_time
        self._running_text_file += f'Epic Start Time:\t{str( self.epic_start_time )}\nEpic End Time:\t\t{str( self.epic_end_time )}\n'

        patient_side = input( "\t-- Side of Patient's Body --\n\tEnter '1' for Right, '2' for Left, or '3' for N/A:\t" )
        assert patient_side in ['1', '2', '3'], f'Invalid Side of Patient Body! Please only enter only 1, 2, or 3. You entered: {patient_side}'
        if patient_side == '1':     self._side_of_patient_body = 'Right-side'.upper()
        elif patient_side == '2':   self._side_of_patient_body = 'Left-side'.upper()
        else: raise ValueError( 'You indicated N/A for the side of the patient body; please contact the data librarian to clarify this before proceding!' )
        self._running_text_file += f"Side of Patient's Body:\t{self.side_of_patient_body}\n"

        self._OR_location = input( '\tOperating Room Location:\t' )
        self._running_text_file += f'Operating Room Location:\t{self.OR_location}\n'

        supervising_surgeon_hawk_id = input( "\tSupervising Surgeon's HAWKID:\t" )
        assert self.metatables.item_exists( table_name='Surgeons', item_name=supervising_surgeon_hawk_id ), f'HAWKID {supervising_surgeon_hawk_id} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{self.metatables.list_of_all_items_in_table( "Surgeons" )}'
        supervising_surgeon_presence = input( "\t-- Supervising Surgeon Presence --\n\tEnter '1' for Present, '2' for Retrospective Review, or '3' for Other:\t" )
        assert supervising_surgeon_presence in ['1', '2', '3'], f'Invalid Supervising Surgeon Presence! Please only enter "1" for Present, "2" for Retrospective Review, or "3" for Other. You entered: {supervising_surgeon_presence}'
        if supervising_surgeon_presence == '1':     self._supervising_surgeon_presence = 'Present'.upper()
        elif supervising_surgeon_presence == '2':   self._supervising_surgeon_presence = 'Retrospective Review'.upper()
        else: raise ValueError( 'You indicated other for the supervision surgeon presence; please contact the data librarian to clarify this before proceding!' )
        self._supervising_surgeon_hawk_id, self._supervising_surgeon_presence = self.metatables.get_uid( 'Surgeons', supervising_surgeon_hawk_id ), supervising_surgeon_presence
        self._running_text_file += f"Supervising Surgeon's HAWKID:\t{self.supervising_surgeon_hawk_id}\nSupervising Surgeon Presence:\t{self.supervising_surgeon_presence}\n"

        performing_surgeon_hawk_id = input( "\tPerforming Surgeon's HAWKID:\t" )
        assert self.metatables.item_exists( table_name='Surgeons', item_name=performing_surgeon_hawk_id ), f'HAWKID {performing_surgeon_hawk_id} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{self.metatables.list_of_all_items_in_table( "Surgeons" )}'
        performer_year_in_residency = input( '\tPerforming Surgeon\'s Years in Residency:\t' )
        assert performer_year_in_residency.isdigit(), 'Invalid entry for Performing Surgeon\'s Years in Residency! Must be an integer.'
        known_number_of_similar_logged_cases = input( '\tDo you know how many similar prior cases have been logged by the performing surgeon?\n\tEnter "1" for Yes or "2" for No\n\t\t***NOTE: 0 prior cases is not the same thing as unknown!!***Answer:\t' )
        assert known_number_of_similar_logged_cases in ['1', '2'], f'Invalid entry for Known # of Similar Cases Logged! Please only enter "1" for Yes or "2" for No. You entered: {known_number_of_similar_logged_cases}'
        if known_number_of_similar_logged_cases == '1':
            performer_num_of_similar_logged_cases = input( '\tPerforming Surgeon\'s # of Similar Cases Logged (if zero, enter "0", if unknown, press "ctrl-c" because you answered the previous question wrong):\t' )
            assert performer_num_of_similar_logged_cases.isdigit(), f'Invalid entry for Performing Surgeon\'s Number of Similar Logged Cases! Must be an integer. You entered '
            self._performer_num_of_similar_logged_cases = int( performer_num_of_similar_logged_cases )
        else:                                               self._performer_num_of_similar_logged_cases = None
        self._performing_surgeon_hawk_id, self._performer_year_in_residency = self.metatables.get_uid( 'Surgeons', performing_surgeon_hawk_id ), int( performer_year_in_residency )
        self._running_text_file += f"Performing Surgeon's HAWKID:\t{self.performing_surgeon_hawk_id}\nYears in Residency:\t{str(self.performer_year_in_residency)}\n# of Similar Cases Logged:\t{str(self.performer_num_of_similar_logged_cases)}\n"

        performer_was_assisted = input( '\tWas the Performing Surgeon Assisted?\n\tEnter "1" for Yes or "2" for No:\t' )
        if performer_was_assisted == '1':
            self._performer_was_assisted, self._performance_enumerated_task_performer = True, self._prompt_user_for_n_surgical_tasks_and_hawkids()
        elif performer_was_assisted == '2':
            self._performer_was_assisted, self._performance_enumerated_task_performer = False, None
        else:   raise ValueError( f'Invalid entry for Performing Surgeon Assisted! Please only enter "1" for Yes or "2" for No. You entered {performer_was_assisted}' )
        self._running_text_file += f'Performing Surgeon Assisted:\t{str( self.performer_was_assisted )}\n\tEnumerated Procedure Tasks and Performers:\n\t\t{self.performance_enumerated_task_performer}\n'

        any_unusual_features_of_performance = input( '\tWere there any unusual features of the performance?\n\tEnter "1" for Yes or "2" for No:\t' )
        if any_unusual_features_of_performance == '1':
            self._unusual_features_present = True
            self._list_unusual_features_of_performance = input( '\tPlease list out any unusual features of the performance:\n\t' )
        elif any_unusual_features_of_performance == '2':
            self._unusual_features_present, self._list_unusual_features_of_performance = False, None
        else:   raise ValueError( f'Invalid entry for Unusual Features of Performance! Please only enter "1" for Yes or "2" for No. You entered {any_unusual_features_of_performance}' )
        self._running_text_file += f'Unusual Features Present:\t{str( self.unusual_features_present )}\n\tList of Unusual Features:\n\t\t{self.list_unusual_features_of_performance}\n'

        if self.ortho_procedure_type == 'Arthroscopy':
            diagnostic_notes = input( '\tPlease enter any diagnostic notes about the surgical procedure:\n\t' )
            self._diagnostic_notes = diagnostic_notes
        else:
            self._diagnostic_notes = None
        self._running_text_file += f'Diagnostic Comments/Notes (Arthroscopy only):\t{self.diagnostic_notes}\n'

        misc_comments = input( '\tDo you have any additional comments or notes regarding BMI, pre-existing conditions, etc.?\n\tEnter "1" for Yes or "2" for No:\t' )
        if misc_comments == '1':
            self._misc_surgical_performance_comments = input( '\tPlease enter any additional comments or notes:\n\t' )
        elif misc_comments == '2': self._misc_surgical_performance_comments = None
        else:   raise ValueError( f'Invalid entry for Additional Comments! Please only enter "1" for Yes or "2" for No. You entered {misc_comments}' )
        self._running_text_file += f'Miscellaneous Procedure Comments:\t{self.misc_surgical_performance_comments}\n'
    
    def _prompt_user_for_n_surgical_tasks_and_hawkids( self ) -> str:
        num_tasks = int( input( '\tHow many surgeons participated in the procedure?\n\tEnter an integer:\t' ) )
        assert num_tasks > 0, 'Invalid number of surgeons! Must be a positive integer.'
        task_performers = ''
        for i in range( num_tasks ):
            if i == 0:
                hawkid = input( f'1st HAWKID:\t' )
            elif i == 1:
                hawkid = input( f'2nd HAWKID:\t' )
            elif i == 2:
                hawkid = input( f'3rd HAWKID:\t' )
            else:
                hawkid = input( f'{i+1}th HAWKID:\t' )
            assert self.metatables.item_exists( table_name='Surgeons', item_name=hawkid ), f'HAWKID {hawkid} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{self.metatables.list_of_all_items_in_table( "Surgeons" )}'
            surgeon_tasks = input( f'Enter the task(s) performed by {hawkid}:\t' )
            task_performers += f"\t\t{self.metatables.get_uid(table_name='SURGEONS', item_name=hawkid)} -- {surgeon_tasks}\n"
        return task_performers

    def _skills_assessment_user_prompts( self ):
        header_str = f'\n\nSkills Assessment Information ---\n'
        print( header_str )
        self._running_text_file += header_str
        
        assessment_requested = input( '\tDid the performing surgeon request a Skills Assessment for this procedure?\n\tEnter "1" for Yes or "2" for No:\t' )
        if assessment_requested == '1':
            assessment_requested, assessment_title = True, input( '\tPlease enter the full name of the requested assessment:\t' )
            assessor_name = input( '\tPlease enter the HAWKID of the assessor:\t' )
            assert self.metatables.item_exists( table_name='Surgeons', item_name=assessor_name ), f'HAWKID {assessor_name} is not a registered surgeon in the system. Please enter a valid HAWKID.\nRegistered surgeon hawkids:\n{self.metatables.list_of_all_items_in_table( "Surgeons" )}'
            known_details = input( '\tAny additonal relevant details known about the assessment (date, score, etc.)?\n\tEnter "1" for Yes or "2" for No:\t ')
            if known_details == '1':
                self._assessment_details = input( '\tPlease enter any additional details about the assessment:\t' )
            else:
                self._assessment_details = None
            self._assessment_title, self._assessor_hawk_id = assessment_title, assessor_name
        elif assessment_requested == '2':
            assessment_requested, self._assessment_title, self._assessor_hawk_id, self._assessment_details = False, None, None, None
        else:   raise ValueError( f'Invalid entry for Skills Assessment! Please only enter "1" for Yes or "2" for No. You entered {assessment_requested}' )
        self._running_text_file += f'Assessment Requested:\t{assessment_requested}\nAssessment Title:\t{self.assessment_title}\nAssessor HAWKID:\t{self.assessor_hawk_id}\nAssessment Details:\t{self.assessment_details}\n'


    def _storage_device_user_prompts( self ):
        header_str = f'\n\nStorage Device Information ---\n'
        print( header_str )
        self._running_text_file += header_str
        
        self._storage_device_name_and_type = input( '\tPlease enter the name and type of the storage device:\t' )
        self._relevant_folder_and_file_names = input( '\tPlease enter the name of the relevant folder and file:\t' )

        radiology_contacted = input( '\tWas radiology contacted for this procedure?\n\tEnter "1" for Yes or "2" for No:\t' )
        if radiology_contacted == '1':
            radiology_contact_date = parser.parse( input( '\tRadiology Contact Date (YYYY-MM-DD):\t' ) ).date()
            radiology_contact_time = parser.parse( input( '\tRadiology Contact Time (HH:MM):\t' ) ).time()
            self._radiology_contact_date, self._radiology_contact_time = radiology_contact_date, radiology_contact_time
        elif radiology_contacted == '2':
            self._radiology_contact_date, self._radiology_contact_time = None, None
        else:   raise ValueError( f'Invalid entry for Radiology Contacted! Please only enter "1" for Yes or "2" for No. You entered {radiology_contacted}' )
        self._running_text_file += f'Storage Device Name and Type:\t{self.name_of_storage_device}\nRadiology Contact Date:\t{self.radiology_contact_date}\nRadiology Contact Time:\t{self.radiology_contact_time}\nRelevant Folder and File Names:\t{self.relevant_folder_and_file_names}\n'
        

    def _create_text_file_reconstruction( self ):
        save_ffn = os.path.join( self.metatables.tmp_data_dir, 'OR_DATA_INTAKE_FORM-tmp.txt' )
        with open( save_ffn, 'w' ) as f:
            f.write( self.running_text_file )

            


    @property
    def metatables( self )          -> MetaTables:  return self._metatables

    @property
    def running_text_file( self )   -> str:         return self._running_text_file

    @property
    def filer_name( self )                              -> str:     return self._filer_name
    @property
    def performance_date( self )                        -> dtdate:  return self._performance_date
    @property
    def institution_name( self )                        -> str:     return self._institution_name
    @property
    def ortho_procedure_type( self )                    -> str:     return self._ortho_procedure_type # Trauma or arthro
    @property
    def ortho_procedure_name( self )                    -> str:     return self._ortho_procedure_name
    @property
    def epic_start_time( self )                         -> dttime:  return self._epic_start_time
    @property
    def epic_end_time( self )                           -> dttime:  return self._epic_end_time
    @property
    def side_of_patient_body( self )                    -> str:     return self._side_of_patient_body
    @property
    def OR_location( self )                             -> str:     return self._OR_location
    @property
    def supervising_surgeon_hawk_id( self )             -> str:     return self._supervising_surgeon_hawk_id
    @property
    def supervising_surgeon_presence( self )            -> str:     return self._supervising_surgeon_presence # Present, retrospective, other
    @property
    def performing_surgeon_hawk_id( self )              -> str:     return self._performing_surgeon_hawk_id
    @property
    def performer_year_in_residency( self )             -> int:     return self._performer_year_in_residency
    @property
    def performer_was_assisted( self )                  -> bool:    return self._performer_was_assisted
    @property
    def performer_num_of_similar_logged_cases( self )   -> Opt[int]:return self._performer_num_of_similar_logged_cases
    @property
    def performance_enumerated_task_performer( self )   -> Opt[str]:return self._performance_enumerated_task_performer
    @property
    def unusual_features_present( self )                -> bool:    return self._unusual_features_present
    @property
    def list_unusual_features_of_performance( self )    -> Opt[str]:return self._list_unusual_features_of_performance
    @property
    def diagnostic_notes( self )                        -> Opt[str]:return self._diagnostic_notes
    @property
    def misc_surgical_performance_comments( self )      -> Opt[str]:return self._misc_surgical_performance_comments # body habitus, pre-existing conditions, specific technical struggles, damage to tissue, non-technical issues, anything that happened before/after the procedure began 
    
    @property
    def assessment_title( self )    -> Opt[str]:    return self._assessment_title
    @property
    def assessor_hawk_id( self )    -> Opt[str]:    return self._assessor_hawk_id
    @property
    def assessment_details( self )  -> Opt[str]:    return self._assessment_details

    @property
    def name_of_storage_device( self )          -> str:         return self._storage_device_name_and_type
    @property
    def radiology_contact_date( self )          -> Opt[dtdate]: return self._radiology_contact_date
    @property
    def radiology_contact_time( self )          -> Opt[dttime]: return self._radiology_contact_time
    @property
    def relevant_folder_and_file_names( self )  -> str:         return self._relevant_folder_and_file_names
    