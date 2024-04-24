from pyxnat import Interface, schema
import pydicom
from pydicom.dataset import FileDataset
from pydicom.dataelem import DataElement
from pydicom.datadict import dictionary_VR, dictionary_has_tag

# import getpass, sys, csv, string
import os
import zipfile
from pathlib import Path, PurePosixPath
import glob
import warnings
import json
import shutil
import tempfile

# from abc import ABC, abstractmethod
from typing import Optional as Opt
from typing import Union, Tuple

from datetime import datetime
import pandas as pd
import numpy as np
import cv2

# def main( arg1, arg2 ):
#     print(f'Argument 1: {arg1}, Argument 2: {arg2}')

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Process some arguments.')
#     parser.add_argument('--arg1', type=str, help='Description for arg1')
#     parser.add_argument('--arg2', type=str, help='Description for arg2')

#     args = parser.parse_args()

#     main(args.arg1, args.arg2)
    
xnat_project_name = 'domSandBox'

doc_dir = os.path.join( os.path.dirname(os.getcwd()), 'doc' )
cataloged_resources_ffn = os.path.join( doc_dir, r'cataloged_resources.json' )
template_ffn = r'C:\Users\dmattioli\Projects\XNAT\src\unwanted_dcm_image_template.png'
template_img = cv2.imread( template_ffn, 0 ).astype( np.uint8 )


class CatalogedData:
    '''
    For now let's just use a json file. It would make more sense in the future to use a simple relational database, though.
    '''
    def __init__( self, ffn: str ):
        if os.path.isfile( cataloged_resources_ffn ):
            with open( cataloged_resources_ffn, 'r' ) as f:
                self.__raw = json.load( f )
        else:
            self.__raw = { 'acquisition_sites' : ['UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS', 'UNIVERSITY_OF_HOUSTON' ], 
                            'groups' : ['PEDIATRIC_SUPRACONDYLAR_HUMERUS_FRACTURE', 'DYNAMIC_HIP_SCREW', 'KNEE_ARTHROSCOPY'],
                            'subject_uids': []
                            }
            with open( cataloged_resources_ffn, 'w' ) as f:
                json.dump( self.__raw, f )
        self.__acquisition_sites = self.__raw['acquisition_sites']
        self.__groups = self.__raw['groups']
        self.__subject_uids = self.__raw['subject_uids']
        self.__xnat_project_name = xnat_project_name

    def catalog_new_item( self, type_name: str, value: str ) -> bool:
        #  we don't want to change the file just yet. to-do: collection routine once new items are confirmed as valid
        if type_name in self.__raw.keys():
            self.__raw[type_name].append( value )
        else:
            self.__raw[type_name] = [value]
            if type_name == 'acquisition_sites':
                self.__acquisition_sites = self.__raw['acquisition_sites']
            elif type_name == 'groups':
                self.__groups = self.__raw['groups']
            elif type_name == 'subject_uids':
                self.__subject_uids = self.__raw['subject_uids']
        # with open( cataloged_resources_ffn, 'w' ) as f:
        #     json.dump( self.__raw, f )
        
    def __str__( self ) -> str:
        return f'Cataloged Data for {self.__xnat_project_name}\n\tAcquisition Sites:\t{self.__acquisition_sites}\n\tGroups:\t\t\t{self.__groups}\n\tSubject UIDs:\t\t{self.__subject_uids}'

catalog = CatalogedData( cataloged_resources_ffn )


class DeIdentifiedDicom:
    def __init__( self, ffn: str ):
        self.data, self.is_valid = None, 0
        assert os.path.isfile( ffn ), f'Inputted file not found: {ffn}'
        try:
            self.__dcm = pydicom.dcmread( ffn )
        except Exception:
            warnings.warn( f'File cannot be read by pydicom.dcmread: {ffn};\nignoring the file.' )
            return
        self.__template_img = template_img
        self.is_valid, self.data = self.deidentify_dicom( self.__dcm.copy() )
        self.institution = self.extract_institution_information()
        self.date, self.time = self.derive_date_and_time()
    
    def person_names_callback( self, dataset, data_element ):
        if data_element.VR == "PN":
            data_element.value = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"

    def curves_callback( self, dataset, data_element ):
        if data_element.tag.group & 0xFF00 == 0x5000:
            del dataset[data_element.tag]

    def template_image_matching( self, img: np.ndarray, thresh: Opt[float] = 0.90 ) -> bool:
        min_val, _, _, _ = cv2.minMaxLoc( cv2.matchTemplate( img, template_img, cv2.TM_CCOEFF_NORMED ) )
        if min_val < thresh:
            return False
        return True

    def deidentify_dicom( self, dataset: FileDataset ) -> Tuple[bool, FileDataset]:
        dataset.walk( self.person_names_callback )
        dataset.walk( self.curves_callback )
        dataset.remove_private_tags()

        # Remove all overlay data
        for i in range( 0x6000, 0x60FF, 2 ):
            tag = (i, 0x3000)
            if tag in dataset:
                del dataset[tag]

        # Check image for identifiable information  inf the image -- if it matches a template then no-go
        #  to-do: with a gpu we could use a more advanced approach like ocr and simply blur the text within the image.
        pa = dataset.pixel_array
        matches_template = self.template_image_matching( pa.copy().astype( np.uint8 ) )
        if matches_template:
            return False, dataset
        else:
            return True, dataset
    
    def extract_institution_information( self ) -> list:
        out = []
        if 'InstitutionName' in self.data:
            if not self.data.InstitutionName: # If it isn't empty, store the source
                out.append( self.data.InstitutionName )
        if 'IssuerOfPatientID' in self.data:
            if not self.data.IssuerOfPatientID:
                out.append( self.data.IssuerOfPatientID )
    
    def derive_date_and_time( self ) -> Tuple[str, str]:
        # return the content date and content time
        if 'InstanceCreationDate' in self.data:
            return self.data.InstanceCreationDate, self.data.InstanceCreationTime
        elif 'ContentDate' in self.data:
            return self.data.ContentDate, self.data.ContentTime
        else:
            raise ValueError( 'No reliable/relevant date or time information found in the dicom file.' )


def connect_to_xnat( login: dict ) -> Interface:
    assert all( k in login for k in ['User', 'Pw', 'Url']), "Dict of login info must contain a username, password, and the project's 'url' vis a vis rpacs."
    server = Interface( server=login['Url'], user=login['User'], password=login['Pw'] )
    if server is None:
        raise ValueError( 'Could not connect to XNAT server; invalid credentials, maybe.' )
    return server


# currently unused function:
def check_if_study_instance_exists( dicom_data: pydicom.Dataset, cataloged_data ) -> bool:
    return study_instance_uid in cataloged_data['subject_uids']


def retrieve_all_subjects_in_project( xnat: Interface ) -> pd.DataFrame:
    out = xnat.select( 'xnat:subjectData', [
    'xnat:subjectData/SUBJECT_ID', 'xnat:subjectData/SUBJECT_LABEL',
    'xnat:subjectData/GROUP'] ).where(
        [('xnat:subjectData/PROJECT', 'LIKE', xnat_project_name )] ).dumps_json()
    out = json.loads( out )
    subjects_df = pd.DataFrame( out )
    return subjects_df


def get_uids( dicom_data: FileDataset ) -> dict:
    uids = []
    for element in dicom_data.iterall():
        if "UID" in element.name:
            uids.append( element.value.replace( '.', '_' ) ) # Must replace '.' with underscores because that is how theyre stored in xnat
    return uids


def is_eligible_dicom_extension( ffn: str ) -> bool:
    _, ext = os.path.splitext( ffn )
    return ext == '' or ext == '.dcm'
    

def precheck_candidate_case( ffn: str, xnat: Interface ) -> Tuple[bool, list]:
    '''
    Pre-check case by cross-referencing all uids across all files in the directory against
    all existing subject_labels.
    '''

    # Get all files in candidate case
    assert os.path.isdir( ffn ), f"{ffn} must correspond to a valid directory of dicoms."
    all_files = glob.glob( os.path.join( ffn, '**', '*' ), recursive=True )

    # Get list of all dicom files' uid info -- note there may be a few uids
    list_of_existing_subj_labels = retrieve_all_subjects_in_project( xnat )
    uids_to_check = []
    for f in all_files:
        if is_eligible_dicom_extension( f ):
            uids_to_check.extend( get_uids( pydicom.dcmread( f ) ) )
    uids_to_check = list( set( uids_to_check ) )

    # Get all subjects and cross-reference. Return the subjects found
    matches = list_of_existing_subj_labels[list_of_existing_subj_labels['subject_label'].isin( uids_to_check )]
    matched_values = matches['subject_label'].unique()
    if not len( matched_values ):
        return True, all_files, matched_values
    else:
        return False, all_files, []
    

def check_any_eligible_files( shots_df: pd.DataFrame ) -> bool:
    # Invalid only when empty or all shots are invalid
    return len( shots_df ) > 0 and shots_df['valid'].any()


def read_shots( all_files: list, login: dict ) -> pd.DataFrame:
    '''
     return a dataframe of all files prepared/checked for import
    '''
    
    shots = pd.DataFrame( index = range( len( all_files ) ), columns = ['fn', 'ext', 'new_fn', 'date', 'time', 'valid', 'dicom'] )
    # shots.attrs['Path'] = os.path.basename( ffn )
    for idx, _ in shots.iterrows():
        shots.at[idx,'fn'], shots.at[idx,'ext'] = os.path.splitext( os.path.basename( all_files[idx] ) )
        de_id_dcm = DeIdentifiedDicom( all_files[idx] )
        shots.at[idx,'dicom'], shots.at[idx,'valid'] = de_id_dcm.data, de_id_dcm.is_valid
        if shots.at[idx,'valid'] is True:
            shots.at[idx,'date'], shots.at[idx,'time'] = de_id_dcm.date, de_id_dcm.time

    # if all date values are the same, assign thatto the series
    assert shots['date'].nunique() == 1, 'BUG in Code: Method for finding the date is wrong -- returned different dates for same case.'
    shots.attrs['date'] = shots.at[0,'date']
    
    # if all of the StudySeriesUID values are not the same then we need to generate a new uid for the series
    # ids = shots['dicom'].apply( lambda dicom: dicom.StudyInstanceUID )
    ids = [] #to-do: make this a list comprehension to mimic the line above, which does not work if a non dicom (eg .json) file is present in the directory
    for idx, row in shots.iterrows():
        if row['valid']:
            ids.append( row['dicom'].StudyInstanceUID )
    ids = pd.Series( ids )
    if ids.nunique() == 1:
        shots.attrs['label'] = ids.at[0]
    else:
        shots.attrs['label'] = pydicom.uid.generate_uid( prefix=None, entropy_srcs=[shots.attrs['date']] )
        shots = deal_with_inconsistent_study_instance_uid( shots, shots.attrs['label'], login )

    # Sort rows according to time; fill in new file name according to sorted time
    shots.sort_values( by='time', inplace=True, na_position='last' )
    return generate_file_names( shots )


def generate_file_names( shots: pd.DataFrame ) -> pd.DataFrame:
    '''
    '''
    assert len( shots ) < 1000, 'More than 999 shots discovered for performance -- this will break the code due to leading digits; maintenance needed.'
    num_digits = 3 # to-do: figure out what happens when this is insufficeint for the subject naming convention.
    shots['new_fn'] = shots.apply( lambda s: f"{str( s.name+1 ).zfill( num_digits )}" if s['valid'] else '', axis=1 )
    return shots


def deal_with_inconsistent_study_instance_uid( shots: pd.DataFrame, new_study_uid: str, login: dict ) -> pd.DataFrame:
    '''
    '''
    for idx, _ in shots.iterrows():
        if not shots.at[idx,'valid']:
            continue
        # Copy the value for 'StudyInstanceUID' to a new private tag; add new private tags detailing this change
        description = "0x0019,0x1001: Copied original inconsistent StudyInstanceUID upon upload to XNAT, 0x0019,0x1002: Date of Edit, 0x0019,0x1003: Author of Edit"
        shots.at[idx,'dicom'].add_new((0x0019, 0x1001), 'LO', shots.at[idx,'dicom'].StudyInstanceUID )
        shots.at[idx,'dicom'].add_new( (0x0019, 0x1002), 'DA', datetime.today().strftime('%Y%m%d') )
        shots.at[idx,'dicom'].add_new( (0x0019, 0x1003), 'LO', ['Added by: ' + login['User']] )
        shots.at[idx,'dicom'].add_new((0x0019, 0x1004), 'LO', description )

        # Insert new study id in place
        shots.at[idx,'dicom'].StudyInstanceUID = new_study_uid
    return shots


def write( zip_dest: str, shots: pd.DataFrame ) -> str:
    '''
    '''

    assert os.path.isdir( zip_dest ), 'Destination for zipped folder must be an existing directory.'
    write_d = os.path.join( zip_dest, shots.attrs['label'] )
    with tempfile.TemporaryDirectory() as tmp_dir:
        for idx, row in shots.iterrows():
            if row['valid']:
                pydicom.dcmwrite( os.path.join( tmp_dir, row['new_fn'] ), row['dicom'] )

        # Write the folder to a zip file in the destination directory
        shutil.make_archive( write_d, 'zip', tmp_dir )
    return write_d + '.zip'


def create_new_rf_session( shots: pd.DataFrame, zipped_ffn: str, xnat: Interface ) -> Tuple[bool, str]:
    '''
    '''

    try: # Create query strings and select object in xnat then create the relevant objects
        exp_label = ( 'Source_Images' + '-' + shots.attrs['label'] ).replace( '.', '_' )
        scan_label = 'Original'
        type_label = 'DICOM'
        resource_label = 'Raw'
        proj_qs = '/project/' + xnat_project_name
        proj_qs = PurePosixPath( proj_qs ) # need to revisit this because it is hard-coded but Path makes it annoying
        subj_qs = proj_qs / 'subject' / str( shots.attrs['label'] ).replace( '.', '_' )
        exp_qs = subj_qs / 'experiment' / exp_label
        scan_qs = exp_qs / 'scan' / scan_label
        files_qs = scan_qs / 'resource' / 'files'
        subj_inst = xnat.select( str( subj_qs ) )
        exp_inst = xnat.select( str( exp_qs ) )
        scan_inst = xnat.select( str( scan_qs ) )
        project_inst = xnat.select( str( proj_qs ) )

        # Create stepwise - can't figure out how to create all in one go instead of attrs.mset() (it wouldn't work properly -- to:do)
        subj_inst.create() # works, but following lines dont
        subj_inst.attrs.mset( {
        'xnat:subjectData/GROUP': shots.attrs['group']
        })
        exp_inst.create( **{
        'experiments':'xnat:rfSessionData',
        })
        exp_inst.attrs.mset(
        {  'xnat:experimentData/ACQUISITION_SITE': shots.attrs['source'],
            'xnat:experimentData/DATE': shots.attrs['date'] }
        )
        scan_inst.create( **{
        'scans':'xnat:rfScanData',
        })
        scan_inst.attrs.mset( {
        'xnat:imageScanData/TYPE': type_label
        })
        scan_inst.resource( resource_label ).put_zip( zipped_ffn )
        return True, f"({shots['valid'].sum()}/{len( shots )}) shots successfully uploaded to XNAT!"
    except Exception as e:
        return False, f'Error in creating new rf session:\n{e}'
    

def pushing_sub_routine( all_files: list,
                        zip_dest_dir: str,
                        login: dict,
                        group: str,
                        source: str,
                        xnat: Interface,
                        ) -> Tuple[bool, pd.DataFrame, str, str]:
    '''
    '''

    shots_df = read_shots( all_files, login )
    is_valid = check_any_eligible_files( shots_df )
    shots_df.attrs['group'], shots_df.attrs['source'] = group, source

    # If valid, write as a folder as a zip file then push to xnat.
    if is_valid:
        zipped_ffn = write( zip_dest_dir, shots_df )
        success, msg = create_new_rf_session( shots_df, zipped_ffn, xnat )
    else:
        success, msg = False, 'Error: Could write/push this file to xnat.'
    return success, shots_df, zipped_ffn, msg


def push_new_case_to_xnat( source_ffn: str,
                          group: str,
                          source: str,
                          login: dict,
                          xnat: Union[Interface, None],
                          zip_dest_dir: str,
                          delete_zip: Opt[bool] = True
                          ) -> Tuple[bool, pd.DataFrame, str, str]:
    '''
    # To-do Need to account for an input ffn being a zipped folder -- we should just write it directly then?
    '''
    
    # Account for user wanting to do this in batches, i.e., they would want to externally connect to xnat and pass it in.
    if xnat is None:
        close_xnat = True # If it is passed in then it was instantiated externally and we don't need to close it.
        xnat = connect_to_xnat( login )
    else:
        close_xnat = False

    # Retrieve all files in the directory, check if it is valid
    is_valid, all_files, duplicate_subjs = precheck_candidate_case( source_ffn, xnat )
    if is_valid:
        success, shots_df, zipped_ffn, msg = pushing_sub_routine( all_files, zip_dest_dir, login, group, source, xnat )
    else:
        return False, pd.DataFrame(), '', f'Duplicates found in {duplicate_subjs}'

    # Garbage
    if delete_zip and success:
        os.remove( zipped_ffn )
        msg = msg + '\n\t---Local temporary zip file successfully deleted!'
    if close_xnat:
        xnat.disconnect()
    return success, shots_df, zipped_ffn, msg

