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

from datetime import datetime
from dateutil import parser
import pytz

import uuid

from pydicom.dataset import FileDataset as pydicomFileDataset
from pydicom import Dataset, Sequence, dcmread, dcmwrite, uid as dcmUID


from pathlib import Path, PurePosixPath

import matplotlib.pyplot as plt

import shutil
import tempfile

from src.Utilities import LibrarianUtilities, MetaTables, USCentralDateTime, XNATLogin, XNATConnection

# Define list for allowable imports from this module -- do not want to import _local_variables.
# __all__ = ['ImageHash', 'ScanFile', 'SourceDicomDeIdentified', 'ExperimentData', 'MTurkSemanticSegmentation']

#--------------------------------------------------------------------------------------------------------------------------
## Class for generating image hashes to cross reference.
class ImageHash( LibrarianUtilities ):
    def __init__( self, metatables: MetaTables, img: Opt[np.ndarray] = None ):
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
        tst1 = ImageHash( MetaTables( XNatLogin( {...} ) ) ) # computes hash using the template dicom image stored in the LibrarianUtilities attributes.
        tst2 = ImageHash( MetaTables( XNatLogin( {...} ) ), np.uint32( tst1.raw_img ) )
        tst3 = ImageHash( MetaTables( XNatLogin( {...} ) ), np.int16(  tst1.raw_img ) )
        print( tst1 )
        print( tst2 )
        print( tst3 )
        print( 'All hash strings the same:', tst1.hash_str == tst1.hash_str and tst1.hash_str == tst3.hash_str and tst2.hash_str == tst3.hash_str )
        '''
        super().__init__()  # Call the __init__ method of the base class
        self._validate_input( img )
        self._processed_img, self._hash_str,  = self.dummy_image(), ''
        self._meta_tables, self._in_img_hash_metatable = metatables, False
        self._convert_to_grayscale()
        self._normalize_and_convert_to_uint8()
        self._resize_image()
        self._compute_hash_str()
        self._check_img_hash_metatable()
    
    @property
    def raw_img( self ) -> np.ndarray:          return self._raw_img
    @property
    def processed_img( self ) -> np.ndarray:    return self._processed_img
    @property
    def hash_str( self ) -> str:                return self._hash_str
    @property
    def metatables( self ) -> MetaTables:       return self._meta_tables
    @property
    def in_img_hash_metatable( self ) -> bool:   return self._in_img_hash_metatable
    
    def _validate_input( self, img: Opt[np.ndarray] = None ):
        if img is None:
            self._raw_img = self.template_img
        else:
            self._raw_img = img.astype( np.uint64 ).copy()
        assert self.raw_img.dtype in self.acceptable_img_dtypes, f'Bitdepth "{self.raw_img.dtype}" is unsupported; inputted image must be one of: {self.acceptable_img_dtypes}.'
        assert 2 <= self.raw_img.ndim <= 3, f'Inputted image must be a 2D or 3D array.'

    def _convert_to_grayscale( self ):
        if len( self.raw_img.shape ) == 3:  # Ensure that the image is in grayscale
            self._processed_img = np.mean( self.raw_img, axis=2 )
            # self._processed_img = cv2.cvtColor( self.raw_img, cv2.COLOR_BGR2GRAY )
        else:
            self._processed_img = self.raw_img

    def _normalize_and_convert_to_uint8( self ): # Normalize the image to the range 0-255
        self._processed_img = cv2.normalize( self.processed_img, np.zeros( self.processed_img.shape, np.uint8 ), 0, 255, cv2.NORM_MINMAX ).astype( np.uint8 )

    def _resize_image( self ):
        self._processed_img = cv2.resize( self.processed_img, self.required_img_size_for_hashing )
    
    def _compute_hash_str( self ):
        self._hash_str = hashlib.sha256( self.processed_img.tobytes() ).hexdigest() # alternatively: imagehash.average_hash( Image.fromarray( image ) )
        assert self.hash_str is not None and len( self.hash_str ) == 64, f'Hash string must be 64 characters long.'
    
    def _check_img_hash_metatable( self ): # check if it exists in the metatables
        assert self.processed_img.shape == self.required_img_size_for_hashing, f'Processed image must be of size {self.required_img_size_for_hashing} (is currently size {self.processed_img.shape}).'
        self._in_img_hash_metatable = self.metatables.item_exists( table_name='IMAGE_HASHES', item_name=self.hash_str )
    
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


#--------------------------------------------------------------------------------------------------------------------------
## Base class for scan files, designed for inheritence.
class ScanFile( LibrarianUtilities ):
    def __init__( self, metatables: MetaTables ):
        super().__init__()  # Call the __init__ method of the base class
        self._ffn, self._metadata, self._image, self._uid_info, self._metatables = '', None, None, {}, metatables # Derived from the input file
        self._acquisition_site, self._group = '', '' # Derived from metadata -- candidates only
        self._datetime, self._is_valid = None, False

    @property
    def ffn( self ) -> str:                 return self._ffn
    @property
    def metadata( self ) -> Union[pydicomFileDataset, pd.Series]:
        assert self._metadata is not None, f'BUG: cannot be calling the _metadata getter for {type(self).__name__} prior to defining it.'
        return self._metadata
    @property
    def image( self ) -> ImageHash:
        assert self._image is not None, f'BUG: cannot be calling the _image getter for {type(self).__name__} prior to defining it.'
        return self._image
    @property
    def uid_info( self ) -> dict:           return self._uid_info #to-do: create a class for this? the inherited ones have not structure
    @property
    def metatables( self ) -> MetaTables:   return self._metatables
    @property
    def acquisition_site( self ) -> str:    return self._acquisition_site # aka source aka institution
    @property
    def group( self ) -> str:               return self._group # AKA "surgical procedure"
    @property
    def datetime( self ) -> USCentralDateTime:
        assert self._datetime is not None, f'BUG: cannot be calling the _datetime getter for {type(self).__name__} prior to defining it.'
        return self._datetime
    @property
    def is_valid( self ) -> bool:           return self._is_valid
    
    
    def _validate_input( self ):        raise NotImplementedError( 'This method must be implemented in the child class.' )

    def _read_image( self ):            raise NotImplementedError( 'This method must be implemented in the child class.' )

    def _check_data_validity( self ):   raise NotImplementedError( 'This method must be implemented in the child class.' )

    def has_eligible_dicom_extension( self, ffn: str ) -> bool:
        _, ext = os.path.splitext( ffn )
        return ext == '' or ext == '.dcm'
    
    def is_local_file( self, ffn : str ) -> bool:
        return os.path.isfile( ffn )

    def is_dicom( self, ffn: str ) -> bool:
        return self.has_eligible_dicom_extension( ffn )
    
    def is_s3_url( self, ffn: str ) -> bool:
        return ffn.startswith( 'https://' ) and '.s3.amazonaws.com/' in ffn
    
    def is_similar_to_template_image( self, thresh: float = 0.9 ) -> bool:
        min_val, _, _, _ = cv2.minMaxLoc( cv2.matchTemplate( self.image.processed_img, self.template_img, cv2.TM_CCOEFF_NORMED ) )
        assert min_val is not None, f'BUG: template matching method should not return None type for min pixel value.'
        return min_val > thresh

    def __str__( self ) -> str:
        return f'-- {self.__class__.__name__} --\nEmpty'

    @staticmethod
    def generate_source_image_file_name( n_str: str, patient_uid: str ) -> str:
        assert len( n_str ) < 4, f'This function is intended for use with creating dicom file names from their metadata instance number. It is assumed that there may be no more than 999 instances possible. You entered "{n_str}", which exceeds that threshold.'
        return '0' * ( 3 - len( n_str ) ) + n_str + '-' + patient_uid


#--------------------------------------------------------------------------------------------------------------------------
## Class for dicom files
class SourceDicomDeIdentified( ScanFile ):
    ''' # Example usage:
    print( SourceDicomDeIdentified( r'...\\data\\examples\\SourceDicomDeIdentified_Example_File' ) )
    '''
    def __init__( self, ffn: str, metatables: MetaTables ):
        super().__init__( metatables )  # Call the __init__ method of the base class
        self._validate_input( ffn )
        self._read_image()
        self._extract_acquisition_site_info()
        self._extract_group_info() #to-do: probably needs to be an ml classifier
        self._extract_date_and_time()
        self._extract_uid_info()
        self._deidentify_metadata()
        self._deidentify_image() # to-do: with a gpu we could use a more advanced approach like ocr and simply blur the text within the image.
        self._check_data_validity() 
    
    def _validate_input( self, ffn: str ):
        assert self.is_local_file( ffn ), f'Inputted file not found: {ffn}'
        assert self.is_dicom( ffn ), f'Inputted file must be a dicom file: {ffn}'
        self._ffn = ffn

    def _read_image( self ):
        self._metadata = dcmread( self.ffn )
        self._image = ImageHash( metatables=self.metatables, img=self.metadata.pixel_array )

    def _check_data_validity( self ): # dicom is valid if the image has not yet been seen and if it does not match the template image.
        self._is_valid = not self.image.in_img_hash_metatable and not self.is_similar_to_template_image()
    
    def _extract_acquisition_site_info( self ):
        acq_site_str = ''
        if 'InstitutionName' in self.metadata:
            if len( self.metadata.InstitutionName ) > 0: # If it isn't empty, store the source
                acq_site_str = self.metadata.InstitutionName
        if 'IssuerOfPatientID' in self.metadata and len( acq_site_str ) == 0:
            if len( self.metadata.IssuerOfPatientID ) > 0:
                acq_site_str = self.metadata.IssuerOfPatientID
        self._acquisition_site = acq_site_str.strip() 

    def _extract_group_info( self ): # to-do: use an ml classifier
        pass

    def _extract_date_and_time( self ):
        dt_str = ''
        if 'ContentDate' in self.metadata:
            if len( self.metadata.ContentDate ) > 0:
                dt_str = self.metadata.ContentDate + ' ' + self.metadata.ContentTime
        elif  'InstanceCreationDate' in self.metadata and len( dt_str ) == 0:
            dt_str = self.metadata.InstanceCreationDate + ' ' + self.metadata.InstanceCreationTime
        else:
            raise ValueError( 'SourceDicomDeIdentified''s extract_date_and_time() method did not produce usable information.' )
        self._datetime = USCentralDateTime( dt_str.strip() )

    def _extract_uid_info( self ):
        self._uid_info = {}
        for element in self.metadata.iterall():
            if "UID" in element.name:
                self._uid_info[element.name] = element.value.replace( '.', '_' ) # Must replace '.' with underscores because that is how theyre stored in xnat

    def _person_names_callback( self, dcm_data, data_element ):
        if data_element.VR == "PN":
            data_element.value = "REDACTED PYTHON-TO-XNAT UPLOAD SCRIPT"

    def _curves_callback( self, dcm_data, data_element ):
        if data_element.tag.group & 0xFF00 == 0x5000:
            del dcm_data[data_element.tag]

    def _deidentify_metadata( self ): # remove all sensitive metadata info
        assert self._metadata is not None, f'BUG: cannot be calling the _deidentify_metadata method for {type(self).__name__} prior to defining it.'
        self._metadata.walk( self._person_names_callback )
        self._metadata.walk( self._curves_callback )
        self._metadata.remove_private_tags()
        for i in range( 0x6000, 0x60FF, 2 ):
            tag = (i, 0x3000)
            if tag in self.metadata:
                del self._metadata[tag]
    
    def _deidentify_image( self ): # to-do: with a gpu we could use a more advanced approach like ocr and simply blur the text within the image.
        '''  # Check image for identifiable information  in the image -- if it matches a template then file is invalid'''
        pass

    def __str__( self ):
        return f'{self.__class__.__name__}:\t{self.ffn}\nIs Valid:\t{self.is_valid}\nAcquisition Site: {self.acquisition_site}\nGroup:\t\t{self.group}\nDatetime:\t{self.datetime}\nUID Info: {self.uid_info}'


#--------------------------------------------------------------------------------------------------------------------------
## Class for *a single* mturk batch file *row*
class MTurkSemanticSegmentation( ScanFile ):
    ''' # Example usage:
    print( MTurkSemanticSegmentation( pd.read_csv( r'...\\data\\examples\\MTurkSemanticSegmentation_Example_File.csv' ) ) )
    '''
    def __init__( self, assignment: pd.Series, metatables: MetaTables ): #to-do: allow for different input types eg batch file data or pulled-from-xnat data
        super().__init__( metatables )  # Call the __init__ method of the base class
        self._validate_input( assignment )
        self._read_image()
        self._extract_target_object_info() #to-do
        self._extract_date_and_time()
        self._extract_uid_info()
        self._extract_pngImageData()
        self._check_data_validity()

    @property
    def bw( self ) -> np.ndarray:   return self._bw
    
    def _validate_input( self, assignment: pd.Series ):
        assert len( set(self.mturk_batch_col_names) - set(assignment.columns) ) == 0, f"Missing required columns: {set(self.mturk_batch_col_names) - set(assignment.columns)}"
        self._metadata = assignment.loc[0]
        img_s3_url = assignment.loc[0,'Input.image_url']
        assert self.is_s3_url( img_s3_url ), f'Input.image_url column of inputted data series (row) must be an s3 url: {img_s3_url}'
        self._ffn = self.metadata['Input.image_url']
        self._bw, self._acquisition_site = self.image.dummy_image(), 'AMAZON_MECHANICAL_TURK' #to-do: this is copy-pasted from the MetaTables, need to figure out how to query it.

    def _read_image( self ):
        response = requests.get( self.ffn, stream=True )
        response.raw.decode_content = True
        arr = np.asarray( bytearray( response.raw.read() ), dtype=np.uint8 )
        self._image = ImageHash( metatables=self.metatables, img=cv2.imdecode( arr, cv2.IMREAD_GRAYSCALE ) ) # img = Image.open( response.raw )

    def _check_data_validity( self ): # need to check that the pnd image *does* exist in metatables
        self._is_valid = self.image.in_img_hash_metatable and not self.is_similar_to_template_image()
    
    def _extract_target_object_info( self ):
        pass

    def _extract_date_and_time( self ):
        self._datetime = USCentralDateTime( self.metadata.loc['SubmitTime'] )

    def _extract_uid_info( self ):
        self._uid_info = { 'HIT_ID': self.metadata['HITId'], 'ASSIGNMENT_ID': self.metadata['AssignmentId'], 'WORKER_ID': self.metadata['WorkerId'] }

    def _extract_pngImageData( self ):
        pngImageData_index = [i for i, c in enumerate( self.metadata.index.to_list() ) if '.pngImageData' in c]
        self._bw = self.convert_base64_to_np_array( self.metadata.iloc[pngImageData_index[0]] )

    def convert_base64_to_np_array( self, b64_str: str ) -> np.ndarray:
        return cv2.imdecode( np.frombuffer( base64.b64decode( b64_str ), np.uint8 ), cv2.IMREAD_GRAYSCALE )
     
    def __str__( self ):
        return f'{self.__class__.__name__}:\t{self.ffn}\nIs Valid:\t{self.is_valid}\nAcquisition Site: {self.acquisition_site}\nGroup:\t\t{self.group}\nDatetime:\t{self.datetime}\nUID Info: {self.uid_info}'


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all xnat experiment sessions.
class ExperimentData():
    def __init__( self, ffn: str, acquisition_site: str, login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables, group: str, print_out: Opt[bool] = False ):
        self._ffn, self._acquisition_site, self._group = ffn, acquisition_site, group # Required user inputs
        self._login, self._xnat_connection, self._metatables = login, xnat_connection, metatables # Required user inputs
        self._series_description, self._usability, self._past_upload_data = None, None, None # Optional user inputs
        self._df, self._uid, self._datetime, self._is_valid, self._label = None, None, None, False, '' # Derived from inputs
        if print_out:   print( f'Now processing "{self.__class__.__name__}" for "{self.ffn}" ...' )
    
    @property # --- Required Properties ---
    def ffn( self ) -> str:                         return self._ffn
    @property
    def acquisition_site( self ) -> str:            return self._acquisition_site # aka source aka institution
    @property
    def group( self ) -> str:                       return self._group # AKA "surgical procedure"
    @property
    def login( self ) -> XNATLogin:                 return self._login
    @property
    def xnat_connection( self ) -> XNATConnection:  return self._xnat_connection
    @property
    def metatables( self ) -> MetaTables:           return self._metatables
    @property # --- Optional Properties --- to-do: create a class for each so that they can be embedded with their own validation
    def series_description( self ) -> str:
        assert self._series_description is not None, f'BUG: cannot be calling the _series_description getter for {type(self).__name__} prior to defining it.'
        return self._series_description # to-do: open-ended, but should probably be from a set metatable
    @property
    def usability( self ) -> str:
        assert self._usability is not None, f'BUG: cannot be calling the _usability getter for {type(self).__name__} prior to defining it.'
        return self._usability # to-do: xnat only allows specific info, should map those options to metatables
    @property
    def past_upload_data( self ) -> pd.DataFrame:
        assert self._past_upload_data is not None, f'BUG: cannot be calling the _past_upload_data getter for {type(self).__name__} prior to defining it.'
        return self._past_upload_data
    @property # --- Derived Properties ---
    def df( self ) -> pd.DataFrame:
        assert self._df is not None, f'BUG: cannot be calling the _df getter for {type(self).__name__} prior to defining it.'
        return self._df
    @property
    def uid( self ) -> str:
        assert self._uid is not None, f'BUG: cannot be calling the _uid getter for {type(self).__name__} prior to defining it.'
        return self._uid
    @property
    def datetime( self ) -> USCentralDateTime:
        assert self._datetime is not None, f'BUG: cannot be calling the _datetime getter for {type(self).__name__} prior to defining it.'
        return self._datetime
    @property
    def is_valid( self ) -> bool:   return self._is_valid
    @property
    def label( self ) -> str:       return self._label
        
        
    def _generate_session_uid( self ):
        # dt_str = datetime.strptime( self.date + self.time, '%Y%m%d%H%M%S' ).strftime( '%Y-%m-%d %H:%M:%S' )
        self._uid = dcmUID.generate_uid( prefix=None, entropy_srcs=[self.datetime.date + ' ' + self.datetime.time] )

    def _derive_session_label( self ):
        assert self.uid is not None, f'Current uid "{self.uid}" is empty; Label is derived from uid, which cannot be empty.'
        self._label = self.uid.replace( '.', '_' )

    def _populate_df( self ):                       raise NotImplementedError( 'This method must be implemented in the child class.' )
    def _check_session_validity( self ):            raise NotImplementedError( 'This method must be implemented in the child class.' )

    # to-do: the following validate methods should probably become classes
    def _validate_series_description_input( self ): raise NotImplementedError( 'This method must be implemented in the child class.' )
    def _validate_usability_input( self ):          raise NotImplementedError( 'This method must be implemented in the child class.' )
    def _validate_past_upload_data_input( self ):   raise NotImplementedError( 'This method must be implemented in the child class.' )

    def _validate_acquisition_site_input( self ): # to-do: potentially turn this into a class
        assert self.metatables.item_exists( table_name='ACQUISITION_SITES', item_name=self.acquisition_site ), f"The inputted acquisition site '{self.acquisition_site}' is not recognized.\nIt must be one of these:\n{self.metatables.tables['ACQUISITION_SITES']['NAME'].values}"
 
    def _validate_group_input( self ): # to-do shouldn't always be allowed to input none but we will for now; possibly use an ML classifier way down the line.
        assert self.metatables.item_exists( table_name='GROUPS', item_name=self.group ), f"The inputted group (aka 'surgical procedure name') '{self.group}' is not recognized.\nIt must be one of these:\n{self.metatables.tables['GROUPS']['NAME'].values}"



#--------------------------------------------------------------------------------------------------------------------------
## Class for all radio fluoroscopic (source image) sessions.
class SourceRFSession( ExperimentData ): # to-do: Need to detail past and present identifiers for a subject.
    ''' '''
    def __init__( self,
                dcm_dir: str, login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables, acquisition_site: str, group: str,
                series_desc: Opt[str] = None, usability: Opt[str] = None, past_upload_data: Opt[pd.DataFrame] = None,
                print_out: Opt[bool] = False ):
        super().__init__( ffn=dcm_dir, login=login, xnat_connection=xnat_connection, metatables=metatables, acquisition_site=acquisition_site, group=group, print_out=print_out ) # Call the __init__ method of the base class
        self._series_description, self._usability, self._past_upload_data = series_desc, usability, past_upload_data
        self._validate_series_description_input()
        self._validate_usability_input()
        self._validate_past_upload_data_input()
        self._populate_df()
        self._check_session_validity()
        if self.is_valid:
            self._mine_session_metadata() # necessary for publishing to xnat.
        # self._match_past_file_names( past_file_names ) # to-do: ? the Case_Database.csv file in the R-drive FLuoroscopy folder contains info about previous name information of these files. for now i am just going to tryst in my image hash protocol for preventing duplicates.

    @property
    def _all_dicom_ffns( self ) -> list:
        assert os.path.isdir( self.ffn ), f'First input must refer to a directory of dicom files detailing a surgical performance; you entered: "{self.ffn}".'
        return glob.glob( os.path.join( self.ffn, '**', '*' ), recursive=True )

    def _init_rf_session_dataframe( self ):
        df_cols = { 'FN': 'str', 'EXT': 'str', 'NEW_FN': 'str', 'DICOM': 'object', 'IS_VALID': 'bool',
                    'DATE': 'str', 'SERIES_TIME': 'str', 'INSTANCE_TIME': 'str', 'INSTANCE_NUM': 'str' }
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )

    def _populate_df( self ):
        self._init_rf_session_dataframe()
        all_ffns = self._all_dicom_ffns
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext != '.dcm':
                self._df.loc[idx, ['FN', 'EXT', 'IS_VALID']] = [fn, ext, False]
                continue
            deid_dcm = SourceDicomDeIdentified( ffn=ffn, metatables=self.metatables )
            self._df.loc[idx, ['FN', 'EXT', 'DICOM', 'IS_VALID']] = [fn, ext, deid_dcm, deid_dcm.is_valid]
            if deid_dcm.is_valid:
                dt_data = self._query_dicom_series_time_info( deid_dcm )
                self._df.loc[idx, ['DATE', 'INSTANCE_TIME', 'SERIES_TIME', 'INSTANCE_NUM']] = dt_data
                self._df.loc[idx, 'NEW_FN'] = deid_dcm.generate_source_image_file_name( str( deid_dcm.metadata.InstanceNumber ), str( self.label ) )

        # Need to check within-case for duplicates -- apparently those do exist.
        hash_strs = set()
        for idx, row in self.df.iterrows():
            if row['IS_VALID']:
                if row['DICOM'].image.hash_str in hash_strs:
                    self._df.at[idx, 'IS_VALID'] = False
                else:
                    hash_strs.add( row['DICOM'].image.hash_str )
        print( self.df)

    def _query_dicom_series_time_info( self, deid_dcm: SourceDicomDeIdentified ) -> list:
        dt_data = [deid_dcm.datetime.date, deid_dcm.datetime.time, None, deid_dcm.metadata.InstanceNumber]
        if 'SeriesTime' in deid_dcm.metadata:    dt_data[2] = deid_dcm.metadata.SeriesTime
        if 'ContentTime' in deid_dcm.metadata:   dt_data[2] = deid_dcm.metadata.ContentTime
        if 'StudyTime' in deid_dcm.metadata:     dt_data[2] = deid_dcm.metadata.StudyTime
        return dt_data


    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        self._derive_experiment_datetime()
        self._derive_experiment_uid()
        self._derive_session_label()
        # self._derive_acquisition_site_info() # to-do: should warn the user that any mined info is inconsistent with their input
        self._df = self.df.sort_values( by='NEW_FN', inplace=False )

    # def _derive_acquisition_site_info( self ): # Retrieve source institution info across all dicom files and tell user if their input is inconsistent, if given
    #     potential_sources = set( source for sublist in self.df[self.df['IS_VALID']]['DICOM'].apply( lambda x: x.acquisition_site ) for source in sublist )
    #     if len( potential_sources ) > 0:
    #         pass # self._validate_derived_acquisition_sites( potential_sources ) # need to evaluate the derived info against the user input
    #     self._acquisition_site = self.acquisition_site.upper()

    def _validate_series_description_input( self ):
        if self._series_description is None:        self._series_description = ''
    def _validate_usability_input( self, ):
        if self._usability is None:                 self._usability = ''
    def _validate_past_upload_data_input( self, past_upload_data: Opt[pd.DataFrame] = None ):
        if self._past_upload_data is None:          self._past_upload_data = pd.DataFrame() # to-do: this should be a dataframe with a specific structure
        
    def _check_session_validity( self ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
        # valid_rows = self.df[ self.df['IS_VALID'] ].copy()
        # assert self.label, 'Cannot check duplicate without a rfsession label.'
        self._is_valid = self.df['IS_VALID'].any() and not self.metatables.item_exists( table_name='SUBJECTS', item_name=self.label )
        

    def _derive_experiment_datetime( self ):
        assert self.df['DATE'].nunique() == 1, f'Dicom metadata produced either all-nans or different dates across the files for this performance -- should only be one:\n{self._df["DATE"]}'
        assert self.df['SERIES_TIME'].nunique() == 1, f'Dicom metadata produced different SERIES_TIME values across the files for this performance -- should only be one:\n{self._df["SERIES_TIME"]}'
        self._datetime = USCentralDateTime( self.df.at[0,'DATE'] + ' ' + self.df.at[0, 'SERIES_TIME'] )
    
    def _derive_experiment_uid( self ):
        '''Original dicom data should have the same Series Instance UID for all dicom files. The Instance number is the file name.'''
        series_instance_uids = []
        for _, row in self.df.iterrows():
            if row['IS_VALID']:
                series_instance_uids.append( row['DICOM'].uid_info['Series Instance UID'] )
        series_instance_uids = pd.Series( series_instance_uids )
        if series_instance_uids.nunique() == 1:
            self._uid = series_instance_uids.at[0]
        else:
            self._generate_session_uid()
            self._deal_with_inconsistent_series_instance_uid()
    
    def _deal_with_inconsistent_series_instance_uid( self ): # overwrite inconsisten series instance uid information in the metadata.
        for idx, row in self.df.iterrows():
            if row['IS_VALID']: # Copy the value for 'SeriesInstanceUID' to a new private tag; add new private tags detailing this change
                description = "Original (but inconsistent) SeriesInstanceUID on upload to XNAT"
                self._df.at[idx,'DICOM'].metadata.add_new( (0x0019, 0x1001), 'LO', description )
                self._df.at[idx,'DICOM'].metadata.add_new( (0x0019, 0x1002), 'LO', row['DICOM'].metadata.SeriesInstanceUID )
                self._df.at[idx,'DICOM'].metadata.add_new( (0x0019, 0x1003), 'LO', ['Added by: ' + self.login.validated_username] )
                self._df.at[idx,'DICOM'].metadata.add_new( (0x0019, 0x1004), 'DA', datetime.today().strftime( '%Y%m%d' ) )
                self._df.at[idx,'DICOM'].metadata.SeriesInstanceUID = self.uid

    def __str__( self ) -> str:
        select_cols = ['FN','NEW_FN', 'IS_VALID', 'INSTANCE_TIME']
        df = self.df[select_cols].copy()
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{self.uid}\nAcquisition Site:\t{self.acquisition_site}\nGroup:\t\t\t{self.group}\nDate-Time:\t\t{self.datetime}\nValid:\t{self.is_valid}\n{df.head()}...{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{self.acquisition_site}\nGroup:\t\t\t{self.group}\nDate-Time:\t\t{None}\nValid:\t{self.is_valid}\n{df.head()}...{df.tail()}'



    #--------------------------------------------------------------------
    def _generate_queries( self ) -> Tuple[str, str, str, str, str, str]:
        # Create query strings and select object in xnat then create the relevant objects
        exp_label = ( 'Source_Images' + '-' + self.label )
        scan_label = 'Original'
        scan_type_label = 'DICOM'
        resource_label = 'Raw'
        proj_qs = '/project/' + self.xnat_connection.xnat_project_name
        proj_qs = PurePosixPath( proj_qs ) # to-doneed to revisit this because it is hard-coded but Path makes it annoying
        subj_qs =  proj_qs / 'subject' / str( self.label )
        exp_qs = subj_qs / 'experiment' / exp_label
        scan_qs = exp_qs / 'scan' / scan_label
        files_qs = scan_qs / 'resource' / 'files'
        return str(subj_qs), str(exp_qs), str(scan_qs), str(files_qs), scan_type_label, resource_label


    def _select_objects( self, subj_qs: str, exp_qs: str, scan_qs: str, files_qs: str ):
        subj_inst = self.xnat_connection.server.select( str( subj_qs ) )
        assert not subj_inst.exists(), f'Subject already exists with the uri:\n{subj_inst}'         # type: ignore
        exp_inst = self.xnat_connection.server.select( str( exp_qs ) )
        assert not exp_inst.exists(), f'Experiment already exists with the uri:\n{exp_inst}'        # type: ignore
        scan_inst = self.xnat_connection.server.select( str( scan_qs ) )
        assert not scan_inst.exists(), f'Scan already exists with the uri:\n{scan_inst}'            # type: ignore
        return subj_inst, exp_inst, scan_inst


    def publish_to_xnat( self, zipped_ffn: str, print_out: Opt[bool] = False, delete_zip: Opt[bool] = True ):
        try:
            if print_out:
                print( f'\t...Pushing rfSession to XNAT...' )
            subj_qs, exp_qs, scan_qs, files_qs, scan_type_label, resource_label = self._generate_queries()
            subj_inst, exp_inst, scan_inst = self._select_objects( subj_qs, exp_qs, scan_qs, files_qs )

            # Create the items in stepwise fashion -- to-do: can't figure out how to create all in one go instead of attrs.mset(), it wouldn't work properly
            subj_inst.create()                                                                      # type: ignore
            subj_inst.attrs.mset( { 'xnat:subjectData/GROUP': self.group } )                        # type: ignore
            exp_inst.create( **{    'experiments':'xnat:rfSessionData' })                           # type: ignore
            exp_inst.attrs.mset( {  'xnat:experimentData/ACQUISITION_SITE': self.acquisition_site,  # type: ignore
                                    'xnat:experimentData/DATE': self.datetime.date } )
            scan_inst.create( **{   'scans':'xnat:rfScanData' } )                                   # type: ignore
            scan_inst.attrs.mset( { 'xnat:imageScanData/TYPE': scan_type_label } )                  # type: ignore
            scan_inst.resource( resource_label ).put_zip( zipped_ffn )                              # type: ignore
            if delete_zip is True:
                os.remove( zipped_ffn )
            if print_out is True:
                print( f'\t\t...rfSession succesfully uploaded to XNAT!' )
                print( f'\t...Zipped file deleted!\n')
        except Exception as e:
            print( f'\tError: could not publish to xnat.\n{e}' )


    def write( self, zip_dest: Opt[str] = None, print_out: Opt[bool] = False ) -> str: #write individiual dicom files to a zipped folder
        if not self.is_valid:
            if print_out:
                print( f'***Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable.' )

        # Method also adds the subject and image info to the metatables at the same time so we can ensure no duplicates reach 'publish_to_xnat()'
        if zip_dest is None:
            zip_dest = self.login.tmp_data_dir
        assert os.path.isdir( zip_dest ), 'Destination for zipped folder must be an existing directory; you entered: {zip_dest}'
        write_d = os.path.join( zip_dest, self.uid )
        subject_info = { 'ACQUISITION_SITE': self.metatables.get_uid( table_name='ACQUISITION_SITES', item_name=self.acquisition_site ),
                        'GROUP': self.metatables.get_uid( table_name='GROUPS', item_name=self.group ) }
        self.metatables.add_new_item( table_name='SUBJECTS', item_name=self.uid, extra_columns_values=subject_info, print_out=print_out ) # type: ignore
        with tempfile.TemporaryDirectory() as tmp_dir:
            for _, row in self.df.iterrows():
                if row['IS_VALID']:
                    dcmwrite( os.path.join( tmp_dir, row['NEW_FN'] ), row['DICOM'].metadata )
                    img_info = { 'SUBJECT': self.metatables.get_uid( table_name='SUBJECTS', item_name=self.uid ), 'INSTANCE_NUM': row['NEW_FN'] }
                    self.metatables.add_new_item( table_name='IMAGE_HASHES', item_name=row['DICOM'].image.hash_str, extra_columns_values=img_info, print_out=print_out ) # type: ignore
            shutil.make_archive( write_d, 'zip', tmp_dir )
        
        if print_out is True:
            num_valid = self.df['IS_VALID'].sum()
            print( f'\t...Zipped folder of ({num_valid}/{len( self.df )}) dicom files successfully written to: {write_d}.zip' )
        return write_d + '.zip'


    def catalog_new_data( self, print_out: Opt[bool] = False ):
        self.metatables.save( print_out=print_out )
        if print_out is True:
            print( f'\t...Metatables successfully updated to reflect new subject uid and image hashes.' )