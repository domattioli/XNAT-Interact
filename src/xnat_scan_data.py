import os
from typing import Optional as Opt, Tuple, Union
import cv2
import base64
import requests

import numpy as np
import pandas as pd

from datetime import datetime

from pydicom.dataset import FileDataset as pydicomFileDataset, FileMetaDataset as pydicomFileMetaDataset
from pydicom.uid import UID as dcmUID, ExplicitVRLittleEndian, ImplicitVRLittleEndian
from pydicom import Dataset, Sequence, dcmread, dcmwrite
# from pydicom.uid import is_valid_uid as dcm_is_valid_uid # no function exists -- chatgpt is wrong

from pathlib import Path


from src.utilities import UIDandMetaInfo, MetaTables, USCentralDateTime, ImageHash
from src.xnat_resource_data import ORDataIntakeForm


# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['ScanFile', 'SourceDicomDeIdentified', 'MTurkSemanticSegmentation', 'ArthroDiagnosticImage', 'ArthroVideo']


#--------------------------------------------------------------------------------------------------------------------------
## Base class for scan files, designed for inheritence.
class ScanFile( UIDandMetaInfo ):
    def __init__( self, intake_form: ORDataIntakeForm, ffn: Path ):
        super().__init__() # Call the __init__ method of the base class to create a uid for this instance
        assert os.path.isfile( ffn ), f'Inputted file not found: {ffn}'
        self._intake_form, self._ffn = intake_form, ffn
        self._new_ffn, self._metadata, self._image, self._derived_metadata, self._datetime, self._is_valid = '', None, None, {}, None, False


    @property
    def intake_form( self )         -> ORDataIntakeForm:    return self._intake_form
    @property
    def ffn( self )                 -> Path:                return self._ffn
    @property
    def ffn_str( self )             -> str:                 return str( self.ffn )
    @property
    def new_ffn( self )             -> str:                 return self._new_ffn
    @property
    def metadata( self )            -> Union[pydicomFileDataset, pd.Series]:
        assert self._metadata is not None, f'BUG: cannot be calling the _metadata getter for {type(self).__name__} prior to defining it.'
        return self._metadata
    @property
    def image( self )               -> Union[ImageHash, cv2.VideoCapture]:
        assert self._image is not None, f'BUG: cannot be calling the _image getter for {type(self).__name__} prior to defining it.'
        return self._image
    @property
    def derived_metadata( self )    -> dict:                return self._derived_metadata #to-do: create a class for this? the inherited ones have not structure
    @property
    def datetime( self )            -> USCentralDateTime:
        assert self._datetime is not None, f'BUG: cannot be calling the _datetime getter for {type(self).__name__} prior to defining it.'
        return self._datetime
    @property
    def is_valid( self )            -> bool:                return self._is_valid
    
    def set_new_ffn( self, new_ffn: str ) -> None:           self._new_ffn = new_ffn
    def is_dicom(self, ffn: Path)   -> bool:                return os.path.splitext( ffn )[1] in ( '', '.dcm' )
    def is_jpg( self, ffn: Path )   -> bool:                return ffn.suffix in ['.jpg', '.jpeg']
    def is_s3_url( self, ffn: Path )-> bool:                return str( ffn ).startswith( 'https://' ) and '.s3.amazonaws.com/' in str(ffn)
    def is_mp4( self, ffn: Path )   -> bool:                return ffn.suffix == '.mp4'

    def is_similar_to_template_image( self, thresh: float = 0.9 ) -> bool:
        assert not isinstance( self.image, cv2.VideoCapture ), f'BUG: cannot be calling the is_similar_to_template_image method for {type(self).__name__} with a video file.'
        min_val, _, _, _ = cv2.minMaxLoc( cv2.matchTemplate( self.image.processed_img, self.template_img, cv2.TM_CCOEFF_NORMED ) )
        assert min_val is not None, f'BUG: template matching method should not return None type for min pixel value.'
        return min_val > thresh

    def _validate_image( self ):                            raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _validate_input( self ):                            raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _read_image( self ):                                raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _parse_for_derived_metadata( self ):                raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )

    # to-do: with a gpu we could use a more advanced approach like ocr and simply blur the text within the image.
    def _deidentify_image( self ):                          raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )


    def __str__( self ):
        return f'{self.__class__.__name__}:\t{self.ffn}\nIs Valid:\t{self.is_valid}\nAcquisition Site: {self.intake_form.acquisition_site}\nGroup:\t\t{self.intake_form.group}\nDatetime:\t{self.datetime}'
    

    def __del__( self ):
        if isinstance( self.image, cv2.VideoCapture ):  self.image.release()


    @staticmethod
    def generate_source_image_file_name( inst_str: str, patient_uid: str ) -> str:
        assert len( inst_str ) < 4, f'This function is intended for use with creating dicom file names from their metadata instance number. It is assumed that there may be no more than 999 instances possible. You entered "{inst_str}", which exceeds that threshold.'
        if inst_str.isdigit() and int( inst_str ) < 1000:
            # Append the appropriate number of leading zeros
            inst_str = inst_str.zfill(4)
        return f"{inst_str}-{patient_uid}"


#--------------------------------------------------------------------------------------------------------------------------
## Class(es) for arthro files files
class ArthroDiagnosticImage( ScanFile ):
    '''Class representing the XNAT Scan for Arthroscopic Diagnostic Images. Inherits from ScanFile.'''
    def __init__( self, img_ffn: Path, still_num: str, parent_uid: str, metatables: MetaTables, intake_form: ORDataIntakeForm ):
        super().__init__( intake_form=intake_form, ffn=img_ffn )  # Call the __init__ method of the base class
        self._datetime, self._still_num = datetime, still_num
        self._validate_input( parent_uid=parent_uid )
        self._read_image( metatables=metatables )
        self._validate_image() 
        self._create_dicom_representation( parent_uid=parent_uid )


    def _validate_input( self, parent_uid: str ):
        assert self.is_jpg( self.ffn ), f'Inputted file must be a jpg file: {self.ffn}'
        assert self.still_num.isdigit(), f'Inputted still number must be a string of digits: {self.still_num}'
        # assert dcm_is_valid_uid( parent_uid ), f'Inputted parent uid must be a valid dicom uid: {parent_uid}' #to-do: need a method for validating a uid


    def _read_image( self, metatables: MetaTables ):
        self._image = ImageHash( reference_table=metatables, img=cv2.imread( self.ffn_str ) )
        self._derived_metadata = {'ARTHRO_STILL_IMAGE_UID': self.generate_uid()} 


    def _validate_image( self ): # valid if the image has not yet been seen and if it does not match the template image.
        assert not isinstance( self.image, cv2.VideoCapture ), f'BUG: cannot be calling the is_similar_to_template_image method for {type(self).__name__} with a video file.'
        self._is_valid = not self.image.in_img_hash_metatable and not self.is_similar_to_template_image()


    def _create_dicom_representation( self, parent_uid: str ):
        file_meta = pydicomFileMetaDataset()
        file_meta.MediaStorageSOPClassUID = dcmUID( '1.2.840.10008.5.1.4.1.1.77.1.1.1' ) # Video Endoscopic Image IOD
        file_meta.MediaStorageSOPInstanceUID = dcmUID( self._derived_metadata['ARTHRO_STILL_IMAGE_UID'].replace( '_', '.' ) )
        file_meta.ImplementationClassUID = dcmUID( parent_uid.replace( '_', '.' ) )
        file_meta.TransferSyntaxUID = ImplicitVRLittleEndian # Implicit VR Little Endian
        
        date_now_str, time_now_str = str( datetime.now().strftime( '%Y%m%d' ) ), str( datetime.now().strftime( '%H%M%S' ) )
        date_img_str, time_img_str = str( self.datetime.date ), str( self.datetime.time )
        ds = pydicomFileDataset( self.new_ffn, {}, file_meta=file_meta, preamble=b"\0" * 128)
        ds.PatientName, ds.PatientID = self.redacted_string, self.redacted_string
        ds.ContentDate, ds.ContentTime = date_now_str, time_now_str
        ds.StudyDate, ds.StudyTime = date_img_str, time_img_str
        ds.StudyInstanceUID = dcmUID( parent_uid.replace( '_', '.' ) )
        ds.InstanceNumber = self.still_num
        ds.InstanceCreationDate, ds.InstanceCreationTime = date_img_str, '' #to-do: instance creation time is potentially gleanable from the original file info?
        ds.SeriesInstanceUID = dcmUID( self._derived_metadata['ARTHRO_STILL_IMAGE_UID'].replace( '_', '.' ) )
        ds.SeriesDescription = f'Arthro. Diagn. Img. #{self.still_num}'

        ds.InstitutionName = self.intake_form.acquisition_site

        ds.ImageType, ds.Modality = ['ORIGINAL', 'PRIMARY', 'ARTHRO_DIAGN_IMG'], 'ES' # Endoscopy (video)
        ds.is_little_endian, ds.is_implicit_VR = True, True 
        ds.PixelData = self.image.gray_img.tobytes() # type: ignore because the _validate_image() method asserts that self.image is not a cv2.VideoCapture
        ds.Rows, ds.Columns = self.image.gray_img.shape # type: ignore
        ds.BitsAllocated, ds.SamplesPerPixel, ds.BitsStored = 8, 1, 8
        ds.PhotometricInterpretation, ds.PixelRepresentation = 'MONOCHROME1', 0
        self._metadata = ds


    @property
    def still_num( self ) -> str:   return self._still_num



#--------------------------------------------------------------------------------------------------------------------------
class ArthroVideo( ScanFile ):
    '''Class representing the XNAT Scan for Arthroscopic Videos. Inherits from ScanFile.'''
    def __init__( self, vid_ffn: Path, metatables: MetaTables, intake_form: ORDataIntakeForm ):
        super().__init__( intake_form=intake_form, ffn=vid_ffn )  # Call the __init__ method of the base class
        self._validate_input()
        self._read_video()
        # self._uid_info = {'Video_UID': self.generate_uid()} # shouldnt need this if inheriting from ScanFile which inherits from Librarian Utilities
        self._validate_image()
        

    def _validate_input( self ):    assert self.is_mp4( self.ffn ), f'Inputted file must be a mp4 file: {self.ffn_str}'


    def _read_video( self ):        self._image = cv2.VideoCapture( self.ffn_str )


    def _validate_image( self ):
        assert not isinstance( self.image, ImageHash ), f'Inputted file must be a video file: {self.ffn_str}'
        self._is_valid = self.image.isOpened()


#--------------------------------------------------------------------------------------------------------------------------
## Class for dicom (trauma) files
class SourceDicomDeIdentified( ScanFile ):
    ''' # Example usage:
    print( SourceDicomDeIdentified( r'...\\data\\examples\\SourceDicomDeIdentified_Example_File' ) )
    '''
    def __init__( self, dcm_ffn: Path, metatables: MetaTables, intake_form: ORDataIntakeForm ):
        super().__init__( intake_form=intake_form, ffn=dcm_ffn )  # Call the __init__ method of the base class
        assert self.is_dicom( self.ffn ), f'Inputted file must be a dicom file: {self.ffn}'
        self._read_image( metatables=metatables )
        self._parse_for_derived_metadata()
        self._deidentify_dicom()
        self._validate_image() # deleted local method and made it inherited. seems like it should always be the same validation criteria
    

    def _read_image( self, metatables: MetaTables ):
        self._metadata = dcmread( self.ffn )
        self._image = ImageHash( reference_table=metatables, img=self.metadata.pixel_array )


    def _parse_for_derived_metadata( self ):
        # Look for acquisition site info in the metadata:
        acq_site_str = ''
        if 'InstitutionName' in self.metadata:
            if len( self.metadata.InstitutionName ) > 0: # If it isn't empty, store the source
                acq_site_str = self.metadata.InstitutionName
        if 'IssuerOfPatientID' in self.metadata and len( acq_site_str ) == 0:
            if len( self.metadata.IssuerOfPatientID ) > 0:
                acq_site_str = self.metadata.IssuerOfPatientID
        self._derived_metadata['ACQUISITION_SITE'] = acq_site_str.strip() 

        # Group info: # to-do: use an ml classifier?
        self._derived_metadata['GROUP'] = ''
        
        # Date and time info:
        dt_str = ''
        if 'ContentDate' in self.metadata:
            if len( self.metadata.ContentDate ) > 0:
                dt_str = self.metadata.ContentDate + ' ' + self.metadata.ContentTime
        elif  'InstanceCreationDate' in self.metadata and len( dt_str ) == 0:
            dt_str = self.metadata.InstanceCreationDate + ' ' + self.metadata.InstanceCreationTime
        else:
            raise ValueError( 'SourceDicomDeIdentified''s extract_date_and_time() method did not produce usable information.' )
        self._derived_metadata['DATETIME'] = USCentralDateTime( dt_str.strip() )

        # UID info:
        for element in self.metadata.iterall():
            if "UID" in element.name:
                self._derived_metadata['UID_INFO'][element.name] = element.value.replace( '.', '_' ) # Must replace '.' with underscores because that is how theyre stored in xnat
        # self._derived_metadata = { 'DICOM_UID': self.generate_uid() }


    def _person_names_callback( self, dcm_data, data_element ):
        if data_element.VR == "PN":
            data_element.value = self.redacted_string


    def _curves_callback( self, dcm_data, data_element ):
        if data_element.tag.group & 0xFF00 == 0x5000:
            del dcm_data[data_element.tag]


    def _deidentify_dicom( self ): # remove all sensitive metadata info
        assert self._metadata is not None, f'BUG: cannot be calling the _deidentify_metadata method for {type(self).__name__} prior to defining it.'
        self._metadata.walk( self._person_names_callback )
        self._metadata.walk( self._curves_callback )
        self._metadata.remove_private_tags()
        for i in range( 0x6000, 0x60FF, 2 ):
            tag = (i, 0x3000)
            if tag in self.metadata:
                del self._metadata[tag]
    
        # De-identify embedded pixel data
        # to-do: with a gpu we could use a more advanced approach like ocr and simply blur the text within the image.
    

#--------------------------------------------------------------------------------------------------------------------------
## Class for *a single* mturk batch file *row*
class MTurkSemanticSegmentation( ScanFile ):
    ''' # Example usage:
    print( MTurkSemanticSegmentation( pd.read_csv( r'...\\data\\examples\\MTurkSemanticSegmentation_Example_File.csv' ) ) )
    '''
    # def __init__( self, assignment: pd.Series, metatables: MetaTables, intake_form: ORDataIntakeForm ): #to-do: allow for different input types eg batch file data or pulled-from-xnat data
    #     super().__init__( intake_form=intake_form, assignment )  # to:do -- cant pass assignment to super().__init__ because it expects a Path object. need to rethink the baseclass.
    #     self._validate_input( assignment )
    #     self._read_image()
    #     self._extract_target_object_info() #to-do
    #     self._extract_date_and_time()
    #     self._extract_uid_info()
    #     self._extract_pngImageData()
    #     self._validate_image()

    # @property
    # def bw( self ) -> np.ndarray:   return self._bw
    
    # def _validate_input( self, assignment: pd.Series ):
    #     assert len( set(self.mturk_batch_col_names) - set(assignment.columns) ) == 0, f"Missing required columns: {set(self.mturk_batch_col_names) - set(assignment.columns)}"
    #     assert not isinstance( self.image, ImageHash ), f'BUG: the dummy_image() method below will only work if .image is an ImageHash object.'
    #     self._metadata = assignment.loc[0]
    #     img_s3_url = assignment.loc[0,'Input.image_url']
    #     assert self.is_s3_url( img_s3_url ), f'Input.image_url column of inputted data series (row) must be an s3 url: {img_s3_url}'
    #     self._ffn = self.metadata['Input.image_url']
    #     self._bw, self._acquisition_site = self.image.dummy_image(), 'AMAZON_MECHANICAL_TURK' #to-do: this is copy-pasted from the MetaTables, need to figure out how to query it.

    # def _read_image( self ):
    #     response = requests.get( self.ffn_str, stream=True )
    #     response.raw.decode_content = True
    #     arr = np.asarray( bytearray( response.raw.read() ), dtype=np.uint8 )
    #     self._image = ImageHash( reference_table=self.metatables, img=cv2.imdecode( arr, cv2.IMREAD_GRAYSCALE ) ) # img = Image.open( response.raw )
    
    # def _extract_target_object_info( self ):
    #     pass

    # def _extract_date_and_time( self ):
    #     self._datetime = USCentralDateTime( self.metadata.loc['SubmitTime'] )

    # def _extract_uid_info( self ):
    #     self._uid_info = { 'HIT_ID': self.metadata['HITId'], 'ASSIGNMENT_ID': self.metadata['AssignmentId'], 'WORKER_ID': self.metadata['WorkerId'] }

    # def _extract_pngImageData( self ):
    #     pngImageData_index = [i for i, c in enumerate( self.metadata.index.to_list() ) if '.pngImageData' in c]
    #     self._bw = self.convert_base64_to_np_array( self.metadata.iloc[pngImageData_index[0]] )

    # def convert_base64_to_np_array( self, b64_str: str ) -> np.ndarray:
    #     return cv2.imdecode( np.frombuffer( base64.b64decode( b64_str ), np.uint8 ), cv2.IMREAD_GRAYSCALE )
     
    # def __str__( self ):
    #     return f'{self.__class__.__name__}:\t{self.ffn}\nIs Valid:\t{self.is_valid}\nAcquisition Site: {self.acquisition_site}\nGroup:\t\t{self.group}\nDatetime:\t{self.datetime}\nUID Info: {self.uid_info}'


