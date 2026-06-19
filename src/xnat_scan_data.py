import os
from typing import Optional as Opt, Tuple, Union
import cv2
import pandas as pd
from datetime import datetime
from pydicom.dataset import FileDataset as pydicomFileDataset, FileMetaDataset as pydicomFileMetaDataset
from pydicom.uid import UID as dcmUID, ExplicitVRLittleEndian, ImplicitVRLittleEndian
from pydicom import Dataset, Sequence, dcmread, dcmwrite
# from pydicom.uid import is_valid_uid as dcm_is_valid_uid # no function exists -- chatgpt is wrong
from pathlib import Path

from src.utilities import UIDandMetaInfo, ConfigTables, USCentralDateTime, ImageHash
from src.xnat_resource_data import ORDataIntakeForm
from src.services.deidentify import deidentify_dataset


# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['ScanFile', 'SourceDicomDeIdentified', 'MTurkSemanticSegmentation', 'ArthroDiagnosticImage', 'ArthroVideo']


#--------------------------------------------------------------------------------------------------------------------------
## Base class for scan files, designed for inheritence.
class ScanFile( UIDandMetaInfo ):
    """
    Base class for scan files, designed for inheritence. This class is designed to be extensible for all image-sets and videos that are uploaded to XNAT. 
    As of Oct 2024, tt is intended to be used as a base class for the ArthroDiagnosticImage, ArthroVideo, SourceDicomDeIdentified, and MTurkSemanticSegmentation classes.
    
    Attributes:
    See the attributes of UIDandMetaInfo class.

    Example Usage:
    None -- this is a base class and should not be instantiated directly.
    """
    def __init__( self, intake_form: ORDataIntakeForm, ffn: Path ):
        super().__init__() # Call the __init__ method of the base class to create a uid for this instance
        assert os.path.isfile( ffn ), f'Inputted file not found: {ffn}'
        self._intake_form, self._ffn = intake_form, ffn
        self._new_ffn, self._metadata, self._image, self._derived_metadata, self._datetime, self._is_valid = '', None, None, {}, None, False
        assert self.is_valid_pydcom_uid( intake_form.uid ), f'Inputted uid must be a valid dicom uid: {intake_form.uid}'


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
    def is_dicom(self, ffn: Path)   -> bool:
        """
        Detect DICOM by content (magic bytes DICM at offset 128), not extension.
        DICOM Part-10 files have 128-byte preamble + b"DICM" marker.
        Returns False if file is < 132 bytes, unreadable, or missing DICM marker.
        """
        try:
            with open(ffn, 'rb') as f:
                f.seek(128)
                return f.read(4) == b'DICM'
        except (OSError, IOError):
            return False
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
        assert len( inst_str ) < 5, f'This function is intended for use with creating dicom file names from their metadata instance number. It is assumed that there may be no more than 9999 instances possible. You entered "{inst_str}", which exceeds that threshold.'
        if inst_str.isdigit() and int( inst_str ) < 10000:
            # Append the appropriate number of leading zeros (4-digit, fixed width)
            inst_str = inst_str.zfill(4)
        return f"{inst_str}-{patient_uid}"


#--------------------------------------------------------------------------------------------------------------------------
## Class(es) for arthro images files
class ArthroDiagnosticImage( ScanFile ):
    '''
    Class representing the XNAT Scan for Arthroscopic Diagnostic Images. Inherits from ScanFile.

    Attributes:
    still_num: str -- The still/frame number of the image within the sequence. Assumed to be within the title for arthro cases.

    Methods:
    None are intended for direct use by the user beyond the __init__ method.

    Example usage:
    ArthroDiagnosticImage( img_ffn=Path( r'...\\...\\examplefoldername' ), still_num='1', parent_uid='1_2_840_10008', config=..., intake_form=... )
    '''
    def __init__( self, img_ffn: Path, still_num: str, parent_uid: str, config: ConfigTables, intake_form: ORDataIntakeForm ) -> None:
        super().__init__( intake_form=intake_form, ffn=img_ffn )  # Call the __init__ method of the base class
        self._datetime, self._still_num = USCentralDateTime(), still_num
        self._validate_input()
        self._read_image( config=config )
        self._validate_image() 
        self._create_dicom_representation( parent_uid=parent_uid )


    def _validate_input( self ) -> None:
        assert self.is_jpg( self.ffn ), f'Inputted file must be a jpg file: {self.ffn}'
        assert self.still_num.isdigit(), f'Inputted still number must be a string of digits: {self.still_num}'


    def _read_image( self, config: ConfigTables ) -> None:
        self._image = ImageHash( reference_table=config, img=cv2.imread( self.ffn_str ) )


    def _validate_image( self ) -> None: # valid if the image has not yet been seen and if it does not match the template image.
        assert not isinstance( self.image, cv2.VideoCapture ), f'BUG: cannot be calling the is_similar_to_template_image method for {type(self).__name__} with a video file.'
        self._is_valid = not self.image.in_img_hash_metatable and not self.is_similar_to_template_image()


    def _create_dicom_representation( self, parent_uid: str ) -> None:
        file_meta = pydicomFileMetaDataset()
        file_meta.MediaStorageSOPClassUID = dcmUID( '1.2.840.10008.5.1.4.1.1.77.1.1.1' ) # Video Endoscopic Image IOD
        file_meta.MediaStorageSOPInstanceUID = dcmUID( self.generate_uid().replace( '_', '.' ) ) #to-do: need to create a uid in config for this media storage data type
        file_meta.ImplementationClassUID = dcmUID( parent_uid.replace( '_', '.' ) )
        file_meta.TransferSyntaxUID = ImplicitVRLittleEndian # Implicit VR Little Endian
        
        date_now_str, time_now_str = datetime.now().strftime( '%Y%m%d' ),   datetime.now().strftime( '%H%M%S.%f' )[:-3]
        date_img_str, time_img_str = self.datetime.date,                    self.datetime.time

        ds = pydicomFileDataset( self.new_ffn, {}, file_meta=file_meta, preamble=b"\0" * 128)
        ds.PatientName, ds.PatientID = self.redacted_string, self.redacted_string
        ds.ContentDate, ds.ContentTime = date_now_str, time_now_str
        ds.StudyDate, ds.StudyTime = date_img_str, time_img_str
        ds.StudyInstanceUID = dcmUID( parent_uid.replace( '_', '.' ) )
        ds.InstanceNumber = self.still_num
        ds.InstanceCreationDate, ds.InstanceCreationTime = date_img_str, '' #to-do: instance creation time is potentially gleanable from the original file info?
        ds.SeriesInstanceUID = dcmUID( self.uid.replace( '_', '.' ) )
        ds.SeriesDescription = f'Arthro. Diagn. Img. #{self.still_num}'

        # Store the original filename in the Image Comments field for backtracking purposes
        ds.ImageComments = self.ffn_str

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
## Class(es) for arthro videos
class ArthroVideo( ScanFile ):
    '''
    Class representing the XNAT Scan for Arthroscopic Videos. Inherits from ScanFile.
    
    Attributes:
    None -- see ScanFile for inherited attributes.

    Methods:
    None are intended for direct use by the user beyond the __init__ method.

    Example usage:
    ArthroVideo( vid_ffn=Path( r'...\\...\\examplefoldername' ), config=..., intake_form=... )
    '''
    def __init__( self, vid_ffn: Path, config: ConfigTables, intake_form: ORDataIntakeForm ) -> None:
        super().__init__( intake_form=intake_form, ffn=vid_ffn )  # Call the __init__ method of the base class
        self._validate_input()
        self._read_video()
        self._validate_image()
        

    def _validate_input( self ) -> None:    assert self.is_mp4( self.ffn ), f'Inputted file must be a mp4 file: {self.ffn_str}'


    def _read_video( self ) -> None:        self._image = cv2.VideoCapture( self.ffn_str )


    def _validate_image( self ) -> None:
        assert not isinstance( self.image, ImageHash ), f'Inputted file must be a video file: {self.ffn_str}'
        self._is_valid = self.image.isOpened()


#--------------------------------------------------------------------------------------------------------------------------
## Class for dicom (trauma) files
class SourceDicomDeIdentified( ScanFile ):
    '''
    Class representing the XNAT Scan for Source Dicom Files. Inherits from ScanFile.

    Attributes:
    None -- see ScanFile for inherited attributes.

    Methods:
    None are intended for direct use by the user beyond the __init__ method.

    # Example usage:
    SourceDicomDeIdentified( dcm_ffn=Path( r'...\\...\\exampledcmfilename' ), config=..., intake_form=... )
    '''
    def __init__( self, dcm_ffn: Path, config: ConfigTables, intake_form: ORDataIntakeForm ) -> None:
        super().__init__( intake_form=intake_form, ffn=dcm_ffn )  # Call the __init__ method of the base class
        assert self.is_dicom( self.ffn ), f'Inputted file must be a dicom file: {self.ffn}'
        self._datetime = USCentralDateTime()
        self._read_image( config=config )
        self._parse_for_derived_metadata()
        self._deidentify_dicom()
        self._validate_image() # deleted local method and made it inherited. seems like it should always be the same validation criteria
    

    def _read_image( self, config: ConfigTables ) -> None:
        self._metadata = dcmread( self.ffn )
        self._image = ImageHash( reference_table=config, img=self.metadata.pixel_array )

    def _validate_image( self ) -> None: # valid if the image has not yet been seen and if it does not match the template image.
        assert isinstance( self.image, ImageHash ), f'BUG: cannot be validate because the object.image data is not in ImageHash format.'
        self._is_valid = not self.image.in_img_hash_metatable and not self.is_similar_to_template_image()

    def _parse_for_derived_metadata( self ) -> None:
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
        # self._derived_metadata['GROUP'] = ''
        
        # Date and time info:
        dt_str = ''
        if 'ContentDate' in self.metadata:
            if len( self.metadata.ContentDate ) > 0:
                dt_str = self.metadata.ContentDate + ' ' + self.metadata.ContentTime
        elif  'InstanceCreationDate' in self.metadata and len( dt_str ) == 0:
            dt_str = self.metadata.InstanceCreationDate + ' ' + self.metadata.InstanceCreationTime
        self._derived_metadata['DATETIME'] = str( USCentralDateTime( dt_str.strip() ) )
        
        # Also check for ContentTime, SeriesTime, and StudyTime.
        self._derived_metadata['TIMEINFO'] = {}
        if 'ContentTime' in self.metadata:   self._derived_metadata['TIMEINFO']['ContentTime'] = self.metadata.ContentTime
        if 'SeriesTime' in self.metadata:    self._derived_metadata['TIMEINFO']['SeriesTime'] = self.metadata.SeriesTime
        if 'StudyTime' in self.metadata:     self._derived_metadata['TIMEINFO']['StudyTime'] = self.metadata.StudyTime
        self._derived_metadata['TIMEINFO'] = str( self._derived_metadata['TIMEINFO'] )

        # UID info:
        self._derived_metadata['UID_INFO'] = {}
        for element in self.metadata.iterall():
            if "UID" in element.name:       self._derived_metadata['UID_INFO'][element.name] = element.value.replace( '.', '_' ) # Must replace '.' with underscores because that is how theyre stored in xnat
        
        # Convert the dict to a string for storage in the metadata
        self._derived_metadata['UID_INFO'] = str( self._derived_metadata['UID_INFO'] )


    def _deidentify_dicom( self ) -> None:
        # Delegate all metadata scrubbing to the pure function in deidentify.py.
        # Behaviour is byte-identical to the previous inline implementation.
        assert self._metadata is not None, f'BUG: cannot be calling the _deidentify_metadata method for {type(self).__name__} prior to defining it.'
        deidentify_dataset( self._metadata, self.redacted_string )
        # Pixel-PHI review / redaction is handled separately (see T014 / T013).
    

#--------------------------------------------------------------------------------------------------------------------------
## Class for *a single* mturk batch file *row*
class MTurkSemanticSegmentation:
    """
    Thin adapter that converts one row of an MTurk batch results file into a
    canonical ``src.annotations.model.Annotation``.

    Bridges to ``src.annotations.importers.mturk.from_mturk_row`` — the
    annotation-layer importer that supersedes the original commented-out stub.
    Does NOT inherit from ScanFile: MTurk rows are not file-based scan artifacts.

    Usage
    -----
    ann = MTurkSemanticSegmentation.from_row(row)
    # or equivalently via constructor:
    ann = MTurkSemanticSegmentation(row).annotation

    Parameters
    ----------
    row : dict or pandas.Series with MTurk result columns:
          WorkerId, SubmitTime, *.pngImageData (base64-encoded PNG mask).

    Returns (via .annotation)
    -------------------------
    src.annotations.model.Annotation with
        annotation_type = 'binary_segmentation'
        tool            = 'mturk'
        annotator_id    = WorkerId  (opaque token, PHI-free)
        payload         = 2-D uint8 numpy array (decoded grayscale mask)
    """

    def __init__(self, row) -> None:
        from src.annotations.importers.mturk import from_mturk_row
        self._annotation = from_mturk_row(row)

    @property
    def annotation(self):
        """The canonical Annotation produced from this MTurk row."""
        return self._annotation

    @classmethod
    def from_row(cls, row) -> "MTurkSemanticSegmentation":
        """
        Convenience classmethod — identical to calling the constructor directly.

        Returns
        -------
        MTurkSemanticSegmentation instance (use .annotation to get the Annotation).
        """
        return cls(row)

