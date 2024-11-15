import os
from typing import Optional as Opt, Tuple, Union
import numpy as np
import pandas as pd
from datetime import datetime
from pydicom.dataset import FileDataset as pydicomFileDataset
from pydicom import Dataset, Sequence, dcmread, dcmwrite
from pathlib import Path, PurePosixPath
import shutil
import tempfile

from src.utilities import MetaTables, USCentralDateTime, XNATLogin, XNATConnection
from src.xnat_scan_data import *
from src.xnat_resource_data import *

# Define list for allowable imports from this module -- do not want to import _local_variables.
__all__ = ['SourceRFSession', 'SourceESVSession']


#--------------------------------------------------------------------------------------------------------------------------
## Base class for all xnat experiment sessions.
class ExperimentData():
    """
    A class representing the base structure of an XNAT Experiment, in which all source- and derived-data are stored.
    - Experiments may only exist as a child of a subject, and may contain multiple scans and resources.

    Attributes:
    intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
    tmp_source_data_dir (Path): local tmp directory in which all source data is temporarily stored before being pushed to XNAT.
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'rf' for radio fluoroscopic data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM' for radio fluoroscopic data.

    Methods:
    - Most of these methods are for instantiating only and are not meant to be called directly.
    - Some methods are intentionally undefined with the expectation that they will be defined in inherited classes.

    publish_to_xnat(): Publishes the experiment session to XNAT, including all associated scans and resources.
    write(): Writes the experiment session to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    write_publish_catalog_subroutine(): Writes the experiment session to a zipped folder and then publishes it to XNAT, including metatables.

    Example Usage:
    None -- this is a base class and should not be instantiated directly.
    """
    def __init__( self, intake_form: ORDataIntakeForm, invoking_class: str ) -> None:
        """
        Initialize the ExperimentData object with the inputted ORDataIntakeForm object and the invoking class name.
        
        Args:
        intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
        invoking_class (str): name of the class that is invoking this class, e.g., 'SourceRFSession' or 'SourceESVSession'.
            - This is used to determine the schema_prefix_str and scan_type_label attributes, the former of which is necessary and the latter of which is helpful for XNAT publishing.
        """
        assert os.path.isdir( intake_form.relevant_folder ), f"Inputted path must be a valid directory; inputted path: {intake_form.relevant_folder}"
        assert os.path.exists( intake_form.saved_ffn_str ), f"Inputted IntakeForm must be valid; no file found at: {intake_form.saved_ffn_str}"

        self._intake_form, self._tmp_source_data_dir = intake_form, intake_form.saved_ffn.parent / Path( 'SOURCE_DATA' ) # type: ignore
        if not os.path.exists( self.tmp_source_data_dir ):  os.makedirs( self.tmp_source_data_dir )
        self._df, self._is_valid = pd.DataFrame(), False    # Derived in derived classes' init method

        assert invoking_class in __all__, f"Invoking class must be one of the following strings: {__all__}; you entered: {invoking_class}"
        if invoking_class   == 'SourceRFSession':   self._schema_prefix_str, self._scan_type_label = 'rf',  'DICOM'
        elif invoking_class == 'SourceESVSession':  self._schema_prefix_str, self._scan_type_label = 'esv', 'DICOM_MP4'
        elif invoking_class == 'DerivedData':       self._schema_prefix_str, self._scan_type_label = 'otherDicom', 'DERIVED'

    @property
    def intake_form( self )             -> ORDataIntakeForm:        return self._intake_form
    @property
    def tmp_source_data_dir( self )     -> Path:                    return self._tmp_source_data_dir
    @property
    def df( self )                      -> pd.DataFrame:            return self._df
    @property
    def is_valid( self )                -> bool:                    return self._is_valid
    @property
    def schema_prefix_str( self )       -> str:                     return self._schema_prefix_str
    @property
    def scan_type_label( self )         -> str:                     return self._scan_type_label

    
    def _populate_df( self, metatables: MetaTables ):               raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _check_session_validity( self, metatables: MetaTables ):    raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )


    #--------------------------------------------XNAT-Publishing helpers and methods----------------------------------------------------------
    def _generate_queries( self, xnat_connection: XNATConnection ) -> Tuple[str, str, str, str, str]:
        # Create query strings and select object in xnat then create the relevant objects
        exp_label = ( 'SOURCE_DATA' + '-' + self.intake_form.uid )
        scan_label = '0' #to-do: potentially an issue if there are multiple scans in a session
        # scan_type_label = scan_type_label
        # scan_type_series_description = 
        # resource_label = resource_label
        proj_qs = '/project/' + xnat_connection.xnat_project_name
        proj_qs = PurePosixPath( proj_qs ) # to-doneed to revisit this because it is hard-coded but Path makes it annoying
        subj_qs = proj_qs / 'subject' / str( self.intake_form.uid )
        exp_qs = subj_qs / 'experiment' / exp_label
        scan_qs = exp_qs / 'scan' / scan_label
        files_qs = scan_qs / 'resource' / 'files'
        resource_label = 'SRC'
        return str( subj_qs ), str( exp_qs ), str( scan_qs ), str( files_qs ), resource_label


    def _select_objects( self, xnat_connection: XNATConnection, subj_qs: str, exp_qs: str, scan_qs: str, files_qs: str ) -> Tuple[object, object, object]:
        subj_inst = xnat_connection.server.select( str( subj_qs ) )
        assert not subj_inst.exists(), f'Subject already exists with the uri:\n{subj_inst}'         # type: ignore
        exp_inst = xnat_connection.server.select( str( exp_qs ) )
        assert not exp_inst.exists(), f'Experiment already exists with the uri:\n{exp_inst}'        # type: ignore
        scan_inst = xnat_connection.server.select( str( scan_qs ) )
        assert not scan_inst.exists(), f'Scan already exists with the uri:\n{scan_inst}'            # type: ignore
        return subj_inst, exp_inst, scan_inst


    def write( self, metatables: MetaTables, zip_dest: Opt[Path] = None, verbose: Opt[bool] = False )   -> Tuple[dict, MetaTables]: raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    

    def publish_to_xnat( self, xnat_connection: XNATConnection, validated_login: XNATLogin, zipped_data: dict, delete_zip: Opt[bool] = True, verbose: Opt[bool] = False ) -> None:
        if verbose:     print( f'\t...Pushing {self.schema_prefix_str} Session Data to XNAT...' )
        subj_qs, exp_qs, scan_qs, files_qs, resource_label = self._generate_queries( xnat_connection=xnat_connection )
        subj_inst, exp_inst, scan_inst = self._select_objects( xnat_connection=xnat_connection, subj_qs=subj_qs, exp_qs=exp_qs, scan_qs=scan_qs, files_qs=files_qs )

        # Create the items in stepwise fashion -- to-do: can't figure out how to create all in one go instead of attrs.mset(), it wouldn't work properly
        subj_inst.create()                                                                                  # type: ignore -- doesnt recognize .create() attribute of subj_inst
        subj_inst.attrs.mset( { f'xnat:subjectData/GROUP': self.intake_form.group } )                       # type: ignore -- doesnt recognize .attrs attribute of subj_inst
        exp_inst.create( **{    f'experiments': f'xnat:{self.schema_prefix_str}SessionData' })               # type: ignore -- doesnt recognize .create() attribute of exp_inst
        exp_inst.attrs.mset( {  f'xnat:experimentData/ACQUISITION_SITE': self.intake_form.acquisition_site, # type: ignore -- doesnt recognize .attrs attribute of exp_inst
                                f'xnat:experimentData/DATE': self.intake_form.datetime.date                 
                            } )
        scan_inst.create( **{   f'scans': f'xnat:{self.schema_prefix_str}ScanData' } )                      # type: ignore -- doesnt recognize .create() attribute of scan_inst
        scan_inst.attrs.mset( { f'xnat:{self.schema_prefix_str}ScanData/TYPE': self.scan_type_label,        # type: ignore -- doesnt recognize .attrs attribute of scan_inst
                                f'xnat:{self.schema_prefix_str}ScanData/SERIES_DESCRIPTION': self.intake_form.ortho_procedure_type,
                                f'xnat:{self.schema_prefix_str}ScanData/QUALITY': self.intake_form.scan_quality,
                                f'xnat:imageScanData/NOTE': f'BY: {validated_login.validated_username.upper()}; AT: {USCentralDateTime(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))}'
                            } )

        # Assuming that zipped_data is a dict w keys corresponding to the unique types of data to be pushed and the corresponding values being the file paths to the zipped data, iterate through the dict
        for key_zipped_ffn, value_dict in zipped_data.items():
            if verbose:     print( f'\t\t...Uploading {value_dict["FORMAT"]}-formatted files to XNAT...' )
            scan_inst.resource( resource_label ).put_zip( key_zipped_ffn, content=value_dict['CONTENT'], format=value_dict['FORMAT'], tags='DATA' ) # type: ignore -- doesnt recognize .resource attribute of scan instance
        
        # Must also publish the resource file(s)
        self.intake_form.push_to_xnat( subj_inst=subj_inst, verbose=verbose )
        # # Old method for pushing files to XNAT:
        # # scan_inst.resource( 'DATA' ).file( 'mp4_vid.mp4' ).insert( vid_ffn, content='VIDEO', format='MP4', tags='OR_DATA', overwrite=True )
        # # scan_inst.resource( resource_label ).put_zip( zipped_ffn, content='IMAGE', format='DICOM', tags='POST_OP_DATA', overwrite=True )
        # scan_inst.resource( resource_label ).put_zip( zipped_ffn, content='OR_DATA', format='DICOM', tags='', overwrite=True ) # type: ignore -- doesnt recognize .resource attribute of scan instance

        if delete_zip:  [os.remove(key) for key in zipped_data.keys()] # to-do: not sure that this actually work as intended ie deletes the files corresponding to the key names
        if verbose:
            print( f'\t...{self.schema_prefix_str}Session succesfully pushed to XNAT!' )
            print( f'\t...Successfully deleted zip file:\n' + '\n'.join(f'\t\t{key}' for key in zipped_data.keys()) + '\n')


    def write_publish_catalog_subroutine( self, metatables: MetaTables, xnat_connection: XNATConnection, validated_login: XNATLogin, verbose: Opt[bool] = False, delete_zip: Opt[bool] = True ) -> MetaTables:
        try:
            zipped_data, metatables = self.write( metatables=metatables, verbose=verbose )
        except Exception as e:
            if verbose: print( f'\n!!! Failed to write zipped file; exiting without publishing to XNAT.\n\tError given:\n\t{e}' )
            raise

        # Get subject instance so we can delete it if necessary during the ensuing try-except block(s)
        subj_qs, exp_qs, scan_qs, files_qs, _ = self._generate_queries( xnat_connection=xnat_connection )
        subj_inst, _, _ = self._select_objects( xnat_connection=xnat_connection, subj_qs=subj_qs, exp_qs=exp_qs, scan_qs=scan_qs, files_qs=files_qs )

        # Try to publish the data to xnat; if it fails, delete the subject instance
        status_text = f'\t...Attempting to publish {self.schema_prefix_str} session to XNAT...'
        try:
            try:
                self.publish_to_xnat( xnat_connection=xnat_connection, validated_login=validated_login, zipped_data=zipped_data, verbose=verbose, delete_zip=delete_zip )
                status_text = f'\t...Successfully published {self.schema_prefix_str} session to XNAT!\nAttempting to push metatables to XNAT...'
            except Exception as e:
                status_text = f'\t!!! Failed to publish {self.schema_prefix_str} session to XNAT!\nChecking if subject was successfully pushed to xnat...'
                if subj_inst.exists(): # type: ignore
                    status_text += f'\n\t...Subject exists; attempting to delete subject...'
                    subj_inst.delete() # type: ignore
                    status_text += f'\n\t...Subject deleted.'
                    print( f'\n\t!!!Do not try to upload this case again without contacting the data librarian!!!')
                return metatables

            # If successful, try to push the metatables config to xnat
            try: 
                metatables.push_to_xnat( verbose=verbose )
                status_text = f'\t...Successfully pushed config file to XNAT!'
            except Exception as e:
                status_text = f'\t!!! Failed to push config file to XNAT!\nChecking if subject was successfully pushed to xnat...'
                if subj_inst.exists(): # type: ignore
                    status_text += f'\n\t...Subject exists; attempting to delete subject...'
                    subj_inst.delete() # type: ignore
                    status_text += f'\n\t...Subject deleted.'
        except:
            self._write_error_log_file( metatables=metatables, validated_login=validated_login, status_text=status_text, error_message=e )
            raise

        # Delete local copy of the metatables
        if verbose:     print( f'\t...Deleting local copy of metatables...' )
        if os.path.exists( metatables.config_ffn ): os.remove( metatables.config_ffn )
        else: print(f'---------- error deleting metatables config file; no file found at:----------\n\t\t{metatables.config_ffn}')
        return metatables
    
    def _write_error_log_file( self, metatables: MetaTables, validated_login: XNATLogin, status_text: str, error_message: Exception ) -> str:
        # Initialize a text that will eventually be written to a file, beginning with the date, time, user's hawkid, , whether the subject exists on xnat, whether the metatables were updates, and the error message
        text = f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        text += f'User: {validated_login.validated_username}\n'
        text += f'Attempted Session Creation Type: {self.schema_prefix_str}\n'
        text += f'Intake Form:\n{self.intake_form}\n'
        text += status_text + f'\n'
        text += f"\n{'---'*25}\n"
        text += f'Error Message:\n{error_message}\n'

        # Write the text to a file in the user's downloads folder
        failed_ffn = Path.home() / Path( 'Downloads' ) / Path( f'FAILED_{self.schema_prefix_str}_SESSION_CREATION_{datetime.now().strftime("%Y-%m-%d_%H-%M-%S")}.txt' )
        with open( failed_ffn, 'w' ) as f: f.write( text )
        print( f'\t--- Log file detailing the failed {self.schema_prefix_str} session creation has been written to:\n\t{failed_ffn}\n\n\tPlease notify the Data Librarian ({metatables.data_librarian}).' )
        return text

    


#--------------------------------------------------------------------------------------------------------------------------
## Class for all radio fluoroscopic (source image) sessions.
from src.xnat_experiment_data import ExperimentData
class SourceRFSession( ExperimentData ):
    """
    A class representing the XNAT Experiment for Radio Fluoroscopic (RF) Source Images. Inherits from ExperimentData. Intended for structuring trauma cases with fluoroscopic image sequences.

    Inputs:
    - dcm_dir (Path): directory containing all dicom files detailing a surgical performance.
    - intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT.
    - login (XNATLogin): login object containing the user's validated username and password.
    - xnat_connection (XNATConnection): connection object containing the user's validated xnat server and project name.
    - metatables (MetaTables): metatables object containing the user's validated metatables configuration.

    Attributes:
    intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
    tmp_source_data_dir (Path): local tmp directory in which all source data is temporarily stored before being pushed to XNAT.
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'rf' for radio fluoroscopic data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM' for radio fluoroscopic data.

    Methods (unique to this inherited class):
    write(): Writes the SourceRFSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    - See docstring for ExperimentData for other methods.

    Example Usage:
    SourceRFSession( dcm_dir=Path('path/to/dcm_dir'), intake_form=ORDataIntakeForm, login=XNATLogin, xnat_connection=XNATConnection, metatables=MetaTables )
    """
    def __init__( self, intake_form: ORDataIntakeForm, metatables: MetaTables ):
        """
        Initialize the SourceRFSession object with the inputted ORDataIntakeForm object and the invoking class name.

        Note on boolean columns in the dataframe:
        - 'IS_VALID' (bool): flag indicating whether the file is in a (valid) dicom-readable format.
        - 'IS_QUESTIONABLE' (bool): flag indicating whether the file is possibly a duplicate within-case, based on image hash strings.
    
        Populate a dataframe to represent all intraoperative images in the inputted folder. Check the validity of the session and mine metadata for the session.
         """
        super().__init__( intake_form=intake_form, invoking_class='SourceRFSession' ) # Call the __init__ method of the base class
        self._populate_df( metatables=metatables )
        self._check_session_validity( metatables=metatables )
        if self.is_valid:   self._mine_session_metadata() # necessary for publishing to xnat.

    def _populate_df( self, metatables: MetaTables ):
        self._init_rf_session_dataframe()
        all_ffns = self._all_dicom_ffns()
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext != '.dcm':
                self._df.loc[idx, ['FN', 'EXT', 'IS_VALID']] = [fn, ext, False]
                continue
            deid_dcm = SourceDicomDeIdentified( dcm_ffn=ffn, metatables=metatables, intake_form=self.intake_form )
            self._df.loc[idx, ['FN', 'EXT', 'OBJECT', 'IS_VALID']] = [fn, ext, deid_dcm, deid_dcm.is_valid]
            # if deid_dcm.is_valid:
            #     dt_data = self._query_dicom_series_time_info( deid_dcm )
            #     self._df.loc[idx, ['DATE', 'INSTANCE_TIME', 'SERIES_TIME', 'INSTANCE_NUM']] = dt_data

        # Need to check within-case for duplicates -- apparently those do exist.
        hash_strs = set()
        for idx, row in self.df.iterrows():
            if row['IS_VALID']:
                if row['OBJECT'].image.hash_str in hash_strs:   self._df.at[idx, 'IS_QUESTIONABLE'] = True
                else:
                    hash_strs.add( row['OBJECT'].image.hash_str )
                    self._df.at[idx, 'IS_QUESTIONABLE'] = False
        
    def _check_session_validity( self, metatables: MetaTables ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
        self._is_valid = self.df['IS_VALID'].any() and not metatables.item_exists( table_name='SUBJECTS', item_name=self.intake_form.uid )
        
    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        
        # Process the dataframe to overwrite metadata and ensure consistency
        num_valid_shots = self.df['IS_VALID'].sum()
        for idx, row in self.df.iterrows():
            if row['IS_VALID'] is False:    continue
            
            # Append ContentTime and InstanceNumber to the DataFrame
            dicom_obj = row['OBJECT']
            self._df.at[idx, 'ContentTime']     = dicom_obj.ContentTime     if hasattr( dicom_obj, 'ContentTime' )      else None
            self._df.at[idx, 'InstanceNumber']  = dicom_obj.InstanceNumber  if hasattr( dicom_obj, 'InstanceNumber' )   else None
            
            # Pull date and UID info and write as new private-tag pair so we can overwrite it with standardized info.
            if hasattr( dicom_obj, 'StudyDate' ):
                dicom_obj.metadata.add_new((0x0019, 0x1001), 'DA', 'Old_StudyDate: ' + dicom_obj.StudyDate )
            dicom_obj.StudyDate = self.intake_form.operation_date # Provided by intake form
            if hasattr( dicom_obj, 'StudyTime' ):
                dicom_obj.metadata.add_new((0x0019, 0x1002), 'TM', 'Old_StudyTime: ' + dicom_obj.StudyTime )
            dicom_obj.StudyTime = self.intake_form.epic_start_time # Provided by intake form
            if hasattr( dicom_obj, 'StudyInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1002), 'UI', 'Old_StudyInstanceUID: ' + dicom_obj.StudyInstanceUID)
            dicom_obj.StudyInstanceUID = self.intake_form.uid
            if hasattr( dicom_obj, 'SeriesInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1003), 'UI', 'Old_SeriesInstanceUID: ' + dicom_obj.SeriesInstanceUID)
            dicom_obj.SeriesInstanceUID = self.intake_form.uid
            if hasattr( dicom_obj, 'SOPInstanceUID' ):
                dicom_obj.metadata.add_new((0x0019, 0x1004), 'UI', 'Old_SOPInstanceUID: ' + dicom_obj.SOPInstanceUID)
            dicom_obj.SOPInstanceUID = self.intake_form.uid
            if hasattr( dicom_obj, 'NumberOfStudyRelatedInstances' ):
                dicom_obj.metadata.add_new((0x0019, 0x1005), 'IS', 'Old_NumberOfStudyRelatedInstances: ' + dicom_obj.NumberOfStudyRelatedInstances)
                dicom_obj.NumberOfStudyRelatedInstances = num_valid_shots
            if row['IS_QUESTIONABLE']: # If the shot is questionably a duplicate within-case, add a private tag to explain why.
                dicom_obj.metadata.add_new( (0x0019, 0x1007), 'LT', 'This shot was flagged by the XNAT-Interact software as a potential duplicate (within-performance).' )
            
            # Walk through each key-value pair in the _derived_metadata dict and add_new to the DICOM object as a long length text tag.
            for key, value in dicom_obj._derived_metadata.items():
                dicom_obj.metadata.add_new( (0x0019, 0x1000), 'LT', f'{key}: {value}' )
             
            # Create a private long length text tag to explain what this function did.
            dicom_obj.metadata.add_new( (0x0019, 0x1006), 'LT', f'Metadata de-identified & standardized by XNAT-Interact script on {dicom_obj._derived_metadata["DATETIME"]}.' )

            # Save the modified DICOM object back to the DataFrame; Generate a new file name for each shot in the session given its instance number, then overwrite metadata to ensure consistency throughout all shots.
            self._df.at[idx, 'OBJECT'] = dicom_obj
            self._df.at[idx, 'NEW_FN'] = dicom_obj.generate_source_image_file_name( str( dicom_obj.metadata.InstanceNumber ), self.intake_form.uid )

        # self._derive_acquisition_site_info() # to-do: should warn the user that any mined info is inconsistent with their input
        self._df = self.df.sort_values( by='NEW_FN', inplace=False )

    # ---------------------------------_populate_df Helper Methods---------------------------------
    def _all_dicom_ffns( self ) -> list:
        # Filter out files with extensions other than .dcm
        all_ffns = [f for f in self.intake_form.relevant_folder.rglob("*") if f.suffix.lower() == '.dcm' or f.suffix == '']
        return [f for f in all_ffns if f.suffix.lower() == '.dcm' or f.suffix == '']

    def _init_rf_session_dataframe( self ):
        df_cols = { 'FN': 'str', 'EXT': 'str', 'NEW_FN': 'str', 'OBJECT': 'object', 'IS_VALID': 'bool', 'IS_QUESTIONABLE': bool,
                    'DATE': 'str', 'SERIES_TIME': 'str', 'INSTANCE_TIME': 'str', 'INSTANCE_NUM': 'str' }
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )

    def _query_dicom_series_time_info( self, deid_dcm: SourceDicomDeIdentified ) -> list:
        dt_data = [deid_dcm.datetime.date, deid_dcm.datetime.time, None, deid_dcm.metadata.InstanceNumber]
        if 'SeriesTime' in deid_dcm.metadata:    dt_data[2] = deid_dcm.metadata.SeriesTime
        if 'ContentTime' in deid_dcm.metadata:   dt_data[2] = deid_dcm.metadata.ContentTime
        if 'StudyTime' in deid_dcm.metadata:     dt_data[2] = deid_dcm.metadata.StudyTime
        return dt_data
    # ---------------------------------_populate_df Helper Methods---------------------------------

    def __str__( self ) -> str:
        select_cols = ['NEW_FN', 'IS_VALID', 'IS_QUESTIONABLE']
        df, intake_form = self.df[select_cols].copy(), self.intake_form
        num_valid, num_questionable, num_rows = df['IS_VALID'].sum(), df['IS_QUESTIONABLE'].sum(), len( df )
        valid_str = f'{self.is_valid} ({num_valid}/{num_rows})'
        questionable_str = f'{num_questionable}/{num_rows}'
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{intake_form.uid}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{intake_form.datetime}\nValid:\t\t\t{valid_str}\nQuestionable:\t\t{questionable_str}\n{df.head()}\n...\n{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{None}\nValid:\t\t\t{valid_str}\nQuestionable:\t\t{questionable_str}\n{df.head()}\n...\n{df.tail()}'

    def write( self, metatables: MetaTables, verbose: Opt[bool] = False ) -> Tuple[dict, MetaTables]:
        """
        Writes the SourceRFSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
        Inputs:
        - metatables (MetaTables): metatables object containing the user's validated metatables configuration.
        - verbose (bool): flag indicating whether to print intermediate steps to the console.

        Outputs:
        - zipped_data (dict): dictionary containing the path to the zipped folder of source data.
        - metatables (MetaTables): metatables object containing the user's validated metatables configuration.

        Example Usage:
        rf_sess.write( metatables=MetaTables, verbose=True )
        """
        assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."
        
        # (Try to) Add the subject to the metatables
        metatables.add_new_item( table_name='SUBJECTS', item_name=self.intake_form.uid, item_uid=self.intake_form.uid, verbose=verbose,
                                extra_columns_values={ 'ACQUISITION_SITE': metatables.get_uid( table_name='ACQUISITION_SITES', item_name=self.intake_form.acquisition_site ),
                                                      'GROUP': metatables.get_uid( table_name='GROUPS', item_name=self.intake_form.group ) }
                                )

        # Zip the mp4 and dicom data to separate folders
        zipped_data, home_dir = {}, self.tmp_source_data_dir
        num_dicom = 0
        with tempfile.TemporaryDirectory( dir=home_dir ) as dcm_temp_dir:
            for idx in range( len( self.df ) ): # Iterate through each row in the DataFrame, writing each to a temp directory before we zip it up and delete the unzipped folder.
                # if self.df.loc[idx, 'IS_VALID']:
                
                    file_obj_rep = self.df.loc[idx, 'OBJECT']
                    assert isinstance( file_obj_rep, SourceDicomDeIdentified ), f"Object representation of file at index {idx} is {type( file_obj_rep )}, which is not a valid SourceDicomDeIdentified object."
                    dcmwrite( os.path.join( dcm_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) ), file_obj_rep.metadata )          # type: ignore
                    tmp = { 'SUBJECT': metatables.get_uid( table_name='SUBJECTS', item_name=self.intake_form.uid ), 'INSTANCE_NUM': self.df.loc[idx, 'NEW_FN'] }
                    metatables.add_new_item( table_name='IMAGE_HASHES', item_name=file_obj_rep.image.hash_str, item_uid=file_obj_rep.uid, # type: ignore
                                            extra_columns_values = tmp, verbose=verbose
                                            )
                    num_dicom += 1
                
            # Zip these temporary directories into a slightly-less-temporary directory.
            dcm_zip_path = os.path.join( home_dir, "dicom_files.zip" )
            dcm_zip_full_path = shutil.make_archive( dcm_zip_path[:-4], 'zip', dcm_temp_dir )
            zipped_data[dcm_zip_full_path] = { 'CONTENT': 'IMAGE', 'FORMAT': 'DICOM', 'TAG': 'INTRA_OP' }

        if verbose:     print( f'\t...Zipped folder(s) of {num_dicom} dicom files successfully written to:\n\t\t{dcm_zip_full_path}' )
        return zipped_data, metatables


#--------------------------------------------------------------------------------------------------------------------------
## Class for arthroscopy post-op diagnostic images.
class SourceESVSession( ExperimentData ):
    """
    A class representing the XNAT Experiment for Arthroscopy Post-Op Diagnostic Images and Intraoperative videos. Inherits from ExperimentData. Intended for structuring arthro cases.

    Inputs:
    - intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded with the source data.
    - metatables (MetaTables): metatables object containing the user's validated metatables configuration.

    Attributes:
    intake_form (ORDataIntakeForm): digitized json-formatted form detailing the surgical data to be uploaded to XNAT. This is also uploaded.
    tmp_source_data_dir (Path): local tmp directory in which all source data is temporarily stored before being pushed to XNAT.
    df (pd.DataFrame): dataframe containing all relevant data for the experiment session, e.g., a fluorosequence of 15 images will have 15 rows in df.
    is_valid (bool): flag indicating whether the experiment session is valid and can be pushed to XNAT.
    schema_prefix_str (str): string prefix for the schema type of the experiment session, e.g., 'esv' for endoscopic video data.
    scan_type_label (str): string label for the type of scan data, e.g., 'DICOM_MP4' for arthroscopic video data.

    Methods (unique to this inherited class):
    write(): Writes the SourceESVSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
    - See docstring for ExperimentData for other methods.

    Example Usage:
    SourceESVSession( intake_form=ORDataIntakeForm, metatables=MetaTables )
    """
    def __init__( self, intake_form: ORDataIntakeForm, metatables: MetaTables ) -> None:
        """
        Initializes the SourceESVSession object.
        Populate a dataframe to represent all post-op images and intraoperative videos in the inputted folder. Check the validity of the session and mine metadata for the session.
        """
        super().__init__( intake_form=intake_form, invoking_class='SourceESVSession' ) # Call the __init__ method of the base class
        self._populate_df( metatables=metatables )
        self._check_session_validity( metatables=metatables )
        if self.is_valid:   self._mine_session_metadata() # necessary for publishing to xnat.


    @property
    def mp4( self )                             -> List[ArthroVideo]:                               return self._mp4
    @mp4.setter
    def mp4(self, videos: List[ArthroVideo])    -> None:
        if isinstance(videos, list) and all(isinstance(video, ArthroVideo) for video in videos):    self._mp4 = videos
        else:                                                                                       raise ValueError("mp4 must be a list of ArthroVideo objects.")

    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        
        # For each row, generate a new file name now that we have a session label.
        vid_count = 0
        for idx in range( len( self.df ) ): # to-do: BUG - for some reason the iterrows() enumerator creates a row variable that cannot be accessed -- error occurs because the str method is overridden somewhere...
            if self.df.loc[idx, 'IS_VALID']:
                file_obj_rep = self.df.loc[idx, 'OBJECT']
                if isinstance( file_obj_rep, ArthroVideo ): # video is assigned instance number 000. Our convention is to begin at 001 for image files.
                    if vid_count == 0:  vid_prefix, vid_count = '000', vid_count + 1 #to-do: horrendous code; need to fix this
                    else:               vid_prefix =  str( len( self.df ) + 1 ).zfill( 3 )
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( vid_prefix, self.intake_form.uid )
                    # self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( '000', file_obj_rep.uid_info['Video_UID'] ) 
                    # self._df.loc[idx, 'NEW_FN'] = self.df.loc[idx, 'OBJECT'].uid_info['Video_UID']
                elif isinstance( self.df.loc[idx, 'OBJECT'], ArthroDiagnosticImage ):
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( file_obj_rep.still_num, self.intake_form.uid ) # type: ignore
                    # self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( file_obj_rep.still_num, file_obj_rep.uid_info['Still_UID'] ) # type: ignore
                else:
                    raise ValueError( f"Unrecognized object type: {type( self.df.loc[idx, 'OBJECT'] )}." )


    def _populate_df( self, metatables: MetaTables ):
        # Read in mp4 data
        mp4_ffn = list( self.intake_form.relevant_folder.rglob("*.[mM][pP]4") )
        assert len( mp4_ffn ) > 0, f"There should be at least one (1) mp4 file in the directory; found {len( mp4_ffn )} mp4 files."
        self._mp4 = [ArthroVideo( vid_ffn=ffn, metatables=metatables, intake_form=self.intake_form ) for ffn in mp4_ffn]
        if isinstance( self.mp4, list ): # Check that all mp4 files are valid
            for vid in self.mp4: assert vid.is_valid, f"Could not open video file: {vid.ffn}"

        # Read all jpg images;  sort images by their creation date-time, append mp4 ffn to the list before we build the dataframe
        all_ffns = list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][gG]") ) + list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][eE][gG]") )
        if len( all_ffns ) == 0: # prompt the user to confirm that they do indeed want to proceed without any images.
            print( f'\n\tNo image files were found in the inputted folder; if this is correct, enter "1" to proceed, otherwise "2" to exit.' )
            print( f'\t\tFiles found: {all_ffns}' )
            proceed_without_images = self.intake_form.prompt_until_valid_answer_given( 'No Images in Found in Folder', acceptable_options=['1', '2'] ) 
            if proceed_without_images != '1': raise ValueError( f'User did not enter "1" to proceed without images; software currently does not support this option -- exiting application...' )
            print( f'\n\t...Proceeding without images...' )
        # assert len( all_ffns ) > 0, f"No image files found in the inputted folder; make sure that all image files in folder have the correct ('.jpg' or '.jpeg') extension.\n\tDirectory given:  {self.intake_form.relevant_folder}."
            all_ffns = mp4_ffn
        else:
            all_ffns = sorted( all_ffns, key=lambda x: os.path.getctime( x ) ) + mp4_ffn
        
        # Assemble into a dataframe
        df_cols = { 'FN': 'str', 'NEW_FN': 'str', 'OBJECT': 'object', 'TYPE': 'str', 'IS_VALID': 'bool'}    # ---Used to be its own function---
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )              # ---------------------------------
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        mp4_idx = 0
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext.lower() == '.mp4':
                self._df.loc[idx, ['FN', 'OBJECT', 'IS_VALID', 'TYPE']] = [fn, self.mp4[mp4_idx], self.mp4[mp4_idx].is_valid, 'MP4']
                mp4_idx += 1
            elif ext.lower() in ['.jpg', '.jpeg']:
                instance_num = str( idx+1 )
                diag_img_obj = ArthroDiagnosticImage( img_ffn=ffn, metatables=metatables, intake_form=self.intake_form, still_num=instance_num, parent_uid=self.intake_form.uid ) 
                self._df.loc[idx, ['FN', 'OBJECT', 'IS_VALID', 'TYPE']] = [fn, diag_img_obj, diag_img_obj.is_valid, 'JPG']
            else:
                raise ValueError( f"Unrecognized file extension: {ext}.\n\tFile: {ffn}" )

        # Need to check within-case for duplicates -- apparently those do exist.
        hash_strs = set()
        for idx, row in self.df.iterrows():
            if row['IS_VALID'] and isinstance( row['OBJECT'], ArthroDiagnosticImage ):
                if row['OBJECT'].image.hash_str in hash_strs:
                    self._df.at[idx, 'IS_VALID'] = False
                else:
                    hash_strs.add( row['OBJECT'].image.hash_str )
        
        
    def _check_session_validity( self, metatables: MetaTables ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
        self._is_valid = True if self.df['IS_VALID'].any() and not metatables.item_exists( table_name='SUBJECTS', item_name=self.intake_form.uid ) else False


    def write( self, metatables: MetaTables, verbose: Opt[bool] = False ) -> Tuple[dict, MetaTables]:
        """
        Writes the SourceESVSession to a zipped folder in a temporary local directory, which can then be pushed to XNAT.
        Inputs:
        - metatables (MetaTables): metatables object containing the user's validated metatables configuration.
        - verbose (bool): flag indicating whether to print intermediate steps to the console.

        Outputs:
        - zipped_data (dict): dictionary containing the path to the zipped folder of source data.
        - metatables (MetaTables): metatables object containing the user's validated metatables configuration.

        Example Usage:
        esv_sess.write( metatables=MetaTables, verbose=True )
        """
        assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."

        # (Try to) Add the subject to the metatables
        metatables.add_new_item( table_name='SUBJECTS', item_name=self.intake_form.uid, item_uid=self.intake_form.uid, verbose=verbose,
                                extra_columns_values={ 'ACQUISITION_SITE': metatables.get_uid( table_name='ACQUISITION_SITES', item_name=self.intake_form.acquisition_site ),
                                                      'GROUP': metatables.get_uid( table_name='GROUPS', item_name=self.intake_form.group ) }
                                )

        # Zip the mp4 and dicom data to separate folders
        zipped_data, home_dir = {}, self.tmp_source_data_dir
        num_dicom, num_mp4 = 0, 0
        with tempfile.TemporaryDirectory( dir=home_dir ) as mp4_temp_dir, \
            tempfile.TemporaryDirectory( dir=home_dir ) as dcm_temp_dir:
            for idx in range( len( self.df ) ): # Iterate through each row in the DataFrame, writing each to a temp directory before we zip it up and delete the unzipped folder.
                if self.df.loc[idx, 'IS_VALID']:
                    file_obj_rep = self.df.loc[idx, 'OBJECT']
                    assert isinstance( file_obj_rep, (ArthroDiagnosticImage, ArthroVideo) ), f"Object representation of file at index {idx} is {type( file_obj_rep )}, which is neither an ArthroDiagnosticImage nor an ArthroVideo object."
                    if isinstance( file_obj_rep, ArthroDiagnosticImage ):
                        dcmwrite( os.path.join( dcm_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) ), file_obj_rep.metadata )          # type: ignore
                        tmp = { 'SUBJECT': metatables.get_uid( table_name='SUBJECTS', item_name=self.intake_form.uid ), 'INSTANCE_NUM': self.df.loc[idx, 'NEW_FN'] }
                        metatables.add_new_item( table_name='IMAGE_HASHES', item_name=file_obj_rep.image.hash_str, item_uid=file_obj_rep.uid, # type: ignore
                                                extra_columns_values = tmp, verbose=verbose
                                                )
                        num_dicom += 1
                
                    elif isinstance( file_obj_rep, ArthroVideo ): # Don't need to add this to metatables because the diagnostic images (frames from the video) should suffice.
                        vid_ffn = os.path.join( self.intake_form.relevant_folder, str( self.df.loc[idx,'FN'] ) + '.mp4' )
                        shutil.copy( vid_ffn, os.path.join( mp4_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) + '.mp4' ) )
                        num_mp4 += 1
                
            # Zip these temporary directories into a slightly-less-temporary directory.
            mp4_zip_path = os.path.join( home_dir, "mp4_files.zip" )
            dcm_zip_path = os.path.join( home_dir, "dicom_files.zip" )
            mp4_zip_full_path = shutil.make_archive( mp4_zip_path[:-4], 'zip', mp4_temp_dir )
            dcm_zip_full_path = shutil.make_archive( dcm_zip_path[:-4], 'zip', dcm_temp_dir )
            zipped_data[mp4_zip_full_path] = { 'CONTENT': 'VIDEO', 'FORMAT': 'MP4', 'TAG': 'INTRA_OP' }
            zipped_data[dcm_zip_full_path] = { 'CONTENT': 'IMAGE', 'FORMAT': 'DICOM', 'TAG': 'POST_OP' }

        if verbose: print( f'\t...Zipped folder(s) of {num_dicom} dicom and {num_mp4} mp4 files successfully written to:\n\t\t{dcm_zip_full_path}\n\t\t{mp4_zip_full_path}' )
        return zipped_data, metatables
    

    def __str__( self ):
        select_cols = ['NEW_FN', 'IS_VALID', 'TYPE']
        df, intake_form = self.df[select_cols].copy(), self.intake_form
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{intake_form.uid}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{intake_form.datetime}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{None}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'


    def __del__( self ):
        try:
            for video in self.mp4:
                video.__del__()
        except Exception as e:
            pass


#--------------------------------------------------------------------------------------------------------------------------
## Class for derived data.
