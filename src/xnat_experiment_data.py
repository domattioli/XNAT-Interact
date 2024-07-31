import os
import glob
import requests
from typing import Optional as Opt, Tuple, Union

import numpy as np
import pandas as pd

from datetime import datetime

from pydicom.dataset import FileDataset as pydicomFileDataset
from pydicom import Dataset, Sequence, dcmread, dcmwrite

import pyxnat

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
    def __init__( self, intake_form: ORDataIntakeForm ):    
        assert os.path.isdir( intake_form.relevant_folder ), f"Inputted path must be a valid directory; inputted path: {intake_form.relevant_folder}"
        assert os.path.exists( intake_form.saved_ffn ), f"Inputted IntakeForm must be valid; no file found at: {intake_form.saved_ffn}"

        self._intake_form, self._tmp_source_data_dir = intake_form, intake_form.saved_ffn.parent / Path( 'SOURCE_DATA' ) # type: ignore
        if not os.path.exists( self.tmp_source_data_dir ):  os.makedirs( self.tmp_source_data_dir )
        self._df, self._is_valid, self._series_description = pd.DataFrame(), False, ''    # Derived in derived classes' init method

    @property
    def intake_form( self )             -> ORDataIntakeForm:        return self._intake_form
    @property
    def tmp_source_data_dir( self )     -> Path:                    return self._tmp_source_data_dir
    @property
    def df( self )                      -> pd.DataFrame:            return self._df
    @property
    def is_valid( self )                -> bool:                    return self._is_valid
    @property
    def series_description( self )      -> str:                     return self._series_description
    
    def _populate_df( self ):                                       raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    def _check_session_validity( self, metatables: MetaTables ):    raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )


    #--------------------------------------------XNAT-Publishing helpers and methods----------------------------------------------------------
    def _generate_queries( self, xnat: XNATConnection ) -> Tuple[str, str, str, str, str]:
        # Create query strings and select object in xnat then create the relevant objects
        exp_label = ( 'SOURCE_DATA' + '-' + self.intake_form.uid )
        scan_label = '0' #to-do: potentially an issue if there are multiple scans in a session
        # scan_type_label = scan_type_label
        # scan_type_series_description = 
        # resource_label = resource_label
        proj_qs = '/project/' + xnat.xnat_project_name
        proj_qs = PurePosixPath( proj_qs ) # to-doneed to revisit this because it is hard-coded but Path makes it annoying
        subj_qs = proj_qs / 'subject' / str( self.intake_form.uid )
        exp_qs = subj_qs / 'experiment' / exp_label
        scan_qs = exp_qs / 'scan' / scan_label
        files_qs = scan_qs / 'resource' / 'files'
        resource_label = 'DATA'
        return str( subj_qs ), str( exp_qs ), str( scan_qs ), str( files_qs ), resource_label


    def _select_objects( self, xnat: XNATConnection, subj_qs: str, exp_qs: str, scan_qs: str, files_qs: str ) -> Tuple[object, object, object]:
        subj_inst = xnat.server.select( str( subj_qs ) )
        assert not subj_inst.exists(), f'Subject already exists with the uri:\n{subj_inst}'         # type: ignore
        exp_inst = xnat.server.select( str( exp_qs ) )
        assert not exp_inst.exists(), f'Experiment already exists with the uri:\n{exp_inst}'        # type: ignore
        scan_inst = xnat.server.select( str( scan_qs ) )
        assert not scan_inst.exists(), f'Scan already exists with the uri:\n{scan_inst}'            # type: ignore
        return subj_inst, exp_inst, scan_inst


    def write( self, metatables: MetaTables, zip_dest: Opt[Path] = None, verbose: Opt[bool] = False )   -> Tuple[dict, MetaTables]: raise NotImplementedError( 'This is a placeholder method and must be implemented in an inherited class.' )
    

    def publish_to_xnat( self, xnat: XNATConnection, login: XNATLogin, zipped_data: dict, schema_prefix_str: str, scan_type_label: str, delete_zip: Opt[bool] = True, verbose: Opt[bool] = False ) -> None:
        assert scan_type_label in ['DICOM_MP4', 'DICOM'], f'Your inputted scan type label is not in the list of acceptable strings: ["DICOM_MP4", "DICOM"]'
        assert schema_prefix_str in ['rf', 'esv', 'otherDicom'], f'Your inputted schema prefix string is not in the list of acceptable strings:\n["rf", "esv", "otherDicom"]'
        if verbose:     print( f'\t...Pushing {schema_prefix_str} Session Data to XNAT...' )
        subj_qs, exp_qs, scan_qs, files_qs, resource_label = self._generate_queries( xnat=xnat )
        subj_inst, exp_inst, scan_inst = self._select_objects( xnat=xnat, subj_qs=subj_qs, exp_qs=exp_qs, scan_qs=scan_qs, files_qs=files_qs )

        # Create the items in stepwise fashion -- to-do: can't figure out how to create all in one go instead of attrs.mset(), it wouldn't work properly
        subj_inst.create()                                                                                  # type: ignore -- doesnt recognize .create() attribute of subj_inst
        subj_inst.attrs.mset( { f'xnat:subjectData/GROUP': self.intake_form.group } )                       # type: ignore -- doesnt recognize .attrs attribute of subj_inst
        exp_inst.create( **{    f'experiments': f'xnat:{schema_prefix_str}SessionData' })                   # type: ignore -- doesnt recognize .create() attribute of exp_inst
        exp_inst.attrs.mset( {  f'xnat:experimentData/ACQUISITION_SITE': self.intake_form.acquisition_site, # type: ignore -- doesnt recognize .attrs attribute of exp_inst
                                f'xnat:experimentData/DATE': self.intake_form.datetime.date                 
                            } )
        scan_inst.create( **{   f'scans': f'xnat:{schema_prefix_str}ScanData' } )                           # type: ignore -- doesnt recognize .create() attribute of scan_inst
        scan_inst.attrs.mset( { f'xnat:{schema_prefix_str}ScanData/TYPE': scan_type_label,                  # type: ignore -- doesnt recognize .attrs attribute of scan_inst
                                f'xnat:{schema_prefix_str}ScanData/SERIES_DESCRIPTION': self.series_description,
                                f'xnat:{schema_prefix_str}ScanData/QUALITY': self.intake_form.scan_quality,
                                f'xnat:imageScanData/NOTE': f'BY: {login.validated_username}; AT: {USCentralDateTime(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).verbose}'
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

        if delete_zip:
            [os.remove(key) for key in zipped_data.keys()] # to-do: not sure that this actually work as intended ie deletes the files corresponding to the key names
        if verbose:
            print( f'\t...{schema_prefix_str}Session succesfully pushed to XNAT!' )
            print( f'\t...Successfully deleted zip file:\n' + '\n'.join(f'\t\t{key}' for key in zipped_data.keys()) + '\n')


    def write_publish_catalog_subroutine( self, metatables: MetaTables, xnat: XNATConnection, login: XNATLogin, schema_prefix_str: str, scan_type_label: str, verbose: Opt[bool] = False, delete_zip: Opt[bool] = True ) -> MetaTables:
        try:
            zipped_data, metatables = self.write( metatables=metatables, verbose=verbose )
        except Exception as e:
            if verbose: print( f'\t!!! Failed to write zipped file; exiting without publishing to XNAT.\n\tError given:\n\t{e}' )
            raise
        try:
            self.publish_to_xnat( xnat=xnat, login=login, zipped_data=zipped_data, schema_prefix_str=schema_prefix_str, scan_type_label=scan_type_label, verbose=verbose, delete_zip=delete_zip )

        except Exception as e:
            print( f'\tError: failed to publish to xnat.\n\t{e}' )
            raise
        metatables.push_to_xnat( verbose=verbose )
        return metatables
    


#--------------------------------------------------------------------------------------------------------------------------
## Class for all radio fluoroscopic (source image) sessions.
class SourceRFSession( ExperimentData ): # to-do: Need to detail past and present identifiers for a subject.
    ''' '''
    # def __init__( self, dcm_dir: Path, intake_form: ORDataIntakeForm, login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables, past_upload_data: Opt[pd.DataFrame] = None ):
    #     super().__init__( intake_form=intake_form, login=login, xnat_connection=xnat_connection, metatables=metatables, past_upload_data=past_upload_data ) # Call the __init__ method of the base class
    #     self._validate_past_upload_data_input()
    #     self._populate_df()
    #     self._check_session_validity()
    #     if self.is_valid:
    #         self._mine_session_metadata() # necessary for publishing to xnat.
    #     # self._match_past_file_names( past_file_names ) # to-do: ? the Case_Database.csv file in the R-drive FLuoroscopy folder contains info about previous name information of these files. for now i am just going to tryst in my image hash protocol for preventing duplicates.
    #         self.update_intake_form()

    # @property
    # def _all_dicom_ffns( self ) -> list:
    #     assert os.path.isdir( self.pn ), f'First input must refer to a directory of dicom files detailing a surgical performance; you entered: "{self.pn}".'
    #     return glob.glob( os.path.join( self.pn, '**', '*' ), recursive=True )


    # def _init_rf_session_dataframe( self ):
    #     df_cols = { 'FN': 'str', 'EXT': 'str', 'NEW_FN': 'str', 'DICOM': 'object', 'IS_VALID': 'bool',
    #                 'DATE': 'str', 'SERIES_TIME': 'str', 'INSTANCE_TIME': 'str', 'INSTANCE_NUM': 'str' }
    #     self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )


    # def _populate_df( self ):
    #     self._init_rf_session_dataframe()
    #     all_ffns = self._all_dicom_ffns
    #     self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
    #     for idx, ffn in enumerate( all_ffns ):
    #         fn, ext = os.path.splitext( os.path.basename( ffn ) )
    #         if ext != '.dcm':
    #             self._df.loc[idx, ['FN', 'EXT', 'IS_VALID']] = [fn, ext, False]
    #             continue
    #         deid_dcm = SourceDicomDeIdentified( dcm_ffn=ffn, metatables=self.metatables )
    #         self._df.loc[idx, ['FN', 'EXT', 'DICOM', 'IS_VALID']] = [fn, ext, deid_dcm, deid_dcm.is_valid]
    #         if deid_dcm.is_valid:
    #             dt_data = self._query_dicom_series_time_info( deid_dcm )
    #             self._df.loc[idx, ['DATE', 'INSTANCE_TIME', 'SERIES_TIME', 'INSTANCE_NUM']] = dt_data

    #     # Need to check within-case for duplicates -- apparently those do exist.
    #     hash_strs = set()
    #     for idx, row in self.df.iterrows():
    #         if row['IS_VALID']:
    #             if row['DICOM'].image.hash_str in hash_strs:
    #                 self._df.at[idx, 'IS_VALID'] = False
    #             else:
    #                 hash_strs.add( row['DICOM'].image.hash_str )
                    

    # def _query_dicom_series_time_info( self, deid_dcm: SourceDicomDeIdentified ) -> list:
    #     dt_data = [deid_dcm.datetime.date, deid_dcm.datetime.time, None, deid_dcm.metadata.InstanceNumber]
    #     if 'SeriesTime' in deid_dcm.metadata:    dt_data[2] = deid_dcm.metadata.SeriesTime
    #     if 'ContentTime' in deid_dcm.metadata:   dt_data[2] = deid_dcm.metadata.ContentTime
    #     if 'StudyTime' in deid_dcm.metadata:     dt_data[2] = deid_dcm.metadata.StudyTime
    #     return dt_data


    # def _mine_session_metadata( self ):
    #     assert self.df.empty is False, 'Dataframe of dicom files is empty.'
    #     self._derive_experiment_datetime()
    #     self._derive_experiment_uid()
        
    #     # For each row, generate a new file name now that we have a session label.
    #     for idx, row in self.df.iterrows():
    #         if row['IS_VALID']:
    #             self._df.at[idx, 'NEW_FN'] = row['DICOM'].generate_source_image_file_name( str( row['INSTANCE_NUM'] ), self.uid )

    #     # self._derive_acquisition_site_info() # to-do: should warn the user that any mined info is inconsistent with their input
    #     self._df = self.df.sort_values( by='NEW_FN', inplace=False )

    # # def _derive_acquisition_site_info( self ): # Retrieve source institution info across all dicom files and tell user if their input is inconsistent, if given
    # #     potential_sources = set( source for sublist in self.df[self.df['IS_VALID']]['DICOM'].apply( lambda x: x.acquisition_site ) for source in sublist )
    # #     if len( potential_sources ) > 0:
    # #         pass # self._validate_derived_acquisition_sites( potential_sources ) # need to evaluate the derived info against the user input
    # #     self._acquisition_site = self.acquisition_site.upper()

    # def _validate_past_upload_data_input( self, past_upload_data: Opt[pd.DataFrame] = None ):
    #     if self._past_upload_data is None:          self._past_upload_data = pd.DataFrame() # to-do: this should be a dataframe with a specific structure
        

    # def _check_session_validity( self ): # Invalid only when empty or all shots are invalid -- to-do: may also want to check that instance num and time are monotonically increasing
    #     # valid_rows = self.df[ self.df['IS_VALID'] ].copy()
    #     # assert self.label, 'Cannot check duplicate without a rfsession label.'
    #     self._is_valid = self.df['IS_VALID'].any() and not self.metatables.item_exists( table_name='SUBJECTS', item_name=self.uid )
        

    # def _derive_experiment_datetime( self ):
    #     assert self.df['DATE'].nunique() == 1, f'Dicom metadata produced either all-nans or different dates across the files for this performance -- should only be one:\n{self._df["DATE"]}'
    #     assert self.df['SERIES_TIME'].nunique() == 1, f'Dicom metadata produced different SERIES_TIME values across the files for this performance -- should only be one:\n{self._df["SERIES_TIME"]}'
    #     self._datetime = USCentralDateTime( self.df.at[0,'DATE'] + ' ' + self.df.at[0, 'SERIES_TIME'] )
    

    # def _derive_experiment_uid( self ):
    #     '''Original dicom data should have the same Series Instance UID for all dicom files. The Instance number is the file name.'''
    #     series_instance_uids = []
    #     for _, row in self.df.iterrows():
    #         if row['IS_VALID']:
    #             series_instance_uids.append( row['DICOM'].uid_info['Series Instance UID'] )
    #     series_instance_uids = pd.Series( series_instance_uids )
    #     if series_instance_uids.nunique() == 1: # Use the uid that was found within the metadata
    #         self._uid = series_instance_uids.at[0]
    #     else:                                   # Either multiple uid's were found in the metadata, or none were found. In either case, generate a new one to guarantee uniqueness.
    #         self._assign_experiment_uid()
    #         self._deal_with_inconsistent_series_instance_uid()
    

    # def _deal_with_inconsistent_series_instance_uid( self ): # overwrite inconsisten series instance uid information in the metadata.
    #     for idx, row in self.df.iterrows():
    #         if row['IS_VALID']: # Copy the value for 'SeriesInstanceUID' to a new private tag; add new private tags detailing this change
    #             description = "Original (but inconsistent) SeriesInstanceUID on upload to XNAT"
    #             self._df.at[idx,'DICOM'].metadata.add_new( ( 0x0019, 0x1001 ), 'LO', description )
    #             self._df.at[idx,'DICOM'].metadata.add_new( ( 0x0019, 0x1002 ), 'LO', row['DICOM'].metadata.SeriesInstanceUID )
    #             self._df.at[idx,'DICOM'].metadata.add_new( ( 0x0019, 0x1003 ), 'LO', ['Added by: ' + self.login.validated_username] )
    #             self._df.at[idx,'DICOM'].metadata.add_new( ( 0x0019, 0x1004 ), 'DA', datetime.today().strftime( '%Y%m%d' ) )
    #             self._df.at[idx,'DICOM'].metadata.SeriesInstanceUID = self.uid


    # def __str__( self ) -> str:
    #     select_cols = ['FN','NEW_FN', 'IS_VALID', 'INSTANCE_TIME']
    #     df = self.df[select_cols].copy()
    #     if self.is_valid:
    #         return f' -- {self.__class__.__name__} --\nUID:\t{self.uid}\nAcquisition Site:\t{self.acquisition_site}\nGroup:\t\t\t{self.group}\nDate-Time:\t\t{self.datetime}\nValid:\t{self.is_valid}\n{df.head()}...{df.tail()}'
    #     else:
    #         return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{self.acquisition_site}\nGroup:\t\t\t{self.group}\nDate-Time:\t\t{None}\nValid:\t{self.is_valid}\n{df.head()}...{df.tail()}'


    # def write( self, zip_dest: Opt[Path] = None, verbose: Opt[bool] = False ) -> Tuple[dict, MetaTables]::
    #     assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."
    #     if zip_dest is None: zip_dest = self.login.tmp_data_dir
    #     assert os.path.isdir( zip_dest ), f'Destination for zipped folder must be an existing directory; you entered: {zip_dest}'
        
    #     # Method also adds the subject and image info to the metatables at the same time so we can ensure no duplicates reach 'publish_to_xnat()'
    #     write_d = os.path.join( zip_dest, self.uid )
    #     subject_info = { 'ACQUISITION_SITE': self.metatables.get_uid( table_name='ACQUISITION_SITES', item_name=self.acquisition_site ),
    #                     'GROUP': self.metatables.get_uid( table_name='GROUPS', item_name=self.group ) }
    #     self.metatables.add_new_item( table_name='SUBJECTS', item_name=self.uid, extra_columns_values=subject_info, verbose=verbose ) # type: ignore
    #     with tempfile.TemporaryDirectory() as tmp_dir:
    #         for _, row in self.df.iterrows():
    #             if row['IS_VALID']:
    #                 dcmwrite( os.path.join( tmp_dir, row['NEW_FN'] ), row['DICOM'].metadata )
    #                 img_info = { 'SUBJECT': self.metatables.get_uid( table_name='SUBJECTS', item_name=self.uid ), 'INSTANCE_NUM': row['NEW_FN'] }
    #                 self.metatables.add_new_item( table_name='IMAGE_HASHES', item_name=row['DICOM'].image.hash_str, extra_columns_values=img_info, verbose=verbose ) # type: ignore
    #         shutil.make_archive( write_d, 'zip', tmp_dir )
        
    #     if verbose is True:
    #         num_valid = self.df['IS_VALID'].sum()
    #         print( f'\t...Zipped folder of ({num_valid}/{len( self.df )}) dicom files successfully written to: {write_d}.zip' )
    #     return Path( write_d + '.zip' )



#--------------------------------------------------------------------------------------------------------------------------
## Class for arthroscopy post-op diagnostic images.
class SourceESVSession( ExperimentData ):
    '''Class representing the XNAT Experiment for Endoscopy Videos. Inherits from ExperimentData.'''
    def __init__( self, intake_form: ORDataIntakeForm, login: XNATLogin, xnat_connection: XNATConnection, metatables: MetaTables ):        
        super().__init__( intake_form=intake_form ) # Call the __init__ method of the base class
        self._populate_df( metatables=metatables )
        self._check_session_validity( metatables=metatables )
        if self.is_valid:
            self._mine_session_metadata() # necessary for publishing to xnat.


    @property
    def mp4( self )     -> ArthroVideo:     return self._mp4


    def _mine_session_metadata( self ):
        assert self.df.empty is False, 'Dataframe of dicom files is empty.'
        # self._derive_experiment_datetime() # should be inputted for now because I'm not sure how to derive this from images and mp4s unless we use directory names, which aren't reliable
        # self._assign_experiment_uid()
        
        # For each row, generate a new file name now that we have a session label.
        for idx in range( len( self.df ) ): # to-do: BUG - for some reason the iterrows() enumerator creates a row variable that cannot be accessed -- error occurs because the str method is overridden somewhere...
            if self.df.loc[idx, 'IS_VALID']:
                file_obj_rep = self.df.loc[idx, 'OBJECT']
                if isinstance( file_obj_rep, ArthroVideo ): # video is assigned instance number 000. Our convention is to begin at 001 for image files.
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( '000', file_obj_rep.uid_info['Video_UID'] ) 
                    # self._df.loc[idx, 'NEW_FN'] = self.df.loc[idx, 'OBJECT'].uid_info['Video_UID']
                elif isinstance( self.df.loc[idx, 'OBJECT'], ArthroDiagnosticImage ):
                    self._df.loc[idx, 'NEW_FN'] = file_obj_rep.generate_source_image_file_name( file_obj_rep.still_num, file_obj_rep.uid_info['Still_UID'] ) # type: ignore
                else:
                    raise ValueError( f"Unrecognized object type: {type( self.df.loc[idx, 'OBJECT'] )}." )


    def _init_esv_session_dataframe( self ):
        df_cols = { 'FN': 'str', 'NEW_FN': 'str', 'OBJECT': 'object', 'TYPE': 'str', 'IS_VALID': 'bool'}
        self._df = pd.DataFrame( {col: pd.Series( dtype=dt ) for col, dt in df_cols.items()} )


    def _populate_df( self, metatables: MetaTables ):
        # Read in mp4 data
        mp4_ffn = list( self.intake_form.relevant_folder.rglob("*.[mM][pP]4") )
        assert len( mp4_ffn ) == 1, f"There should be exactly one mp4 file in the directory; found --{len( mp4_ffn )}-- mp4 files."
        self._mp4 = ArthroVideo( vid_ffn=mp4_ffn[0], metatables=metatables, intake_form=self.intake_form )
        assert self.mp4.is_valid, f"Could not open video file: {self.mp4.ffn}"

        # Read all jpg images;  sort images by their creationg date-time, append mp4 ffn to the list before we build the dataframe
        all_ffns = list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][gG]") ) + list( self.intake_form.relevant_folder.rglob("*.[jJ][pP][eE][gG]") )
        assert len( all_ffns ) > 0, f"No image files found in the inputted folder; make sure that all image files in folder have the correct ('.jpg' or '.jpeg') extension.\n\tDirectory given:  {self.intake_form.relevant_folder}."
        all_ffns = sorted( all_ffns, key=lambda x: os.path.getctime( x ) ) + [mp4_ffn[0]]
        
        # Assemble into a dataframe
        self._init_esv_session_dataframe()
        self._df = self._df.reindex( np.arange( len( all_ffns ) ) )
        for idx, ffn in enumerate( all_ffns ):
            fn, ext = os.path.splitext( os.path.basename( ffn ) )
            if ext.lower() == '.mp4':
                self._df.loc[idx, ['FN', 'OBJECT', 'IS_VALID', 'TYPE']] = [fn, self.mp4, self.mp4.is_valid, 'MP4']
            elif ext.lower() in ['.jpg', '.jpeg']:
                instance_num = str( idx+1 )
                diag_img_obj = ArthroDiagnosticImage( img_ffn=ffn, still_num=instance_num, parent_uid=self.mp4.uid_info['Video_UID'], metatables=metatables, intake_form=self.intake_form ) 
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
        ''' Method adds the subject and image info to the metatables at the same time so we can ensure no duplicates reach 'publish_to_xnat() '''
        assert self.is_valid, f"Session is invalid; could be for several reasons. try evaluating whether all of the image hash_strings already exist in the matatable."

        # Add the subject to the metatables
        metatables.add_new_item( table_name='SUBJECTS', item_name=self.intake_form.uid, verbose=verbose,
                                extra_columns_values={ 'ACQUISITION_SITE': metatables.get_uid( table_name='ACQUISITION_SITES', item_name=self.intake_form.acquisition_site ),
                                                      'GROUP': metatables.get_uid( table_name='GROUPS', item_name=self.intake_form.group ) }
                                )

        # Zip the mp4 and dicom data to separate folders
        zipped_data, home_dir = {}, self.tmp_source_data_dir
        with tempfile.TemporaryDirectory( dir=home_dir ) as mp4_temp_dir, \
            tempfile.TemporaryDirectory( dir=home_dir ) as dcm_temp_dir:
            for idx in range( len( self.df ) ): # Iterate through each row in the DataFrame, writing each to a temp directory before we zip it up and delete the unzipped folder.
                if self.df.loc[idx, 'IS_VALID']:
                    file_obj_rep = self.df.loc[idx, 'OBJECT']
                    if isinstance( file_obj_rep, ArthroDiagnosticImage ):
                        dcmwrite( os.path.join( dcm_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) ), file_obj_rep.metadata )          # type: ignore
                        tmp = { 'SUBJECT': metatables.get_uid( table_name='SUBJECTS', item_name=self.intake_form.uid ), 'INSTANCE_NUM': self.df.loc[idx, 'NEW_FN'] }
                        metatables.add_new_item( table_name='IMAGE_HASHES', item_name=file_obj_rep.image.hash_str, verbose=verbose, # type: ignore
                                                extra_columns_values = tmp
                                                )
                
                    elif isinstance( file_obj_rep, ArthroVideo ): # Don't need to add this to metatables because the diagnostic images (frames from the video) should suffice.
                        vid_ffn = os.path.join( self.intake_form.relevant_folder, str( self.df.loc[idx,'FN'] ) + '.mp4' )
                        shutil.copy( vid_ffn, os.path.join( mp4_temp_dir, str( self.df.loc[idx, 'NEW_FN'] ) + '.mp4' ) )

                    else:
                        raise ValueError( f"Object representation of file at index {idx} is {type( file_obj_rep )}, which is neither an ArthroDiagnosticImage nor an ArthroVideo object." )
                
            # Zip these temporary directories into a slightly-less-temporary directory.
            mp4_zip_path = os.path.join( home_dir, "mp4_file.zip" )
            dcm_zip_path = os.path.join( home_dir, "dicom_files.zip" )
            mp4_zip_full_path = shutil.make_archive( mp4_zip_path[:-4], 'zip', mp4_temp_dir )
            dcm_zip_full_path = shutil.make_archive( dcm_zip_path[:-4], 'zip', dcm_temp_dir )
            zipped_data[mp4_zip_full_path] = { 'CONTENT': 'VIDEO', 'FORMAT': 'MP4', 'TAG': 'INTRA_OP' }
            zipped_data[dcm_zip_full_path] = { 'CONTENT': 'IMAGE', 'FORMAT': 'DICOM', 'TAG': 'POST_OP' }

        if verbose is True:
            num_valid = self.df['IS_VALID'].sum() 
            print( f'\t...Zipped folder of ({num_valid}/{len( self.df )}) dicom and mp4 files successfully written to:\n\t\t{dcm_zip_full_path}\n\t\t{mp4_zip_full_path}' )
        return zipped_data, metatables
    

    def __str__( self ):
        select_cols = ['NEW_FN', 'IS_VALID', 'TYPE']
        df, intake_form = self.df[select_cols].copy(), self.intake_form
        if self.is_valid:
            return f' -- {self.__class__.__name__} --\nUID:\t{intake_form.uid}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{intake_form.datetime}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'
        else:
            return f' -- {self.__class__.__name__} --\nUID:\t{None}\nAcquisition Site:\t{intake_form.acquisition_site}\nGroup:\t\t\t{intake_form.group}\nDate-Time:\t\t{None}\nValid:\t\t\t{self.is_valid}\n{df.head()}\n...\n{df.tail()}'


    def __del__( self ):    
        try:    self.mp4.__del__()
        except: pass
