import argparse
from pyxnat import Interface
from Utilities import MetaTables

def initialize_basic_metatable_items( mt: MetaTables ) -> MetaTables:
        mt.add_new_table( 'AcquisitioN_sites' )
        mt.add_new_item( 'acquisitIon_sites', 'UNIVERSITY_OF_IOWA_HOSPITALS_AND_CLINICS' )
        mt.add_new_item( 'acqUisition_sites', 'UNIVERSITY_OF_HOUSTON' )
        mt.add_new_item( 'ACQUISITION_SItes', 'AMAZON_MECHANICAL_TURK' )
        mt.add_new_table( 'gRouPs' )
        # trauma:
        mt.add_new_item( 'grOups', 'Open_reduction_hip_fracture–dynamic_hip_screw' )
        mt.add_new_item( 'grOups', 'Open_reduction_hip_fracture–cannulated_hip_screw' )
        mt.add_new_item( 'grOups', 'Closed_reduction_hip_fracture–cannulated_hip_screw' )
        mt.add_new_item( 'grOups', 'Percutaneous_sacroliac_fixation' )
        mt.add_new_item( 'groUps', 'PEDIATRIC_SUPRACONDYLaR_HUMERUS_FRACTURE_reduction_and_pinning' )
        mt.add_new_item( 'grOups', 'Open_and_percutaneous_pilon_fractures' )
        mt.add_new_item( 'grOups', 'Intramedullary_nail-CMN' )
        mt.add_new_item( 'grOups', 'Intramedullary_nail-Antegrade_femoral' )
        mt.add_new_item( 'grOups', 'Intramedullary_nail-Retrograde_femoral' )
        mt.add_new_item( 'grOups', 'Intramedullary_nail-Tibia' )
        mt.add_new_item( 'grOups', 'Scaphoid_Fracture' )
        mt.add_new_item( 'groups', 'Shoulder_ARTHROSCOPY' )
        mt.add_new_item( 'groups', 'KNEE_ARTHROSCOPY' )
        mt.add_new_item( 'groups', 'Hip_ARTHROSCOPY' )
        mt.add_new_item( 'groups', 'Ankle_ARTHROSCOPY' )
        mt.add_new_table( 'subjects', ['acquisition_site', 'group'] ) # need additional columns to reference uids from other tables
        mt.add_new_table( 'IMAGE_HASHES', ['subject', 'INSTANCE_NUM'] ) # need additional columns to reference uids from other tables
        mt.add_new_table( 'Surgeons', ['first_name', 'last_name', 'middle_initial'] )
        mt.add_new_item( 'surgeons', 'karamm', extra_columns_values={'first_name':'matthew', 'last_name': 'karam', 'middle_initial': 'd' } )
        mt.add_new_item( 'surgeons', 'kowalskih', extra_columns_values={'first_name':'heather', 'last_name': 'kowalski', 'middle_initial': 'r' } )
        return mt

# if __name__ == "__main__":
#     parser = argparse.ArgumentParser( description='Initializing metatables with default table-item value pairs that we expect in our data.' )
#     parser.add_argument( '--metatables_object', required=True, help='Object to store metatables in.')

#     args = parser.parse_args()

#     initialize_basic_metatable_items( args.mt )