# How to delete a Subject from the database
### This is only an instruction to base a future function off of. We should give some careful thought to a protocol for deleting something
    - Would need to delelete all relevant data (e.g., from the SUBJECTS and IMAGE_HASHES tables) in the database_config.json file.

#### Given some list of subjects as denoted by their dicom-derived UIDS
'''
from pyxnat import Interface, schema
image_subjects = [] # input list of subjects here

xnat_uiowa_username = '...' # input your username here
xnat_uiowa_password = '...' # input your password here
with Interface( server='https://rpacs.iibi.uiowa.edu/xnat', user=xnat_uiowa_username, password=xnat_uiowa_password ) as xnat:
    project = xnat.select.project('GROK_AHRQ_Data')
    for s in image_subjects:
        subject = project.subject( s )
        try:
            subject.delete()
            print( f"Deleted subject {s}" )
        except:
            print( f"Could not delete subject {s}" )
'''