# XNAT-Interact
Some scripts for getting your source and derived fluoroscopic image data to and from your team's XNAT RPACS server.

## Installation
### Option 1 via git clone
1. Install python 3.7
    - Don't forget to add the python37.exe file to your environment PATH variable.
2. Open a command terminal and navigate to the folder where this library will live.
3. Run: 
```bash
git clone https://github.com/domattioli/XNAT-Interact.git
```
3. Navigate into the cloned directory:
```bash
cd XNAT-Interact
```
4. Create and then activate a virtual environment:
    - You'll need to install pip, too, by first downloading a get-pip.py file and then running it via python
```bash
python -m venv .venv-xnat-interact
```
```bash
.venv-xnat-interact\Scripts\activate # Windows only
```
```bash
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
pip --version
```
5. Install the requirements
```bash
pip install -r requirements.txt
```
6. Run a test command **todo**

### Option 2 via pip **todo**

```bash
pip install XNAT-Interact
```

## Example Usage
#### 1. Uploading a new case
- Given a new surgical performance with N dicom files representing the intraoperative fluoros:
```python
from XNAT-Interact import upload_new_case, download_queried_cases, upload_new_case

# Note:refer to the acceptable_inputs_catalog.md **todo**
success, info = upload_new_case( in_dir='/full/local/path/to/folder/containing/new_case/all/dicom/files',
                                procedure='Surgical_Procedure_Name', 
                                source='University_of_Iowa Hospitals_and_Clinics'
                                )
## returns printout of True/False success, and info
```
#### 2. Downloading a set of queried cases
- Download zipped folders containing the source/derived/all data for each surgical performance meeting a set of specifications.
```python
success, out_dir = download_queried_cases(  procedures=['all'], # can specify any of the cataloged groups or all
                                            sources=['all'], # can specify any of the cataloged sources of all
                                            dates=['YYYYMMDD','YYYYMMDD'] # if not all, then a list of length two indicating start and end date.
                                            ## **todo** more criteria in the future
                                        )
## returns printout of True/False success, and the parent directory of all saved zipped folders
```

#### 3. Uploading derived data for existing cases. **todo** additional derived data examples.
- Given a batch_results.csv file from Amazon's Mechanical turk
```python

# Note that the result file contains s3 urls but they 
success, out_dir = upload_derived_data( result_csv='/full/local/path/to/mturk_batch_result_file.csv',
                                        type='MTurk_Semantic_Segmentation',
                                        target_object='Humerus'
                                        )
## returns printout with success boolean, the parent directory of all saved zipped folders
```