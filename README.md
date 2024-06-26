# XNAT-Interact
Some scripts for getting your source and derived fluoroscopic image data to and from your team's XNAT RPACS server.


---
## Installation
### Option 1 via git clone
0. Open a command prompt/terminal.
1. Ensure that [git](https://git-scm.com/) installed on your machine.
```bash
git --version
```
- If you get an error, install git (for windows, go [here](https://gitforwindows.org/)).
  
2. Ensure that python 3.8 (64-bit) is installed on your machine.
```bash
python3 --version
```
- If you get an error, install [python3.8](https://www.python.org/downloads/release/python-380/).
    - Don't forget to add the python38.exe file to your environment PATH variable.
      
2. Navigate to the folder where this library will live.
```bash
cd path_to_my_fav_local_folder_for_storing_repositories_of_code
```

3. Copy the repository to a local directory: 
```bash
git clone https://github.com/domattioli/XNAT-Interact.git
```
3. Navigate into the cloned directory:
```bash
cd XNAT-Interact
```
4. Create and then activate a virtual environment:
```bash
python3 -m venv .my_venv_for_xnat_interact
```
>Activate virtual environment using _Windows_:
>```bash
>.my_venv_for_xnat_interact\Scripts\activate 
>```
>
>Activate virtual environment using _Unix_:
>```bash
>source .my_venv_for_xnat_interact/Scripts/Activate
>```

You'll need to install [pip](https://pypi.org/project/pip/), too, by first downloading a get-pip.py file and then running it via python
>Intall pip using _Windows_:
>```bash
>curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
>python3 get-pip.py
>pip --version
>```
>
>Intall pip using Unix:
> ```bash
>apt-get install python3-pip
> ```

5. Install the requirements, i.e., the python libraries necessary for running our code:
```bash
pip install -r requirements.txt
```

6. Run a test to verify correct installation:
```bash
python3 src/tests/test_install.py
```

7. Test out the functions by following the prompts.
```bash
python3 main.py
```


===
### Option 2 via pip **todo**

```bash
pip install XNAT-Interact
```
===


---
## Example Usage --- Note as of 5/20/2024 the following examples are outdated; going to shift to using just main.py and input prompts to achieve the functionality documented below.
#### 1. Uploading a new case
- Given a new surgical performance with N dicom files representing the intraoperative fluoros.

```python
from XNAT-Interact import upload_new_case, download_queried_cases, upload_new_case

# Note:refer to the acceptable_inputs_catalog.md **todo**
success, info = upload_new_case( in_dir='/full/local/path/to/folder/containing/new_case/all/dicom/files', 
                                group='Dyanmic_Hip_Screw',
                                acquisition_site='University_of_Iowa Hospitals_and_Clinics' 
                                )
## returns printout of True/False success, and info
```
>###### **Required** inputs:
>- 'in_dir', the directory just above where all the dicom files live on your local machine.
>- 'group', the surgical Procedure Name                                                     | see metatables.json for acceptable options.
>- 'acquisition_site, the source institution/entity from which the data originated          | see metatables.json for acceptable options.
>###### Optional inputs:
>- None currently supported.


#### 2. Downloading a set of queried cases
- Download zipped folders containing the source/derived/all data for each surgical performance meeting a set of specifications.
```python
success, out_dir = download_queried_cases(  data='source',                                            
                                            groups=['all'],                                           
                                            acquisition_sites=['all'],                                  
                                            dates=['YYYYMMDD','YYYYMMDD']                             
                                            ## **todo** more criteria in the future, e.g., femur_segmentation_available=True
                                        )
## returns printout of True/False success, and the parent directory of all saved zipped folders
```
>###### **Required** inputs:
>- None are required. This function is intended for specific queries.
>###### Optional inputs:
>- 'data', e.g., 'source' | 'derived' | 'all'
>- 'dates' e.g., 'all' | list of [start, end] dates in YYYYMMDD format
>- 'groups', e.g., 'all' | ['procedure_a', 'procedure_b'] | see metatables.json for acceptable options.
>- 'acquisition_sites', e.g, 'all' | ['University_of_iowa', 'Amazon_mechantical_turk'] | see metatables.json for acceptable options.

#### 3. Uploading derived data for existing cases. **todo** additional derived data examples.
- Given a batch_results.csv file from Amazon's Mechanical turk
```python

# Note that the result file contains s3 urls but they 
success, out_dir = upload_derived_data( result_csv='/full/local/path/to/mturk_batch_result_file.csv',
                                        acquisition_site='MTurk_Semantic_Segmentation',                         
                                        target_object='Humerus'                                      
                                        )
## returns printout with success boolean, the parent directory of all saved zipped folders
```
>###### **Required** inputs:
>- 'result_csv', the full file name of your files (to-do: currently only supporting mturk batch result .csv files)
>- 'acquisition_site', see example 1.
>- 'target_object', a specific bone/surgical tool; see metatables.json for acceptable options. (to-do: this won't work for things like IDEA score.
>###### Optional inputs:
>- None currently supported.

