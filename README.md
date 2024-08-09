# XNAT-Interact
First things First:
- You will need to be added as a member to the RPACS XNAT server by the Data Librarian.

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
python --version
```
  - Note: "python" may not work correctly if you have python 2 and python 3 both installed on your machine. If that is the case, use "python3".
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
python -m venv .my_venv_for_xnat_interact
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
>python get-pip.py
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
python src/tests/test_install.py
```

===
### Option 2 via pip **todo**

```bash
pip install XNAT-Interact
```
===


---
## Example Usage
***If you are not running the following code from a university machine, you must make sure you are logged into the [UIowa Cisco VPN](https://its.uiowa.edu/support/article/1876) before trying to run these commands.

#### Best Practice: run the following script each time you begin a new session:
```bash
python update_and_test.py
```

#### All functionality requires running the following command in the terminal:
```bash
python main.py --username my_hawkid --password my_password
```
###### You may also run the following command if you prefer your password to be obscured.
```bash
python main.py
```
- Note that you must be added as a registered user by the librarian for the XNAT server.
- For more helpful feedback from the software as you perform a task, include the --verbose argument:
  
```bash
python main.py --verbose
```
```bash
python main.py --username my_hawkid --password my_password --verbose
```

###### Follow the prompts to (1) Upload new data (source images), (2) Upload derived data for existing source images, and/or (3) perform a query to download existing source and derived data.
