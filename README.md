# XNAT-Interact

This repository contains scripts for getting your source and derived fluoroscopic image data to and from our XNAT RPACS server.

## First Things First:
- You will need to register with XNAT and then be added as a member to the RPACS XNAT server by the Data Librarian.
    - Make sure that you register a new account with your HawkID.
    - You must be using your VPN when you access the link included in the invitation email!

- NOTE: the SSL certificate for our accounts seems to require an annual renewal. It was most recently requested for renewal as of 2025-03-04.
---

## Installation

### **Option 1: Cloning the Repository (*For General Use*)**
*If you **only need to use the software and do not plan to contribute changes**, follow these steps*:

0. Open a command prompt/terminal.
1. Ensure that [git](https://git-scm.com/) is installed on your machine:
    ```bash
    git --version
    ```
   - If you get an error, install git (for Windows, go [here](https://gitforwindows.org/)).

2. Ensure that Python 3.8 (64-bit) is installed:
    ```bash
    python --version
    ```
   - If Python 3 is not installed, download [Python 3.8](https://www.python.org/downloads/release/python-380/).
   - If you have both Python 2 and Python 3 installed, use `python3` instead.

3. Navigate to your preferred directory:
    ```bash
    cd path_to_my_fav_local_folder_for_storing_repositories_of_code
    ```

4. Clone the repository:
    ```bash
    git clone https://github.com/domattioli/XNAT-Interact.git
    ```

5. Navigate into the cloned directory:
    ```bash
    cd XNAT-Interact
    ```

6. Create and activate a virtual environment:
    ```bash
    python -m venv .my_venv_for_xnat_interact
    ```
   **Activate the virtual environment:**
   - **Windows**:
     ```bash
     .my_venv_for_xnat_interact\Scripts\activate
     ```
   - **Unix (Mac/Linux)**:
     ```bash
     source .my_venv_for_xnat_interact/bin/activate
     ```

7. Install the required dependencies:
    ```bash
    pip install -r requirements.txt
    ```

8. Run a test to verify correct installation:
    ```bash
    python update_and_test.py
    ```

---

### **Option 2: Forking the Repository (*For Contributing Changes*)**
*If you want to **make changes to the code and push them to GitHub**, follow these steps instead*:

1. **Fork this repository**:
   - Go to [XNAT-Interact](https://github.com/domattioli/XNAT-Interact) on GitHub.
   - Click the **Fork** button (top right) to create your own copy of the repository.

2. **Clone your forked repository** (replace `YOUR-USERNAME` with your GitHub username):
    ```bash
    git clone https://github.com/YOUR-USERNAME/XNAT-Interact.git
    ```

3. **Navigate into the directory**:
    ```bash
    cd XNAT-Interact
    ```

4. **Set the upstream remote** (so you can sync with the original repo later):
    ```bash
    git remote add upstream https://github.com/domattioli/XNAT-Interact.git
    ```

5. **Create and activate a virtual environment (same as in Option 1)**.

6. **Create a new branch for your changes**:
    ```bash
    git checkout -b my-feature-branch
    ```

7. **Make your changes and commit them**:
    ```bash
    git add .
    git commit -m "Describe the changes you made"
    ```

8. **Push your changes to your fork**:
    ```bash
    git push origin my-feature-branch
    ```

9. **Create a pull request**:
   - Go to your fork on GitHub (`https://github.com/YOUR-USERNAME/XNAT-Interact`).
   - Click **"Compare & pull request"**.
   - Select:
     - **Base repository:** `domattioli/XNAT-Interact`
     - **Base branch:** `main`
     - **Head repository:** `YOUR-USERNAME/XNAT-Interact`
     - **Head branch:** `my-feature-branch`
   - Add a description and click **"Create pull request"**.
   - 
### **Option 2: Installing via `pip` (To-Do)**
🚧 **Note:** This installation method is not yet implemented. Future versions of this repository may support installation via `pip`. 🚧
        - This will require setting up the environment.yaml github workflow, I think.
Once available, you will be able to install `XNAT-Interact` directly using:
```bash
pip install XNAT-Interact
```
---


## **Example Usage**
### **UIowa VPN -- Required**
- If you are not running the following code from a university machine, you must be logged into the [UIowa Cisco VPN](https://its.uiowa.edu/support/article/1876) before running any commands.
- **Note:** You cannot go back once you begin one of the tasks (e.g., uploading a case). If you make a mistake, press **Ctrl + C** to exit and restart the task.

## **Best Practice: Run the following script at the start of each session**:
```bash
python update_and_test.py
```
