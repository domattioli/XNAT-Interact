# XNAT-Interact

A tool for de-identifying and uploading/downloading surgical fluoroscopic images to the University of Iowa RPACS XNAT server.

---

## Before You Start

You need three things:

1. **An XNAT account** — contact the Data Librarian (your lab's designated person) to be added to the project. Register with your HawkID.
2. **UIowa VPN** — you must be connected to the [UIowa Cisco VPN](https://its.uiowa.edu/support/article/1876) whenever you run this tool off-campus.
3. **Python 3.8+** — a 64-bit Python 3 installation.

> **SSL certificate note:** The XNAT server certificate requires annual renewal. If you see an SSL error, contact the Data Librarian — you cannot connect until it is renewed.

---

## Quick Start

Open a terminal and run these steps once to set up:

```bash
# 1. Clone the repository
git clone https://github.com/domattioli/XNAT-Interact.git
cd XNAT-Interact

# 2. Create and activate a virtual environment
python -m venv .venv

# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Verify your installation and pull the latest code
python update_and_test.py
```

Then open `main.py` and follow the prompts. When asked for a password, type it at the prompt — never put credentials in a file.

The server address and project name are configured automatically. If you need to point to a different server or project, set the environment variables `XNAT_SERVER_URL` and `XNAT_PROJECT_NAME` before running.

---

## Staying Up to Date

Run this at the start of each session to pull the latest code and check your environment:

```bash
python update_and_test.py
```

---

## Contributing Changes

1. Fork the repo on GitHub ([XNAT-Interact](https://github.com/domattioli/XNAT-Interact)).
2. Clone your fork and create a branch: `git checkout -b my-feature`.
3. Make changes, commit, push to your fork, then open a pull request targeting `main`.

---

## Getting Help

Contact your Data Librarian or open an issue on GitHub. For SSL certificate problems, email IIBI staff directly.

> **Note:** A packaged `pip install` version is planned for a future release to remove the manual install steps above.
