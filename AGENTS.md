# AGENTS.md

## Project

XNAT-Interact de-identifies surgical fluoroscopic images and uploads or downloads
them from the University of Iowa RPACS XNAT server. `main.py` is the interactive
command-line interface. The repository also contains Streamlit interfaces,
installer code, and backend data-processing services.

Python 3.9 or newer is required. This repository is not a pip-installable
package.

## Setup and commands

Create an environment and install runtime and test dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt -r requirements-dev.txt
```

Run the offline test suite:

```bash
pytest
```

Run an interface from the repository root:

```bash
python main.py
streamlit run streamlit_app.py
streamlit run streamlit_guided.py
bash scripts/run_demo.sh --fake
```

The optional pixel de-identification lane has separate dependencies in
`requirements-pixeldeid.txt`. The canonical CI test lane is
`.github/workflows/ci-lite.yml`. It runs on Python 3.11.

## Layout

- `app/` contains the Streamlit application and guided interface.
- `src/` contains the command-line workflow, XNAT models, annotations, and
  backend services.
- `tests/` contains the offline suite, `FakeXNAT`, and synthetic fixtures.
- `tests/integration/xnat_local/` contains the local XNAT Docker environment.
- `installer/` contains launcher and platform build support.
- `data/device_profiles/` contains pixel de-identification device profiles.
- `docs/DATA_MODEL.md`, `docs/METADATA.md`, and `docs/XNAT_MODEL.md` define the
  data, identity, metadata, and XNAT contracts.
- `docs/OPS_CHECKLIST.md` records operator-only packaging and deployment work.
- Finished numbered implementation phases live in DomI
  `specs/consumers/XNAT-Interact/specs/`. New phases use a local `specs/` during
  the build and move to DomI at the end. Follow each phase's `spec.md`, `plan.md`, and `tasks.md` in that order. Complete tasks
  from top to bottom; `[P]` marks tasks that may run in parallel.

## Project rules

- Never put protected health information (PHI) in this repository, tests,
  logs, telemetry, or error messages. Use only generators from
  `tests/synthetic_data.py` for patient-like test data.
- The default test suite must run offline without an XNAT server or virtual
  private network (VPN). Keep server access behind the gateway seam and use
  `tests/fakes/fake_xnat.py` in tests.
- Never pass credentials in command arguments or store them in configuration.
  Prompt for credentials at runtime. Do not log them.
- Configure non-secret connection values with `XNAT_SERVER_URL` and
  `XNAT_PROJECT_NAME`. Do not hardcode a server URL or project name.
- Fail softly for foreseeable errors. Do not show users a raw traceback. Give
  them a useful next step.
- Preserve the identity, de-identification, duplicate-detection, quarantine,
  and metadata contracts in `docs/DATA_MODEL.md` and `docs/METADATA.md`.
- Use plain prose for the README, onboarding pages, and other user-facing
  documentation. Keep requirement lists, success criteria, task identifiers,
  code, and commit messages exact.

## Branches and labels

- The working branch is `development`. Releases go through a pull request from
  `development` to `main`.
- Do not push directly to `main` and do not force-push shared branches.
- `.github/labels.yml` defines this repository's issue label taxonomy.

## Governance

This repository is a downstream consumer of `domattioli/DomI`.
Universal git, coding dispatch, secrets, session lifecycle, and communication rules live in DomI `.claude/policies/`.
Spec Kit governance artifacts for this repository live in DomI `specs/consumers/XNAT-Interact/`; never create a local `.specify/` directory.
