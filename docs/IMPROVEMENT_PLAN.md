# XNAT-Interact — Improvement Plan

*Audience: the maintainer (you) and any future contributor. Written to be read
without deep coding experience. Last updated: 2026-06-04.*

## The mission, in one sentence

Make it **easy and low-risk** for student researchers to get their surgical
images into (and out of) the XNAT RPACS server — so they actually use the tool,
stay organized, and don't get scared off by the terminal or by confusing
crashes.

Two concrete goals drive every recommendation below:

1. **Lower the skill floor.** Today you must be comfortable with a terminal,
   `git`, Python virtual environments, and typing file paths by hand. That's a
   lot to ask of a rotating cast of students.
2. **Fail softly, with a human in the loop.** Today the program often crashes
   with a raw Python traceback, or silently swallows the real error, and the
   student is stuck with no next step. We want every failure to say *what went
   wrong* and *what to do about it* — retry, skip, save progress, or contact the
   Data Librarian.

---

## How the tool works today (plain-language map)

```
  Student's computer (on UIowa VPN)                 UIowa network
  ┌─────────────────────────────────────┐          ┌──────────────────────┐
  │  XNAT-Interact (Python, terminal)    │          │  XNAT RPACS server   │
  │                                      │  HTTPS   │  rpacs.iibi.uiowa.edu│
  │  • asks questions in the terminal    │ ───────► │  (images + metadata) │
  │  • reads DICOM / JPG / MP4 files     │          │                      │
  │  • DE-IDENTIFIES them locally        │          └──────────────────────┘
  │  • uploads, or downloads back        │
  └─────────────────────────────────────┘
```

- **Entry point:** `main.py` → a long series of terminal prompts ("enter 1 for
  yes, 2 for no").
- **The brains:** `src/` (~4,000 lines). Highlights:
  - `utilities.py` — login, server connection, a JSON "database" of
    surgeons/sites/procedures (`ConfigTables`), the image-hash de-duplicator.
  - `xnat_resource_data.py` — the 34-question **intake form** (`ORDataIntakeForm`).
  - `xnat_scan_data.py` — reading and **de-identifying** DICOM/JPG/MP4.
  - `xnat_experiment_data.py` — bundling a case and publishing it.
  - `batch_upload.py` — uploading many cases from a spreadsheet.

**The single most important constraint:** the server is **behind the UIowa VPN**
and the data is **PHI (de-identified on the student's machine before it ever
leaves)**. This rules out a public, hosted "upload website" — the data and the
VPN both live on the student's computer. The right shape is therefore a
**local app that runs on their machine** but *looks and feels like a website*.
(A small *static* info/onboarding site is still worth doing — see Phase 4.)

---

## What's holding it back (evidence, with file references)

These aren't guesses — each points at a real line of code.

### A. Getting started is a cliff
- The README is ~140 lines of terminal steps before a single image moves:
  install `git`, install Python 3.8, make a virtual environment, `pip install`,
  run a script. Any one of these can stop a non-technical student cold.
- The whole interface is sequential `input()` prompts and **typing full folder
  paths by hand** (`main.py:154`, `main.py:249`, etc.). There are no dropdowns,
  no file pickers — even though the lists of surgeons/sites/procedures *already
  exist* in `ConfigTables` and would make perfect dropdowns.

### B. It fails loudly, or silently — rarely helpfully
- **6 bare `except:` blocks** hide the real error. The worst is
  `utilities.py:497`: *any* failure during startup (network down, bad file,
  permissions) is treated as "first-time database setup," masking the true
  cause.
- **~60 `assert` statements used as error handling.** When one trips, the
  student sees a raw traceback, not "your VPN looks disconnected."
- The top-level handler (`main.py:466`) just prints the error and quits — no
  retry, no "save my progress," no guidance.
- **Batch upload aborts the entire batch if any single row is bad**
  (`batch_upload.py:530`). One typo in a 50-row spreadsheet = start over.
- **`delete_contents_of_server.py` wipes the entire XNAT project with no
  confirmation** and silently swallows failures (`:15`, `:23`). This is a
  serious footgun for whoever runs it.
- **`is_dicom()` treats any extensionless file as a DICOM** (`xnat_scan_data.py:67`)
  — a stray `notes` file could be mistaken for imaging data.
- **"Tests" aren't tests.** `update_and_test.py` does `git reset --hard
  origin/master` (and the default branch may be `main`, not `master` — likely
  broken) plus an import check. Its own README claims it "runs the test suite";
  it does not. CI runs `pytest` against **zero tests**, so it passes vacuously.

### C. Smaller issues worth noting
- Hardcoded server URL and project name (`utilities.py:85-86`, `main.py:264+`)
  — switching between a test and production server means editing code.
- `environment.yml` pins Python 3.8 while CI uses 3.10 — drift.
- A `testing 123.txt` file and a large sandbox notebook are committed in `src/`.

---

## Your install question, answered

> *"Do the instructions imply they need IT setup before installing XNAT-Interact
> itself? They're on a school IT-managed machine…"*

**Yes — there is an implied IT/setup prerequisite today.** The README requires,
before XNAT-Interact is even downloaded:

1. **`git` installed** (to clone the repo).
2. **Python 3.8, 64-bit, installed** and on the PATH.
3. Rights to create a virtual environment and `pip install` packages.
4. UIowa VPN access + an XNAT account (granted by the Data Librarian).

On a **school IT-managed machine**, items 1–3 are the catch:

- Many managed machines **don't let users install software** (no admin rights),
  so `git`/Python may need to be provisioned by IT, or come from a software
  portal (e.g. UIowa "Software Center"/self-service).
- A **one-click installer is technically possible** — tools like *PyInstaller*
  bundle Python *inside* the app, so the student wouldn't need to install Python
  or git separately. **But** managed machines often block running unsigned
  downloaded `.exe` files (SmartScreen / execution policy), which can defeat the
  point.

**Maintainer's decision (2026-06-04): we cannot assume Python is installed on
the student's machine.** That rules out a "guided setup script that needs Python
already there." Packaging (Phase 3) must therefore **bundle the Python runtime**
so a student with *nothing* installed can still run the app — e.g. a PyInstaller
one-file build, or an installer that ships its own Python.

**Still to confirm with UIowa IT** (one open question, not a blocker for Phases
0–2): can students **run a downloaded, unsigned executable**, or must apps be
**code-signed / distributed through the campus software portal**? The answer
decides only the *delivery mechanism* in Phase 3 (self-served `.exe` vs. signed
build vs. software-portal package) — not whether we bundle Python (we will).

The plan below is structured so we **don't bet the project on that remaining
answer**: the GUI (Phase 2) works whichever delivery mechanism we land on.

---

## The plan (phased, lowest-risk first)

> **This round delivers Phase 0 only** (the plan you're reading + a real test
> harness with synthetic data). Everything else is designed and sequenced but
> not yet built, per your "plan only with tests first" decision.

### ✅ Phase 0 — Make the code testable (DONE in this PR)
*Goal: be able to change things safely. Nothing in the app's behavior changes.*

- Added a **`tests/` suite that runs fully offline** — no server, no VPN, no
  real PHI. See `tests/README.md`.
- Added **synthetic data generators** (`tests/synthetic_data.py`): fake DICOM
  (with fake PHI), JPG, MP4, intake form, and batch spreadsheet.
- Pinned the highest-value, safety-critical behavior first:
  - **DICOM de-identification** (patient name, physician, accession #, study ID,
    private tags, overlays, curves all scrubbed; pixel data preserved).
  - The **duplicate-image hash**, **date/time normalization**, the reused
    **menu-prompt** logic, **batch string validators**, and **file-type
    detection**.
- Marked current footguns with a `known_issue` marker so they're easy to find
  and flip as we fix them (`pytest -m known_issue`).
- Wired nothing new into CI is required — the existing `pytest` CI step now runs
  real tests instead of zero.

**Why first:** every later change (especially refactoring error handling and
de-identification) is safe only if we can prove we didn't break the scrubbing of
patient data. Now we can.

### Phase 1 — Reliability & "human-in-the-loop" failures *(no GUI yet)*
*Goal: the existing tool stops crashing unhelpfully. Pure backend wins that the
future GUI will sit on top of.*

1. **One error helper, used everywhere.** A small module that, on any failure,
   writes a log file and shows a plain-language message with **recourse**:
   *retry / skip this item / save progress / copy this error to send the Data
   Librarian.* Replace the 6 bare `except:` blocks so the real error is
   preserved, not hidden.
2. **Preflight checks before long operations:** Is the VPN/server reachable? Are
   the credentials valid? Does the folder exist? Fail *early* with a clear
   message instead of halfway through an upload.
3. **Batch upload: continue-on-error.** Process every row; collect failures;
   show an end-of-run summary (succeeded / skipped / failed + why); let the
   student **re-run only the failed rows**. Stop aborting the whole batch over
   one cell (`batch_upload.py:530`).
4. **Guard the destructive script.** `delete_contents_of_server.py` must require
   an explicit typed confirmation (and ideally a `--dry-run`), and must stop
   swallowing deletion failures. (Run only by the maintainer / Data Librarian,
   per the maintainer — so the goal is a clear "are you sure?" + a dry-run, not
   heavy multi-user lockdown.)
5. **Replace asserts on the user path** with friendly `raise`/messages so
   students never see a raw traceback for a foreseeable problem (wrong password,
   no VPN, bad path).
6. **Fix `is_dicom`** to sniff the file's bytes (the `DICM` marker) instead of
   trusting the file extension.
7. **Externalize the server URL / project name** into a tiny config file so
   test vs. production needs no code edit.

*Each item ships with tests using the Phase 0 harness.*

### Phase 2 — The Streamlit app: a "human-factors view of XNAT"
*Goal: students see and manage their data in a browser-like UI; the terminal
becomes optional.*

This is the big skill-reduction win, and matches your vision: a friendly local
app where students can **see the data**, with an **optional embedded terminal**
for those who want to learn the command-line way.

- **Runs locally** (on the student's VPN-connected machine), opens in their
  browser. PHI never goes anywhere except your XNAT server.
- **Reuses the existing `src/` code underneath** — the GUI is a thin, friendly
  front-end over the same upload/download/de-identify logic. (Phase 1 makes that
  logic GUI-friendly by separating it from `input()`/`print()`.)
- **Screens:**
  - **Login** (with a clear "are you on the VPN?" preflight and a real error
    panel, not a crash).
  - **Browse / see your data** — a searchable, filterable table of what's on the
    server (you already build this kind of table in `print_preview_of_xnat_data`),
    plus image/thumbnail preview. This is the "human-factors view of XNAT" —
    students *see* their cases instead of imagining them.
  - **Upload (single)** — the 34-question intake form rendered as a real web
    form: **dropdowns** for surgeon/site/procedure (pulled from `ConfigTables`),
    **date pickers**, a **file/folder picker** or drag-and-drop, a **preview**
    of what will be de-identified and uploaded, and a **progress bar**.
  - **Upload (batch)** — drag in the spreadsheet, see a per-row validation
    table *before* uploading, fix issues inline, upload with a live progress and
    a clear summary.
  - **Download** — pick cases from the table, choose a folder, watch progress.
  - **Optional "Terminal / Learn mode"** — an embedded terminal panel (e.g.
    `streamlit` component) so a curious student can run the equivalent CLI
    command and learn it, entirely by choice. Default users never need it.
- **Every error is a friendly panel** with the same recourse options from Phase 1
  (retry / skip / save / contact librarian), never a stack trace.

*Recommended tech: **Streamlit*** — minimal code, maps directly onto the pandas
tables and forms you already use, and is easy for you to extend later. Native
desktop (PySide/Tkinter) was considered but costs more UI code to maintain.

### Phase 3 — Remove the install cliff
*Goal: a student goes from "nothing" to "uploading" with as few steps as
possible. Exact approach decided by the IT answer above.*

- **Bundle Python — always.** Per the maintainer, we cannot assume Python is on
  the machine, so the deliverable must include its own runtime (PyInstaller
  one-file build, or an installer that ships Python). A student with nothing
  installed should be able to run it.
- **Delivery mechanism depends on the IT answer:**
  - *If students can run downloaded apps:* a **one-click launcher** (double-click
    → opens the Streamlit app in their browser).
  - *If managed-machine policy blocks unsigned apps:* a **code-signed build**
    and/or distribution through the **campus software portal**.
- Either way: **rewrite the README** down to a short, friendly quick-start, and
  retire the misleading `update_and_test.py` (or fix it to do what its name and
  README claim, against the correct default branch).

### Phase 4 — A small static onboarding site *(optional, cheap)*
*Goal: a single friendly web page students are pointed to first.*

- A static page (GitHub Pages) with: a 2-minute "what is this / how do I start"
  explainer, the download/setup link, the VPN reminder, and "who to contact."
- This is the *only* truly-hosted piece, and it stores **no data** — so the PHI
  constraint doesn't apply.

---

## Suggested order & effort (rough)

| Phase | Outcome | Effort | Risk |
|---|---|---|---|
| 0 ✅ | Testable code + synthetic data | done | none (no behavior change) |
| 1 | No more unhelpful crashes; safe batch; guarded delete | medium | low–med (backend, well-tested) |
| 2 | Streamlit "see-your-data" app + optional terminal | large | medium (new UI, reuses tested core) |
| 3 | Easy install (decided by IT answer) | small–med | low (mostly packaging/docs) |
| 4 | Static onboarding page | small | none |

**Recommended next step after this PR:** Phase 1. It delivers immediate
day-to-day relief for current students *and* untangles the app logic from the
terminal, which is exactly what makes Phase 2's GUI feasible.

---

## Decisions on file (resolved 2026-06-04)

1. **Python is NOT assumed installed** on student machines → Phase 3 packaging
   must bundle the Python runtime. *Remaining sub-question for IT:* may students
   run an unsigned downloaded app, or is signing / the software portal required?
   (Decides only the Phase 3 delivery mechanism.)
2. **Trauma batch upload is out of scope** for now → tracked for future speckit
   development in **issue #24** (`batch_upload.py:546`).
3. **The destructive delete script is run only by the maintainer / Data
   Librarian** → guard rails = clear confirmation + dry-run, not multi-user
   lockdown.
