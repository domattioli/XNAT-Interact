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

### D. Safety & security gaps (found while writing this plan)
- **🔴 Burned-in pixel PHI is NOT removed.** De-identification
  (`xnat_scan_data.py:291`) scrubs *metadata* only. Fluoroscopy/OR images often
  have the **patient name and date burned into the image pixels**, and the code
  knows it — there's an unfinished `to-do` at `xnat_scan_data.py:305-306`
  ("De-identify embedded pixel data… OCR and blur"). Today that PHI is uploaded.
  This is the single most important thing to address.
- **Password accepted as a plaintext CLI argument** (`main.py:29`, `--password`)
  — visible in process listings and shell history.
- **The config "database" has a lost-update race.** `ConfigTables` works by
  download → edit locally → re-upload-and-overwrite `MetaTables.json`. Two
  students uploading at once can silently clobber each other's registrations and
  image-hashes.
- **The download path is under-built and buggy** — hardcoded Windows `\` paths,
  a stray `print('hello')`, and it only queries one session type
  (`main.py:388-404`). The plan was upload-heavy; download needs its own rework.
- **Annual SSL-cert expiry is a recurring failure.** The server cert needs
  yearly renewal (README); `XNATConnection` catches it but surfaces it
  generically instead of "the cert expired — contact the Data Librarian."

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

- UIowa-managed machines run under **least privilege** — students generally
  **lack local admin**, so they can't just download and install arbitrary tools.
- The institution's intended channel is the **ITS Software Center** (MECM), but
  software has to be **packaged/deployed by ITS** before it appears there.
- A **one-click installer is technically possible** — *PyInstaller* bundles
  Python *inside* the app — **but** managed machines may block running unsigned
  downloaded `.exe` files (app-control / SmartScreen), which can defeat the
  point. (Full policy detail + sources below.)

**Maintainer's decision (2026-06-04): we cannot assume Python is installed on
the student's machine.** That rules out a "guided setup script that needs Python
already there." Packaging (Phase 3) must therefore **bundle the Python runtime**
so a student with *nothing* installed can still run the app — e.g. a PyInstaller
one-file build, or an installer that ships its own Python.

**What UIowa's policy actually says** (researched 2026-06-04 — sources at the
bottom of this doc):

- UIowa centrally manages Windows via **MECM/SCCM** and Macs via **Jamf**, under
  a **least-privilege** policy. Students generally **do not have local admin
  rights**.
- The **Just-in-Time Admin (JTA)** self-elevation only applies to *Managed
  Application Services servers*, **not** student/staff desktops — so it is not a
  route for us.
- The sanctioned way onto a managed machine is the **ITS Software Center**, but
  an app must first be **packaged and deployed by ITS** (the user must be the
  device's primary user and in the right security group). Some Software-Center
  apps then install **without** admin.

**Consequence for us:** a self-served, unsigned `.exe` *may* run from the user's
profile without admin, but it is **not guaranteed** — app-control / SmartScreen /
antivirus can block unsigned binaries on managed machines. The **reliable** path
is therefore **distribution through ITS Software Center** (which avoids the
admin-rights and signing problems by design). Self-served download is a fallback,
and **code-signing materially improves its odds**.

**Net recommendation (updated):**

1. **Primary delivery = ITS Software Center package.** Have the Data Librarian /
   departmental IT work with ITS to package the app. Most robust on managed
   machines; needs lead time and an ITS relationship.
2. **Bundle Python** so a machine with *nothing* installed still works (per the
   maintainer's decision).
3. **The installer/launcher must detect an existing Python first** (per the
   maintainer): if a compatible interpreter is already present, *reuse it* (a
   lighter, faster path); only fall back to the bundled runtime when none is
   found. Avoids forcing a heavy install when Python already exists, and avoids
   assuming it when it doesn't.
4. **Fallback delivery = signed self-served installer** for non-managed / BYOD
   machines or where Software Center isn't an option.

The plan below is structured so we **don't bet the project on the delivery
mechanism**: the GUI (Phase 2) works whether it's launched from a Software-Center
install, a signed installer, or a dev's `pip` setup.

---

## Safety first: PHI handling (must-address, threads through every phase)

This is a medical tool; getting de-identification *right* outranks every UX
goal. Three concrete commitments:

1. **Burned-in pixel PHI.** Metadata scrubbing is not enough (see Finding D).
   - *Phase 1 (interim, no new infra):* before any upload, show the student each
     image and require a **"these images contain no visible patient
     name/date" confirmation**, with a simple manual redaction (draw a black box)
     option. A human-in-the-loop check is far better than today's silent pass.
   - *Later:* OCR-assisted detection to auto-flag/blur suspected text regions.
   - Tracked as a `known_issue` test now (`tests/test_dicom_deidentification.py`)
     so the gap is visible and can't be forgotten.
2. **"Here's exactly what will be uploaded" review.** A pre-flight summary
   (which subject, how many images, PHI-removed preview) before data leaves the
   machine — the student confirms, then it sends.
3. **Local PHI cleanup.** Intake forms, zips, and the downloaded
   `MetaTables.json` are written to temp/Downloads. Ensure local PHI artifacts
   are deleted after a successful upload (and document where they live until
   then).

## Cross-cutting refinements (added 2026-06-04)

These attach to the phases below rather than being phases of their own.

**Security & access** *(Phase 1)*
- Remove the `--password` CLI flag; never accept the password as an argument.
  Use a session-only prompt / GUI field; consider the OS keychain later, never a
  plaintext file.
- Make the **annual SSL-cert expiry** a first-class, dated, actionable error
  ("server certificate expired on <date> — contact the Data Librarian"), and add
  a renewal reminder to the maintainer runbook.

**Reliability & data integrity** *(Phase 1–2)*
- **Add a fake/mock XNAT layer for tests.** Phase 0 only covers pure logic; as
  soon as we touch upload/download we need a stand-in for `pyxnat` (a fake
  object, or recorded request/response fixtures) so networked paths are testable
  without the live server. This is the bridge that makes Phases 1–2 safe.
- **Fix the config lost-update race.** Treat `MetaTables.json` edits as a
  critical section: at minimum re-download-merge-reupload with a check, ideally a
  server-side lock or moving this metadata into XNAT-native fields. Flag clearly
  if concurrent edits are detected.
- **Rework the download path** (its own mini-project): cross-platform paths,
  remove debug code, query all relevant session types, progress + verification.
- **Resumable single uploads + post-upload verification.** A dropped VPN
  mid-upload should be recoverable, and the tool should confirm the server
  received exactly what was sent (counts / hashes) before declaring success.

**Adoption & process** *(Phases 2–4)*
- **Onboarding checklist.** Before code runs, a student needs an XNAT account
  (HawkID), to be added to the project by the Librarian, and VPN. Add a guided
  "are you set up? ① account ② added to project ③ VPN" status screen, and
  optionally auto-draft the access-request email to the Librarian. This first-week
  friction is a real adoption killer.
- **Update mechanism.** Once packaged, `git reset --hard` (today's
  `update_and_test.py`) is gone. Software Center handles updates for a packaged
  app; for the self-served build, add an in-app "a new version is available"
  check. State the chosen path explicitly.
- **Phase 2 "walking skeleton."** De-risk the big GUI phase: ship a thin slice
  first — **login → browse your data → single upload** — validate it with one
  real student, *then* add batch, download, and the terminal panel.

## Success metrics (how we'll know it worked)

The mission is adoption. Pick a small number and watch them before/after:

- **Time-to-first-upload** for a new student (from "I have access" to "data is
  on the server").
- **Upload error rate** (failed/aborted attempts ÷ attempts).
- **Active student uploaders** per term.
- **Support tickets to the Data Librarian** per student (should drop).

A lightweight, **non-PHI** local log of these events (opt-in, aggregate only) is
enough to start; no analytics service required.

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
8. **Remove the `--password` CLI flag** and stop accepting credentials as
   arguments (see Security & access above).
9. **Make SSL-cert expiry a clear, dated error** with a "contact the Data
   Librarian" recourse.
10. **Build the fake/mock XNAT test layer** so items 1–9 that touch the server
    are testable offline — this is the prerequisite that makes the rest of
    Phase 1 verifiable.
11. **Burned-in pixel-PHI confirmation step** (interim, human-in-the-loop) and
    **local PHI cleanup** after upload (see "Safety first" above).

*Each item ships with tests using the Phase 0 harness (plus the new fake-XNAT
layer for the networked ones).*

### Phase 2 — The Streamlit app: a "human-factors view of XNAT"
*Goal: students see and manage their data in a browser-like UI; the terminal
becomes optional.*

This is the big skill-reduction win, and matches your vision: a friendly local
app where students can **see the data**, with an **optional embedded terminal**
for those who want to learn the command-line way.

> **Build it as a walking skeleton.** Ship the thin slice first — **login →
> browse your data → single upload (with the PHI-review step)** — and validate it
> with one real student before adding batch, download, and the terminal panel.
> The onboarding checklist (account / project access / VPN) belongs on the login
> screen.

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
possible. Delivery channel informed by the UIowa policy research above.*

- **Primary delivery = ITS Software Center** (MECM/Jamf package). Most reliable
  on UIowa-managed machines because it sidesteps admin-rights and code-signing
  blocks. Requires the Data Librarian / departmental IT to work with ITS to
  package it; budget lead time.
- **Bundle Python — but detect it first.** Per the maintainer:
  - We **cannot assume Python is installed**, so the deliverable must be able to
    ship its own runtime.
  - The installer/launcher must **check for an existing compatible Python**
    (correct major/minor version) and **reuse it when present** — only falling
    back to the bundled runtime when none is found. (Detection: look on PATH /
    Windows registry / common install locations; verify version; verify it can
    import the app's deps or create a venv.)
- **Fallback delivery = signed self-served installer** for BYOD / non-managed
  machines, or where Software Center isn't available. Code-signing materially
  improves the odds of an unsigned-binary block.
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
| 1 | No unhelpful crashes; safe batch; guarded delete; **no plaintext password; SSL-cert error; fake-XNAT test layer; PHI-review + cleanup; config-race fix** | medium–large | low–med (backend, well-tested) |
| 2 | Streamlit "see-your-data" app + optional terminal (**walking skeleton first**) | large | medium (new UI, reuses tested core) |
| 3 | Easy install (bundle-or-detect Python; Software Center primary) | small–med | low (mostly packaging/docs) |
| 4 | Static onboarding page | small | none |

**Recommended next step after this PR:** Phase 1, **starting with the fake-XNAT
test layer and the burned-in-PHI review step.** Phase 1 delivers immediate
day-to-day relief for current students, closes the most important safety gap,
*and* untangles the app logic from the terminal — exactly what makes Phase 2's
GUI feasible.

---

## Decisions on file (resolved 2026-06-04)

1. **Python is NOT assumed installed** on student machines → Phase 3 packaging
   must be able to bundle the Python runtime, **and the installer must detect an
   existing Python and reuse it when present** (maintainer instruction). Policy
   research (below) shows UIowa-managed machines are least-privilege (no student
   admin) and that the **ITS Software Center** is the reliable delivery channel →
   Software Center is now the **primary** Phase 3 path, signed self-served
   installer the fallback. The only thing left to do is *operational*: have the
   Data Librarian / departmental IT open the ITS packaging request.
2. **Trauma batch upload is out of scope** for now → tracked for future speckit
   development in **issue #24** (`batch_upload.py:546`).
3. **The destructive delete script is run only by the maintainer / Data
   Librarian** → guard rails = clear confirmation + dry-run, not multi-user
   lockdown.
4. **All four refinement bundles folded in** (maintainer, 2026-06-04): PHI
   safety (burned-in pixel PHI — now a `known_issue` test, plus pre-upload review
   + local cleanup), security & access (no plaintext password; SSL-cert error),
   reliability & integrity (fake-XNAT test layer; config lost-update race;
   download rework; resumable uploads + verification), and adoption & process
   (onboarding checklist; success metrics; update mechanism; walking-skeleton
   sequencing). See "Safety first", "Cross-cutting refinements", and "Success
   metrics" above.

---

## Sources (UIowa IT policy research, 2026-06-04)

- ITS — Access Management Software Delivery (Software Center model, primary-user
  + security-group requirement):
  https://its.uiowa.edu/services/software-and-technology-tool-licensing-and-acquisition/access-management-software-delivery
- IT Security — Device Security Standard (managed-device / least-privilege
  expectations):
  https://itsecurity.uiowa.edu/device-security-standard
- ITS — Just-in-Time Admin (JTA) (self-elevation is for *managed app servers*,
  not desktops):
  https://its.uiowa.edu/services/managed-application-services/just-time-admin-jta-feature
- ITS — Installing software via Software Center / Self Service (example KB):
  https://its.uiowa.edu/support/article/117336
- ITS — Available Software / Software portal:
  https://its.uiowa.edu/available-software

*Note:* the Device Security Standard and JTA pages block automated fetching;
their contents above are summarized from ITS search results and should be
confirmed with ITS / departmental IT before the Phase 3 packaging request.
