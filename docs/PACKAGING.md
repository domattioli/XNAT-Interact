# PACKAGING.md — Delivery and Installation Runbook

**Audience:** Data Librarian, departmental IT, ITS liaison.
**Purpose:** Explains the two delivery paths for XNAT-Interact, who does what on each path, and how updates reach students.

---

## Why two paths?

UIowa IT-managed laptops (Windows and Mac) lock out local admin for students. Software must be deployed by ITS, not installed by the student. This is the primary and most reliable path.

Some students work on personal (BYOD) or temporarily unmanaged machines where the Software Center is unavailable. The signed self-served installer is the fallback for those cases.

Both paths use the same application. The only difference is who performs the installation and how updates arrive.

---

## Decision table — which path for which machine?

| Machine type | Student has admin? | Recommended path |
|---|---|---|
| UIowa managed laptop (standard) | No | **Software Center (Primary)** |
| UIowa managed desktop | No | **Software Center (Primary)** |
| BYOD / personal laptop | Yes | **Signed self-served installer (Fallback)** |
| Loaner / lab workstation managed by ITS | No | **Software Center (Primary)** |
| Machine where Software Center policy is not yet scoped | No | Ask ITS to scope the policy, *then* Software Center |
| Machine blocked by app-control even after signing | Varies | Escalate to ITS — do not distribute an unsigned binary |

When in doubt, always try the Software Center path first. The self-served installer is a backup, not the default.

---

## Path 1 — Software Center (Primary)

### What this path is

ITS packages the signed XNAT-Interact build into the **MECM Software Center** (Windows) or **Jamf Self Service** (macOS) catalog. Students open the Software Center app, find XNAT-Interact, and click Install. No terminal, no Python, no admin password required.

### Prerequisites (operator actions — see `docs/OPS_CHECKLIST.md`)

- The Data Librarian or departmental IT contact **opens a request with ITS** to add XNAT-Interact to the Software Center catalog.
- The student must be the **primary user** of the device in the ITS system.
- The student must be in the **correct department security group** in MECM/Jamf before the policy is scoped to their machine.
- ITS requires a **signed build artifact** (see Path 2 for signing) before they will package it.

### How students use it

1. Open the ITS Software Center app (Windows) or Jamf Self Service (macOS).
2. Search for "XNAT-Interact."
3. Click Install.
4. Launch from the Start Menu (Windows) or Applications folder (macOS).

That is the entire process from the student's perspective.

### How updates work on this path

ITS publishes a new build through the same Software Center policy. Students receive the update automatically as part of the managed deployment cycle — no student action required. The Data Librarian notifies ITS when a new release is ready and provides the updated signed artifact.

---

## Path 2 — Signed self-served installer (Fallback)

### What this path is

A code-signed installer that a student downloads and runs directly, without going through the Software Center. Because it is code-signed:

- **Windows:** SmartScreen and app-control policies recognize the publisher and do not block it.
- **macOS:** Gatekeeper allows it to run without quarantine warnings because it is signed *and* notarized.

An **unsigned** binary will be blocked on managed machines and will produce a warning on most modern personal machines. Do not distribute an unsigned build.

### Prerequisites (operator actions — see `docs/OPS_CHECKLIST.md`)

- A **Windows code-signing certificate** (OV or EV) from a trusted CA, used with signtool.
- An **Apple Developer ID Application certificate** and Apple Developer Program enrollment, used with codesign + notarytool.
- Both sets of credentials live in CI secrets and are never stored in the repository.

### How students use it

1. Download the installer from the link the Data Librarian provides (e.g., a GitHub release or a shared drive link).
2. **Windows:** Run the installer or unzip the archive and double-click `XNAT-Interact.exe`.
3. **macOS:** Open the `.dmg`, drag `XNAT-Interact.app` to Applications (or `~/Applications` for a user-profile install without admin).

The app installs to the user's profile — no admin password is required.

### How updates work on this path

The app includes an **in-app update checker** (built in Phase 3). On each launch it silently checks whether a newer version has been published. If one is found, a non-blocking notice appears: *"A new version of XNAT-Interact is available — download it here."* The student can dismiss the notice and continue working; the current version keeps running. If the network is unavailable, no notice appears and no error is shown.

The update checker does **not** download or install anything automatically. The student downloads the new signed installer and runs it when they are ready.

---

## The detect-or-bundle Python design

Both paths use the same launcher. On startup, the launcher:

1. **Scans for an existing compatible Python** — checks PATH, the Windows registry, and common install locations (e.g., `%LOCALAPPDATA%\Programs\Python`, `/usr/local/bin`, `/opt/homebrew/bin`). It verifies the interpreter's major/minor version and its ability to create a virtual environment.
2. **Reuses it if found** — the bundled runtime is not invoked, keeping startup fast.
3. **Falls back to the bundled runtime** if no compatible interpreter is found — the app starts regardless of whether Python is installed on the machine.

The bundled runtime is a full Python interpreter included in the build artifact (packaged with PyInstaller). It is the fallback, not the primary. The launcher records which path was taken in a local log file that contains no patient data.

If the bundled runtime itself is missing or corrupt, the launcher surfaces a plain-language message: *"The installation looks incomplete — reinstall from the Software Center or contact your Data Librarian."* It never shows a Python traceback to the student.

---

## Build artifacts

| Artifact | Platform | Who builds it | How |
|---|---|---|---|
| `dist/XNAT-Interact/` (onedir) | Windows | Build engineer / CI | `bash installer/build/build.sh --platform windows` |
| `dist/XNAT-Interact.app` + `.dmg` | macOS | Build engineer / CI | `bash installer/build/build.sh --platform macos` |

After building, sign the artifact before distributing or handing off to ITS. See `docs/SIGNING.md` for signing steps and `docs/OPS_CHECKLIST.md` for the operator prerequisites.

---

## Related documents

- `docs/SIGNING.md` — step-by-step code-signing runbook for Windows and macOS
- `docs/OPS_CHECKLIST.md` — explicit list of things that require the operator / ITS
- `installer/build/windows.spec` — PyInstaller spec for the Windows build
- `installer/build/macos.md` — macOS build and Jamf packaging notes
- `installer/build/build.sh` — build driver script
