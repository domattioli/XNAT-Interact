# OPS_CHECKLIST.md — Operator and ITS Action Checklist

**Audience:** Data Librarian, departmental IT contact, ITS liaison.
**Purpose:** A complete list of things that *require* a human operator or ITS and *cannot* be done by an automated build process or autonomous agent. Each item identifies what is needed and who is responsible.

Check each item before attempting a release. Items are grouped by the order in which they must be completed.

---

## Before the first release

### (a) Request ITS to package and deploy in the Software Center

> **Who:** Data Librarian (opens the request) + ITS (fulfills it).

The Software Center (MECM on Windows, Jamf on macOS) is the primary delivery path. ITS controls both catalogs. The Data Librarian must open a service request before any student can install the app through the Software Center.

- [ ] Identify the ITS contact or ITSM queue for Software Center additions (check the UIowa ITS service catalog at its.uiowa.edu or contact the departmental IT coordinator).
- [ ] Prepare the request with the following information:
  - Application name: **XNAT-Interact**
  - Publisher: University of Iowa Department of [your department]
  - Version: current release version (from the `installer/VERSION` file)
  - Supported platforms: Windows and/or macOS (specify which you are ready to deploy first)
  - Installation behavior: user-profile install, **no admin elevation required**
  - Silent install command: provided with the build artifact (from `installer/build/windows.spec` comments for Windows; Jamf pkg behavior for macOS)
  - Silent uninstall command: provided with the build artifact
  - Detection method: file/registry key presence (ITS will specify)
  - Signed artifact: attach the signed build output (see items (b) and (c) below — signing must be complete before submitting to ITS)
- [ ] Confirm with ITS that **primary-user assignment** is set correctly for the target student devices. Per UIowa policy, the student must be the primary user of the device in the ITS system before a scoped Software Center policy can deliver software to them.
- [ ] Confirm with ITS that target students are in the **correct department security group** in MECM (Windows) or Jamf (macOS). ITS scopes the delivery policy to that group — students outside the group will not see the app in the catalog.
- [ ] Confirm with ITS the expected **deployment timeline** and how students will be notified.
- [ ] For updates: establish the process for ITS to receive a new signed artifact when a new version is released, and confirm how ITS will push the update through the managed policy.

---

### (b) Procure a Windows code-signing certificate

> **Who:** Data Librarian or departmental IT (purchases or requests through university procurement).

An unsigned Windows executable will be blocked by SmartScreen and by UIowa app-control policies on managed machines. A valid code-signing certificate from a Microsoft-trusted CA is required.

- [ ] Determine whether the university has an enterprise agreement with a CA (DigiCert, Sectigo, or GlobalSign are common). Contact UIowa Procurement or ITS if unsure.
- [ ] Choose the certificate tier:
  - **OV (Organization Validation):** adequate for most purposes; SmartScreen reputation builds over time.
  - **EV (Extended Validation):** recommended; requires a hardware token (USB) or cloud HSM but establishes immediate SmartScreen reputation. Best option for a managed-machine rollout.
- [ ] Purchase and receive the certificate. Store the `.pfx` file (or HSM credentials) **securely — never commit to the repository**.
- [ ] Add the certificate and its password to the CI environment as secrets:
  - `CERT_PATH` (path to the `.pfx` on the build machine, or base64-encoded content)
  - `CERT_PASSWORD` (the certificate password)
  - Refer to `docs/SIGNING.md → Windows section` for how signtool uses these values.
- [ ] Verify signtool can access the certificate on the build machine before attempting a release build.
- [ ] Note the certificate expiration date. Set a calendar reminder to renew **at least 30 days** before it expires. A lapsed certificate invalidates all future signed builds (but does not retroactively invalidate already-released signed builds, provided they were timestamped — see `docs/SIGNING.md`).

---

### (c) Enroll in the Apple Developer Program and set up notarization credentials

> **Who:** Data Librarian or designated university Apple Developer account holder.

macOS Gatekeeper will block any app that is not both code-signed with a Developer ID certificate *and* notarized by Apple. This requires an Apple Developer Program enrollment.

- [ ] Check whether the university or department already has an Apple Developer Program organization account. Contact ITS or the departmental IT coordinator before creating a new one — duplicate accounts are wasteful and complicate team management.
- [ ] If no account exists: enroll at [developer.apple.com/programs](https://developer.apple.com/programs). An organization enrollment (not individual) is recommended for institutional software. Annual fee applies.
- [ ] Once enrolled, create a **Developer ID Application** certificate in the Apple Developer portal and install it in the macOS Keychain on the build machine.
- [ ] Create an **app-specific password** for the Apple ID (at appleid.apple.com under Security → App-Specific Passwords) for use with notarytool. Alternatively, create an **App Store Connect API key** (recommended for CI — avoids 2FA) under Users and Access → Integrations → App Store Connect API.
- [ ] Add the following to CI secrets:
  - `APPLE_ID` — the Apple ID email address used for the developer account
  - `APPLE_APP_PASSWORD` — the app-specific password (or use the API key alternative below)
  - `APPLE_TEAM_ID` — your 10-character Team ID (shown in the Membership section of the Apple Developer portal)
  - `APPLE_API_KEY_PATH`, `APPLE_API_KEY_ID`, `APPLE_API_ISSUER` — if using an API key instead of Apple ID + app-specific password
  - Refer to `docs/SIGNING.md → macOS section` for how notarytool uses these values.
- [ ] Note the Developer ID certificate expiration date (typically 5 years from issuance). Set a calendar reminder to renew before it expires.

---

### (d) Confirm the app-control policy allows the signed self-served installer

> **Who:** Data Librarian + ITS security team.

Even a properly signed and notarized binary can be blocked on UIowa managed machines if the app-control or endpoint security policy does not recognize the publisher. The Software Center path bypasses this (ITS controls the deployment), but the self-served installer path may still be subject to app-control on managed machines.

- [ ] Identify the app-control policy in effect on the target student machines. On Windows, this may be Windows Defender Application Control (WDAC), AppLocker, or a third-party endpoint protection product. On macOS, Jamf MDM policies may restrict app launches.
- [ ] Provide ITS security with the signed binary's publisher name (from the certificate) and request that it be **added to the app-control allowlist** for the relevant machine group.
- [ ] Test the self-served installer on a representative managed machine *before* announcing it to students. Confirm that no app-control block occurs after the allowlist entry is in place.
- [ ] If the app-control policy cannot be updated in a reasonable timeframe, the self-served path is **not viable for those machines** — direct those students to the Software Center path instead. Document this limitation in the README or in communications to students.
- [ ] For BYOD / personal machines: app-control policies generally do not apply, and a validly signed + notarized binary will run without intervention. No ITS action is needed for BYOD.

---

## For each subsequent release

- [ ] Build the new version (run `bash installer/build/build.sh`).
- [ ] Sign the Windows artifact (see `docs/SIGNING.md → Windows section`).
- [ ] Sign, notarize, and staple the macOS artifact (see `docs/SIGNING.md → macOS section`).
- [ ] Verify both signatures before distribution.
- [ ] Notify ITS with the new signed artifact so they can publish the update through the Software Center policy.
- [ ] For self-served users: publish the new signed installer to the distribution location (e.g., a GitHub release). The in-app update checker will surface a notice to users on their next launch.
- [ ] Update the `installer/VERSION` file to the new version number before building.

---

## Related documents

- `docs/PACKAGING.md` — full delivery path descriptions and the detect-or-bundle Python design
- `docs/SIGNING.md` — step-by-step code-signing and notarization instructions
- `installer/build/build.sh` — build driver script
- `installer/build/macos.md` — macOS build and Jamf packaging notes
