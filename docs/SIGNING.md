# SIGNING.md — Code-Signing Runbook

**Audience:** Data Librarian or build engineer preparing a release.
**Purpose:** Step-by-step instructions for signing the Windows build with signtool and signing + notarizing the macOS build with codesign and notarytool.

> Steps marked **[OPERATOR ACTION]** require a purchased certificate, a registered Apple Developer account, or access to CI secrets. An autonomous build process cannot complete these steps on its own.

---

## Overview

Code-signing proves to the operating system and to users that the binary was produced by a known publisher and has not been tampered with since signing. On UIowa managed machines, unsigned or improperly signed executables may be blocked by app-control policies (Windows) or Gatekeeper (macOS) before the student can launch them.

Both platforms are covered below. Complete the steps in order: build → sign → (macOS: notarize + staple) → hand off to ITS.

---

## Windows — signtool + EV/OV certificate

### What you need

**[OPERATOR ACTION]** Procure a **code-signing certificate** from a trusted Certificate Authority (CA) recognized by Microsoft, such as DigiCert, Sectigo, or GlobalSign. Two tiers:

- **OV (Organization Validation):** Shows your organization name in SmartScreen prompts. Lower cost; SmartScreen may still warn on first-seen files until the publisher reputation is established.
- **EV (Extended Validation):** Hardware token (USB) or cloud HSM required. Establishes immediate SmartScreen reputation. Recommended for the fastest unblocked experience on managed machines.

The certificate (`.pfx` file or HSM reference) and its password must be stored in **CI secrets**, not in the repository.

---

### Step 1 — Confirm signtool is available

`signtool.exe` ships with the Windows SDK. On a build machine or CI runner:

```cmd
where signtool
signtool /?
```

If not found, install the Windows SDK (the "Windows App Certification Kit" component is sufficient).

---

### Step 2 — Locate the build output

```
dist\XNAT-Interact\XNAT-Interact.exe
```

You should also sign the DLLs bundled alongside the executable. The command below signs the main executable; for a production release, sign all `.exe` and `.dll` files in `dist\XNAT-Interact\`.

---

### Step 3 — Sign the executable

Replace `CERT_PATH`, `CERT_PASSWORD`, and `TIMESTAMP_URL` with values from your CI secrets or build environment. **Do not hardcode these in the spec file or any committed file.**

```cmd
REM Sign the main executable
signtool sign ^
  /fd SHA256 ^
  /f "%CERT_PATH%" ^
  /p "%CERT_PASSWORD%" ^
  /tr "%TIMESTAMP_URL%" ^
  /td SHA256 ^
  /v ^
  dist\XNAT-Interact\XNAT-Interact.exe
```

Common timestamp server URLs (pick one):

- `http://timestamp.digicert.com`
- `http://timestamp.sectigo.com`
- `http://timestamp.globalsign.com/scripts/timstamp.dll`

The timestamp ensures the signature remains valid after the certificate expires. Always timestamp.

For EV certificates on a cloud HSM (e.g., DigiCert KeyLocker), replace `/f` and `/p` with the HSM provider flags per the CA's documentation.

---

### Step 4 — Sign all bundled DLLs (production release)

```cmd
REM Sign every .exe and .dll in the onedir output tree
for /r "dist\XNAT-Interact" %%f in (*.exe *.dll) do (
    signtool sign ^
      /fd SHA256 ^
      /f "%CERT_PATH%" ^
      /p "%CERT_PASSWORD%" ^
      /tr "%TIMESTAMP_URL%" ^
      /td SHA256 ^
      "%%f"
)
```

---

### Step 5 — Verify the signature

```cmd
signtool verify /pa /v dist\XNAT-Interact\XNAT-Interact.exe
```

Expected output contains `Successfully verified`. If you see `No signature found` or `The file is not digitally signed`, re-run Step 3.

---

### Step 6 — Hand off to ITS

Zip `dist\XNAT-Interact\` and provide it to ITS along with:

- The publisher name shown in the signature (ITS may need it for app-control allowlisting).
- The intended silent install / uninstall commands (for MECM packaging).
- See `docs/PACKAGING.md → Path 1` and `installer/build/windows.spec` comments for MECM notes.

---

## macOS — Developer ID + codesign + notarytool

### What you need

**[OPERATOR ACTION]** The following require enrollment in the Apple Developer Program (annual fee; requires an Apple ID and organization or individual enrollment at developer.apple.com):

- **Developer ID Application certificate** — issued by Apple, installed in your macOS Keychain on the build machine (or available via a CI keychain import step).
- **Apple ID** (or API key) and **app-specific password** (or API key secret) for notarytool. These are distinct from the certificate and belong in CI secrets.
- **Team ID** — shown in your Apple Developer account under Membership.

---

### Step 1 — Confirm your signing identity

```bash
security find-identity -v -p codesigning
```

Look for a line like:

```
1) ABCDEF1234567890ABCDEF1234567890ABCDEF12 "Developer ID Application: Your Name (TEAM_ID)"
```

Copy the full quoted string — this is your `SIGNING_IDENTITY`.

---

### Step 2 — Sign the app bundle

```bash
SIGNING_IDENTITY="Developer ID Application: Your Name (TEAM_ID)"

codesign \
  --deep \
  --force \
  --options runtime \
  --entitlements installer/build/entitlements.plist \
  --sign "${SIGNING_IDENTITY}" \
  dist/XNAT-Interact.app
```

`--options runtime` enables the hardened runtime, which is required for notarization.
`--deep` ensures nested binaries and frameworks are signed.

---

### Step 3 — Verify the local signature

```bash
codesign --verify --deep --strict --verbose=2 dist/XNAT-Interact.app
```

Expected: no errors. Common issues:

- `code object is not signed at all` → an embedded binary was added after signing; re-sign.
- `a sealed resource is missing or invalid` → remove any files added to the `.app` after signing; re-sign.

---

### Step 4 — Wrap in a DMG

```bash
create-dmg \
  --volname "XNAT-Interact" \
  --window-size 600 400 \
  --app-drop-link 400 200 \
  dist/XNAT-Interact.dmg \
  dist/XNAT-Interact.app

# Sign the DMG itself
codesign --sign "${SIGNING_IDENTITY}" dist/XNAT-Interact.dmg
```

---

### Step 5 — Submit for notarization

**[OPERATOR ACTION]** Notarization requires an active Apple Developer account. Credentials go in CI secrets.

```bash
# Using Apple ID + app-specific password:
xcrun notarytool submit dist/XNAT-Interact.dmg \
  --apple-id  "${APPLE_ID}" \
  --password  "${APPLE_APP_PASSWORD}" \
  --team-id   "${APPLE_TEAM_ID}" \
  --wait

# Alternative: using an API key (recommended for CI, avoids 2FA):
xcrun notarytool submit dist/XNAT-Interact.dmg \
  --key       "${APPLE_API_KEY_PATH}" \
  --key-id    "${APPLE_API_KEY_ID}" \
  --issuer    "${APPLE_API_ISSUER}" \
  --wait
```

`--wait` blocks until Apple returns a result. The process typically takes 1–5 minutes.

Successful output includes `status: Accepted`. If it returns `status: Invalid`, retrieve the log:

```bash
xcrun notarytool log <submission-id> \
  --apple-id "${APPLE_ID}" \
  --password "${APPLE_APP_PASSWORD}" \
  --team-id  "${APPLE_TEAM_ID}"
```

The log shows which binary failed and why (commonly: unsigned embedded binary, disallowed entitlement, or invalid architecture).

---

### Step 6 — Staple the notarization ticket

```bash
xcrun stapler staple dist/XNAT-Interact.dmg
```

Stapling embeds the notarization ticket in the DMG so Gatekeeper can verify it offline (important for machines that may not have internet access at install time).

---

### Step 7 — Verify notarization

```bash
xcrun stapler validate dist/XNAT-Interact.dmg

spctl --assess --type open --context context:primary-signature -v dist/XNAT-Interact.dmg
```

Expected from `spctl`: `accepted` with source `Notarized Developer ID`.

---

### Step 8 — Hand off to ITS

Provide the notarized `.dmg` to ITS with:

- The Developer ID publisher name (ITS may need it for Gatekeeper or MDM allowlisting).
- Whether the app installs to `/Applications` (requires Jamf policy elevation) or `~/Applications` (user-profile, no admin needed).
- See `docs/PACKAGING.md → Path 1` and `installer/build/macos.md` for Jamf packaging notes.

---

## Secret management summary

| Secret | Where it lives | Who sets it up |
|---|---|---|
| Windows cert file (`.pfx`) | CI secret (`CERT_PATH` or base64 env var) | **[OPERATOR ACTION]** Data Librarian / build engineer |
| Windows cert password | CI secret (`CERT_PASSWORD`) | **[OPERATOR ACTION]** Data Librarian / build engineer |
| Apple Developer ID certificate | macOS Keychain on build machine or CI keychain step | **[OPERATOR ACTION]** Data Librarian / build engineer |
| Apple ID | CI secret (`APPLE_ID`) | **[OPERATOR ACTION]** Data Librarian |
| Apple app-specific password | CI secret (`APPLE_APP_PASSWORD`) | **[OPERATOR ACTION]** Data Librarian |
| Apple Team ID | CI secret (`APPLE_TEAM_ID`) | **[OPERATOR ACTION]** Data Librarian |

None of these values appear in any committed file. If you see a secret in a committed file, treat it as compromised and rotate it immediately.

---

## Related documents

- `docs/PACKAGING.md` — delivery paths and update mechanisms
- `docs/OPS_CHECKLIST.md` — complete list of operator-required actions
- `installer/build/build.sh` — build driver (run before signing)
- `installer/build/windows.spec` — Windows PyInstaller spec
- `installer/build/macos.md` — macOS build and Jamf packaging notes
