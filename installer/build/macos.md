# macOS Packaging Notes — XNAT-Interact

**Audience:** Build engineer / Data Librarian preparing the macOS artifact.
**Prerequisite reading:** `docs/PACKAGING.md` (delivery paths), `docs/SIGNING.md` (codesign + notarization detail), `docs/OPS_CHECKLIST.md` (operator-only steps).

---

## Tool choice

Two viable options for bundling a Python runtime into a macOS app bundle:

| Tool | Output | Best for |
|---|---|---|
| **PyInstaller** | `XNAT-Interact.app` inside `dist/` | Straightforward; same toolchain as Windows spec |
| **Briefcase** | `.app` → `.dmg` | Richer installer UX; better Jamf-to-DMG integration |

**Recommendation:** start with PyInstaller (reuses the Windows spec pattern), then wrap the `.app` in a DMG with `create-dmg` for the Jamf package. Switch to Briefcase if the app grows native-UI needs.

---

## 1. Prerequisites

Install build dependencies **in your build environment** (not the app's venv):

```bash
pip install pyinstaller
# If wrapping in DMG:
brew install create-dmg
```

Confirm you are building on macOS — PyInstaller does not cross-compile.

---

## 2. Build with PyInstaller

```bash
# From the repo root:
pyinstaller installer/build/macos.spec
```

> **macOS spec file:** `installer/build/macos.spec` (authored separately — follows the same structure as `windows.spec`, with macOS-specific `BUNDLE` block substituted for `COLLECT`).

Output: `dist/XNAT-Interact.app`

Key differences from the Windows spec:

- Replace the `COLLECT` step with a `BUNDLE` step that produces an `.app` bundle.
- Set `bundle_identifier` (e.g. `edu.uiowa.XNAT-Interact`).
- `console=False` → use `LSUIElement=True` in the Info.plist if the app is browser-based and should not show a Dock icon.
- Icon: `app/static/icon.icns` (`.icns` format, not `.ico`).

### Briefcase alternative

```bash
pip install briefcase
briefcase build macOS
# Output: macOS/app/XNAT-Interact/XNAT-Interact.app
```

---

## 3. Code-sign the app bundle

> **OPERATOR ACTION required** — you must have an Apple Developer ID Application certificate and be enrolled in the Apple Developer Program. See `docs/SIGNING.md` → macOS section and `docs/OPS_CHECKLIST.md`.

```bash
# SIGNING_IDENTITY is your "Developer ID Application: ..." certificate name.
# Retrieve it with:
security find-identity -v -p codesigning

SIGNING_IDENTITY="Developer ID Application: Your Name / Org (TEAM_ID)"

# Sign the entire .app bundle (deep = sign all nested binaries and frameworks)
codesign \
  --deep \
  --force \
  --options runtime \
  --entitlements installer/build/entitlements.plist \
  --sign "${SIGNING_IDENTITY}" \
  dist/XNAT-Interact.app
```

Verify:

```bash
codesign --verify --deep --strict --verbose=2 dist/XNAT-Interact.app
```

---

## 4. Wrap in a DMG (optional but recommended for Jamf)

```bash
create-dmg \
  --volname "XNAT-Interact" \
  --window-size 600 400 \
  --app-drop-link 400 200 \
  dist/XNAT-Interact.dmg \
  dist/XNAT-Interact.app
```

Sign the DMG as well:

```bash
codesign --sign "${SIGNING_IDENTITY}" dist/XNAT-Interact.dmg
```

---

## 5. Notarize with notarytool

> **OPERATOR ACTION required** — Apple ID + app-specific password (or API key) for the Apple Developer account. See `docs/SIGNING.md` → notarization section.

```bash
# Submit for notarization.
# --apple-id, --password, and --team-id come from CI secrets, not this file.
xcrun notarytool submit dist/XNAT-Interact.dmg \
  --apple-id  "${APPLE_ID}" \
  --password  "${APPLE_APP_PASSWORD}" \
  --team-id   "${APPLE_TEAM_ID}" \
  --wait
```

If the submission returns `status: Accepted`, staple:

```bash
xcrun stapler staple dist/XNAT-Interact.dmg
```

Verify notarization:

```bash
xcrun stapler validate dist/XNAT-Interact.dmg
spctl --assess --type open --context context:primary-signature -v dist/XNAT-Interact.dmg
```

---

## 6. Jamf packaging and deployment

> **OPERATOR ACTION required** — ITS controls Jamf. The Data Librarian or departmental IT contact opens the request; see `docs/OPS_CHECKLIST.md` items (a) and (d).

Hand the notarized `.dmg` to ITS with:

- **Installation behavior:** drag-to-Applications (or a pkg wrapper that copies to `/Applications`). App must run without admin rights — install to `/Applications` requires a Jamf policy (ITS handles that); for user-profile install, copy to `~/Applications` instead.
- **Uninstall behavior:** trash `XNAT-Interact.app`.
- **Detection method:** file exists at `/Applications/XNAT-Interact.app` (or `~/Applications/...`).
- **Primary-user requirement:** student must be the device's primary user in Jamf and in the correct department security group before the policy is scoped to them.
- **Scope:** limit to the relevant computer group (department / lab).

### Update delivery

ITS publishes a new `.dmg` through the Jamf policy. Students receive it as a background Jamf policy run; no student action required beyond the managed update.

---

## 7. entitlements.plist template

Create `installer/build/entitlements.plist` with the minimum required entitlements:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <!-- Allow the app to run in the hardened runtime -->
    <key>com.apple.security.cs.allow-jit</key>
    <false/>
    <key>com.apple.security.cs.allow-unsigned-executable-memory</key>
    <false/>
    <!-- Network access for XNAT server connection -->
    <key>com.apple.security.network.client</key>
    <true/>
</dict>
</plist>
```

Expand entitlements only as needed — tighter is safer for notarization approval.

---

## Summary checklist

- [ ] Build the `.app` with PyInstaller (or Briefcase)
- [ ] Sign the `.app` with `codesign --deep --options runtime`
- [ ] Wrap in a DMG with `create-dmg`; sign the DMG
- [ ] Submit DMG to Apple notarytool; wait for `Accepted`
- [ ] Staple the notarization ticket to the DMG
- [ ] Verify with `stapler validate` and `spctl`
- [ ] Hand signed+notarized DMG to ITS for Jamf packaging
