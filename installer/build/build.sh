#!/usr/bin/env bash
# =============================================================================
# build.sh — XNAT-Interact build driver
# =============================================================================
#
# PURPOSE
# -------
# Detect the current platform, run the appropriate bundler (PyInstaller on
# Windows, PyInstaller or Briefcase on macOS), and report the output path.
# This script does NOT sign or notarize the output — see docs/SIGNING.md for
# those steps and docs/OPS_CHECKLIST.md for operator prerequisites.
#
# USAGE
# -----
#   bash installer/build/build.sh [--platform windows|macos] [--clean]
#
#   --platform windows|macos   Override auto-detection (useful in CI)
#   --clean                    Remove dist/ and build/ before building
#
# REQUIREMENTS
# ------------
#   pip install pyinstaller     (both platforms)
#   brew install create-dmg     (macOS only, for DMG wrapping)
#   Python 3.9+ in the build environment (separate from the bundled runtime)
#
# OUTPUT
# ------
#   Windows: dist/XNAT-Interact/XNAT-Interact.exe  (onedir)
#   macOS:   dist/XNAT-Interact.app               (app bundle)
#            dist/XNAT-Interact.dmg               (if create-dmg is available)
#
# NO SECRETS
# ----------
# This script contains no credentials, signing identities, API keys, or server
# URLs. Signing secrets belong in CI environment variables; see docs/SIGNING.md.
#
# SIGNING (post-build, not done here)
# ------------------------------------
# After a successful build:
#   Windows: signtool — see docs/SIGNING.md → Windows section
#   macOS:   codesign + notarytool — see docs/SIGNING.md → macOS section
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

log()  { printf '[build.sh] %s\n' "$*"; }
fail() { printf '[build.sh] ERROR: %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

PLATFORM_OVERRIDE=""
CLEAN=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --platform)
            PLATFORM_OVERRIDE="${2:-}"
            shift 2
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        *)
            fail "Unknown argument: $1. Usage: build.sh [--platform windows|macos] [--clean]"
            ;;
    esac
done

# ---------------------------------------------------------------------------
# Locate the repo root (two levels up from installer/build/)
# ---------------------------------------------------------------------------

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
log "Repo root: ${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Detect platform
# ---------------------------------------------------------------------------

if [[ -n "${PLATFORM_OVERRIDE}" ]]; then
    PLATFORM="${PLATFORM_OVERRIDE}"
    log "Platform override: ${PLATFORM}"
else
    case "$(uname -s)" in
        Darwin)  PLATFORM="macos" ;;
        MINGW*|CYGWIN*|MSYS*) PLATFORM="windows" ;;
        Linux)
            # GitHub Actions Windows runners may report Linux via WSL; treat
            # a forced build env var as the tiebreaker.
            if [[ "${CI_TARGET_PLATFORM:-}" == "windows" ]]; then
                PLATFORM="windows"
            else
                fail "Linux host detected. Use --platform to specify 'windows' or 'macos'."
            fi
            ;;
        *)
            fail "Unrecognised OS: $(uname -s). Use --platform windows|macos."
            ;;
    esac
    log "Detected platform: ${PLATFORM}"
fi

# ---------------------------------------------------------------------------
# Optional clean
# ---------------------------------------------------------------------------

if [[ "${CLEAN}" == "true" ]]; then
    log "Cleaning dist/ and build/ ..."
    rm -rf "${REPO_ROOT}/dist" "${REPO_ROOT}/build"
fi

# ---------------------------------------------------------------------------
# Confirm PyInstaller is available
# ---------------------------------------------------------------------------

if ! command -v pyinstaller >/dev/null 2>&1; then
    fail "pyinstaller not found. Install it: pip install pyinstaller"
fi
PYINSTALLER_VERSION="$(pyinstaller --version 2>&1 || true)"
log "PyInstaller version: ${PYINSTALLER_VERSION}"

# ---------------------------------------------------------------------------
# Platform-specific build
# ---------------------------------------------------------------------------

case "${PLATFORM}" in

    # -----------------------------------------------------------------------
    windows)
    # -----------------------------------------------------------------------
        SPEC="${REPO_ROOT}/installer/build/windows.spec"
        [[ -f "${SPEC}" ]] || fail "Spec file not found: ${SPEC}"

        log "Building Windows artifact with PyInstaller ..."
        pyinstaller "${SPEC}" --distpath "${REPO_ROOT}/dist" --workpath "${REPO_ROOT}/build"

        OUTPUT="${REPO_ROOT}/dist/XNAT-Interact"
        log "Build complete. Output: ${OUTPUT}"
        log ""
        log "Next steps:"
        log "  1. Smoke-test: dist\\XNAT-Interact\\XNAT-Interact.exe"
        log "  2. Sign:       see docs/SIGNING.md → Windows section"
        log "  3. Package:    hand dist\\XNAT-Interact\\ to ITS for MECM wrapping"
        log "                 see docs/PACKAGING.md → Software Center (Primary) path"
        ;;

    # -----------------------------------------------------------------------
    macos)
    # -----------------------------------------------------------------------
        # Use the macOS PyInstaller spec if present, otherwise fall back to a
        # Briefcase build. The macOS spec mirrors windows.spec with a BUNDLE
        # block — see installer/build/macos.md for authoring notes.
        MACOS_SPEC="${REPO_ROOT}/installer/build/macos.spec"

        if [[ -f "${MACOS_SPEC}" ]]; then
            log "Building macOS .app bundle with PyInstaller ..."
            pyinstaller "${MACOS_SPEC}" --distpath "${REPO_ROOT}/dist" --workpath "${REPO_ROOT}/build"
            APP_OUTPUT="${REPO_ROOT}/dist/XNAT-Interact.app"
        elif command -v briefcase >/dev/null 2>&1; then
            log "macos.spec not found; falling back to Briefcase ..."
            (cd "${REPO_ROOT}" && briefcase build macOS)
            APP_OUTPUT="${REPO_ROOT}/macOS/app/XNAT-Interact/XNAT-Interact.app"
        else
            fail "Neither installer/build/macos.spec nor 'briefcase' found. " \
                 "Author macos.spec (see installer/build/macos.md) or install Briefcase."
        fi

        log "App bundle: ${APP_OUTPUT}"

        # Optional: wrap in a DMG if create-dmg is available
        if command -v create-dmg >/dev/null 2>&1; then
            log "create-dmg found — wrapping .app in a DMG ..."
            DMG_OUTPUT="${REPO_ROOT}/dist/XNAT-Interact.dmg"
            create-dmg \
                --volname "XNAT-Interact" \
                --window-size 600 400 \
                --app-drop-link 400 200 \
                "${DMG_OUTPUT}" \
                "${APP_OUTPUT}"
            log "DMG: ${DMG_OUTPUT}"
        else
            log "(create-dmg not found — skipping DMG wrapping. Install with: brew install create-dmg)"
        fi

        log ""
        log "Next steps:"
        log "  1. Sign:        see docs/SIGNING.md → macOS section"
        log "  2. Notarize:    xcrun notarytool submit ... (see docs/SIGNING.md)"
        log "  3. Staple:      xcrun stapler staple dist/XNAT-Interact.dmg"
        log "  4. Package:     hand notarized DMG to ITS for Jamf (see docs/PACKAGING.md)"
        ;;

    *)
        fail "Unsupported platform value: ${PLATFORM}"
        ;;
esac

log "Done."
