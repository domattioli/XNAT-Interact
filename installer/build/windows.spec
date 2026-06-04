# =============================================================================
# XNAT-Interact — PyInstaller build spec (Windows)
# =============================================================================
#
# PURPOSE
# -------
# Produce a Windows executable bundle that ships its own Python runtime.
# The launcher (installer/launcher/launcher.py) first probes for a compatible
# existing Python and reuses it; this bundled runtime is the fallback only.
#
# USAGE
# -----
#   # From the repo root, with PyInstaller installed in the build environment:
#   pyinstaller installer/build/windows.spec
#
#   # Output lands in:
#   #   dist/XNAT-Interact/          (onedir mode — recommended for Software Center)
#   #   dist/XNAT-Interact.exe       (only if you switch to onefile below)
#
# REQUIREMENTS
# ------------
#   pip install pyinstaller
#   PyInstaller 6.x + Python 3.11 target interpreter recommended.
#   Run this on Windows (or in a Windows CI runner) — cross-compilation is
#   not supported by PyInstaller.
#
# SIGNING
# -------
#   After building, sign the output with signtool. See docs/SIGNING.md.
#
# ITS PACKAGING
# -------------
#   Hand the signed dist/ directory to ITS for MECM wrapping.
#   See docs/PACKAGING.md and the MECM packaging notes in
#   installer/build/macos.md (macOS equivalent).
#
# NO SECRETS IN THIS FILE
# -----------------------
#   Server URLs, project names, and credentials are read at runtime from the
#   Phase 1 AppConfig file (~/.xnat-interact/config.json). Nothing sensitive
#   is baked into the build artifact.
# =============================================================================

import sys
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths — resolved relative to the repo root at build time
# ---------------------------------------------------------------------------
# PyInstaller sets SPECPATH to the directory containing this .spec file.
# The repo root is two levels up (installer/build/ → installer/ → repo root).
REPO_ROOT = Path(SPECPATH).resolve().parent.parent  # noqa: F821 (SPECPATH is PyInstaller built-in)
LAUNCHER   = str(REPO_ROOT / "packaging" / "launcher" / "launcher.py")
ICON_WIN   = str(REPO_ROOT / "app" / "static" / "icon.ico") if \
             (REPO_ROOT / "app" / "static" / "icon.ico").exists() else None

# ---------------------------------------------------------------------------
# Analysis — tell PyInstaller what to include
# ---------------------------------------------------------------------------
a = Analysis(
    scripts=[LAUNCHER],          # Entry point: detect-or-bundle launcher

    pathex=[str(REPO_ROOT)],     # Repo root on sys.path so imports resolve

    # Hidden imports that PyInstaller's static analyser may miss.
    # Streamlit and several scientific packages use dynamic imports.
    hiddenimports=[
        "streamlit",
        "streamlit.runtime",
        "streamlit.runtime.scriptrunner",
        "streamlit.web.cli",
        "pyxnat",
        "pydicom",
        "cv2",           # opencv-python
        "openpyxl",
        "pandas",
        "numpy",
        "matplotlib",
        "certifi",
        "charset_normalizer",
        "packaging",     # the PyPI 'packaging' library (version parsing)
    ],

    # Data files to bundle alongside the executable.
    # Format: (source_path_or_glob, dest_folder_in_bundle)
    datas=[
        # App source — Streamlit pages, components, logic
        (str(REPO_ROOT / "app"),        "app"),
        # Phase 1 source library
        (str(REPO_ROOT / "src"),        "src"),
        # Streamlit's own static web assets (required for the embedded server)
        # PyInstaller's Streamlit hook usually handles this; listed here as
        # a safety net. Remove if the hook covers it to avoid duplication.
        # (str(Path(sys.prefix) / "lib" / "python3.11" / "site-packages" / "streamlit" / "static"), "streamlit/static"),
    ],

    # Binary dependencies (e.g. OpenCV native libs) — usually auto-detected.
    binaries=[],

    # Exclude large packages not needed at runtime to keep bundle size down.
    excludes=[
        "tkinter",
        "test",
        "distutils",
        "unittest",
        "xmlrpc",
    ],

    # Do NOT compress the PKG archive — keeps startup fast and makes
    # incremental rebuilds cheaper.
    compress=False,

    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    noarchive=False,
)

# ---------------------------------------------------------------------------
# PYZ — bytecode archive for pure-Python modules
# ---------------------------------------------------------------------------
pyz = PYZ(a.pure, a.zipped_data)

# ---------------------------------------------------------------------------
# EXE — the Windows executable wrapper
# ---------------------------------------------------------------------------
exe = EXE(
    pyz,
    a.scripts,
    [],                      # extra binaries appended to the exe (none here)
    exclude_binaries=True,   # Keep binaries in the onedir tree (not inside the exe)

    name="XNAT-Interact",
    icon=ICON_WIN,

    # console=True  → shows a terminal window (useful for debugging / CLI launch)
    # console=False → no terminal window; use for a pure GUI distribution once
    #                 Streamlit's browser-based UI is the primary surface.
    # For the ITS Software Center package, start with console=True so any
    # launch errors are visible to the user; flip to False after smoke-testing.
    console=True,

    # UAC manifest: do NOT request admin elevation — this app must run without it.
    uac_admin=False,
    uac_uiaccess=False,
)

# ---------------------------------------------------------------------------
# COLLECT — assemble the onedir distribution tree
# ---------------------------------------------------------------------------
# onedir is strongly preferred for the Software Center package:
#   - faster startup than onefile (no self-extraction step)
#   - easier for ITS to wrap in an MECM package
#   - simpler AV/app-control allowlisting
#
# To switch to a single-file EXE, remove COLLECT and change EXE's
# exclude_binaries to False, then add a.binaries, a.zipfiles, a.datas
# directly to the EXE() call.  onefile is NOT recommended for managed deploy.
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,       # Do not strip debug symbols on Windows
    upx=False,         # Do not UPX-compress; AV tools sometimes flag UPX-packed exes
    upx_exclude=[],
    name="XNAT-Interact",  # dist/XNAT-Interact/ is the deliverable directory
)

# =============================================================================
# POST-BUILD CHECKLIST (manual steps — see docs/SIGNING.md for detail)
# =============================================================================
# 1. Verify the build:
#      dist\XNAT-Interact\XNAT-Interact.exe --help   (or just double-click)
#
# 2. Sign the executable and all bundled DLLs (requires a code-signing cert):
#      See docs/SIGNING.md → Windows section
#
# 3. Verify the signature:
#      signtool verify /pa /v dist\XNAT-Interact\XNAT-Interact.exe
#
# 4. Hand dist\XNAT-Interact\ (as a ZIP or MSI wrapper) to ITS for MECM installer.
#      See docs/PACKAGING.md → Software Center (Primary) path.
# =============================================================================
