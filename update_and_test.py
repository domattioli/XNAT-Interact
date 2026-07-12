"""
update_and_test.py — Pull the latest code and run the test suite.

Run this at the start of each session to make sure your local copy is
up to date and that all tests pass before you begin work.
"""
import subprocess
import pkg_resources
import importlib
import sys
import os
from typing import Optional, Tuple
from pathlib import Path


def _detect_default_branch() -> str:
    """
    Return the default remote branch name (e.g. 'main' or 'master').

    Tries `git symbolic-ref refs/remotes/origin/HEAD` first; falls back to
    'main' if that ref is not set (common on freshly cloned repos).
    """
    try:
        result = subprocess.run(
            ['git', 'symbolic-ref', '--short', 'refs/remotes/origin/HEAD'],
            capture_output=True, text=True, check=True
        )
        # Returns something like 'origin/main' — strip the 'origin/' prefix.
        ref = result.stdout.strip()
        return ref.split('/', 1)[-1] if '/' in ref else ref
    except subprocess.CalledProcessError:
        pass

    # Fallback: ask git which branches exist and prefer 'main' over 'master'.
    try:
        result = subprocess.run(
            ['git', 'branch', '-r'],
            capture_output=True, text=True, check=True
        )
        remote_branches = result.stdout
        if 'origin/main' in remote_branches:
            return 'main'
        if 'origin/master' in remote_branches:
            return 'master'
    except subprocess.CalledProcessError:
        pass

    return 'main'


def pull_latest(branch: Optional[str] = None) -> str:
    """
    Fetch and fast-forward to the latest commit on the default remote branch.

    Uses `git pull --ff-only` rather than `git reset --hard` so that any
    uncommitted local changes are NOT silently discarded.  If there are
    conflicting local changes the pull will fail with an informative message
    instead of destroying work.

    Returns the branch name that was pulled.
    """
    if branch is None:
        branch = _detect_default_branch()
    subprocess.run(['git', 'fetch', 'origin'], check=True)
    subprocess.run(['git', 'pull', '--ff-only', 'origin', branch], check=True)
    return branch


def check_that_virtualenv_activated() -> bool:
    return 'VIRTUAL_ENV' in os.environ


def _parse_requirement_line(line: str) -> Optional[str]:
    """
    Return the installable requirement spec on a requirements.txt line, or None.

    Strips inline ``# ...`` comments and skips blank lines, full-line comments,
    and pip option/include lines (``-r``, ``-e``, ``--hash=``, ...).  Lets
    requirements.txt carry explanatory comments (the librarian's single source
    of truth) without breaking the installer.  See issue #10.
    """
    spec = line.split('#', 1)[0].strip()
    if not spec:
        return None
    if spec.startswith('-'):        # -r other.txt, -e ., --hash=..., etc.
        return None
    return spec


def _dist_name(requirement: str) -> str:
    """
    Reduce a requirement spec to its bare distribution name.

    ``SQLAlchemy>=2.0`` -> ``SQLAlchemy``; ``psycopg[binary]`` -> ``psycopg``;
    ``package==1.2; python_version<'3.10'`` -> ``package``.
    """
    name = requirement.split(';', 1)[0]
    for sep in ('===', '==', '>=', '<=', '~=', '!=', '>', '<'):
        name = name.split(sep, 1)[0]
    name = name.split('[', 1)[0]
    return name.strip()


# Distributions whose import name can't be derived by normalization and which
# may not expose an importlib.metadata mapping.  Last-resort fallback only; the
# metadata lookup below handles the general case automatically.
_IMPORT_NAME_FALLBACKS = {
    'opencv-python': 'cv2',
    'matplotlib-inline': 'matplotlib_inline',
    'charset-normalizer': 'charset_normalizer',
    'fonttools': 'fontTools',
    'python-dateutil': 'dateutil',
    'pillow': 'PIL',
    'pyyaml': 'yaml',
    'beautifulsoup4': 'bs4',
    'importlib-resources': 'importlib_resources',
}


def _distribution_import_map() -> dict:
    """Map lower-cased distribution name -> list of top-level import modules."""
    try:
        from importlib.metadata import packages_distributions
    except Exception:
        return {}
    mapping: dict = {}
    for module, dists in packages_distributions().items():
        if module.startswith('_'):
            continue
        for dist in dists:
            mapping.setdefault(dist.lower(), []).append(module)
    return mapping


def resolve_import_name(dist_name: str, dist_map: Optional[dict] = None) -> str:
    """
    Resolve the importable module name for an installed distribution.

    Prefers the authoritative ``importlib.metadata`` distribution->module map so
    requirements.txt stays the ONLY file the librarian edits: a package whose
    import name differs from its distribution name (``opencv-python`` -> ``cv2``,
    ``SQLAlchemy`` -> ``sqlalchemy``) resolves automatically once installed, with
    no per-package special-case needed here.  See issue #10.
    """
    if dist_map is None:
        dist_map = _distribution_import_map()
    modules = dist_map.get(dist_name.lower())
    if modules:
        normalized = dist_name.lower().replace('-', '_')
        for module in modules:
            if module.lower() == normalized:
                return module
        return sorted(modules)[0]
    if dist_name.lower() in _IMPORT_NAME_FALLBACKS:
        return _IMPORT_NAME_FALLBACKS[dist_name.lower()]
    return dist_name.replace('-', '_')


def check_and_install_requirements(requirements_file: Path) -> None:
    with open(requirements_file, 'r') as file:
        lines = file.readlines()

    for line in lines:
        requirement = _parse_requirement_line(line)
        if requirement is None:
            continue
        try:
            pkg_resources.require(requirement)
            print(f"\t\t'{requirement}' is installed.")
        except pkg_resources.DistributionNotFound:
            print(f"\t\t'{requirement}' is NOT installed. Installing...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", requirement])
        except pkg_resources.VersionConflict as e:
            print(f"\t\t'{requirement}' has a version conflict: {e}.\n\t\t-- Installing correct version...")
            subprocess.check_call([sys.executable, "-m", "pip", "install", requirement])


def import_all_necessary_modules(requirements_file: Path) -> None:
    print(f"\t...Checking installation of each required library...")
    check_and_install_requirements(requirements_file)

    with open(requirements_file, 'r') as file:
        lines = file.readlines()

    print(f"\n\t...Checking import of each installed library...")
    dist_map = _distribution_import_map()
    for line in lines:
        requirement = _parse_requirement_line(line)
        if requirement is None:
            continue
        module_name = resolve_import_name(_dist_name(requirement), dist_map)
        try:
            importlib.import_module(module_name)
            print(f"\t\tSuccessfully imported {module_name}")
        except ImportError as e:
            print(f"\t\tError importing {module_name}: {e}")


def run_tests() -> int:
    """
    Run the test suite with pytest.

    Returns the pytest exit code (0 = all tests passed).
    """
    print(f"\n\t...Running test suite with pytest...")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-q"],
        check=False  # Don't raise — we handle the return code ourselves.
    )
    return result.returncode


def main() -> None:
    """
    Pull the latest code and run the test suite to verify your installation.
    """
    print(f'\n...Ensuring that XNAT-Interact installation is correct and up-to-date...\n')

    # --- Step 1: Virtual environment check ---
    try:
        if not check_that_virtualenv_activated():
            raise RuntimeError('Virtual environment not activated.')
        print(f'--- Virtual environment is active...')
    except Exception:
        print(f'ERROR\t-- Activate your virtual environment before running this script!\n')
        sys.exit(1)

    # --- Step 2: Check and install requirements ---
    this_directory = Path(__file__).parent
    requirements_file = this_directory / 'requirements.txt'
    try:
        import_all_necessary_modules(requirements_file=requirements_file)
        print(f'--- All necessary modules are available...\n')
    except Exception as e:
        print(f'ERROR\t-- Could not verify required modules.\n\tError: {e}')
        sys.exit(1)

    # --- Step 3: Pull latest code ---
    print(f'--- Pulling latest changes from the remote repository...')
    print(f'\tNOTE: This uses git pull --ff-only.  If you have uncommitted local')
    print(f'\tchanges that conflict, the pull will stop safely without discarding them.')
    try:
        branch = pull_latest()
        print(f'SUCCESS\t-- Codebase is up-to-date with the {branch!r} branch...\n')
    except subprocess.CalledProcessError as e:
        print(f'FAILURE\t-- Could not pull the latest code.\n\tError: {e}')
        print(f'\tIf you have local changes, commit or stash them first.')
        print(f'\tContact the Data Librarian if this keeps failing.')
        sys.exit(1)

    # --- Step 4: Run the test suite ---
    exit_code = run_tests()
    if exit_code == 0:
        print(f'\nSUCCESS\t-- All tests passed.  You may now proceed to main.py\n')
        sys.exit(0)
    else:
        print(f'\nFAILURE\t-- Some tests failed (pytest exit code {exit_code}).')
        print(f'\tDo not upload data until the test failures are resolved.')
        print(f'\tContact the Data Librarian if you need help.\n')
        sys.exit(exit_code)


if __name__ == '__main__':
    main()
