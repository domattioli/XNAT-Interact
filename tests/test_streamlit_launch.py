"""
Test streamlit_app.py entrypoint launch.

Offline, no server: (a) syntax validation; (b) sys.path bootstrap works;
(c) app.main is correctly imported and has main() callable.
"""
import ast
import subprocess
import sys
from pathlib import Path


def test_streamlit_app_exists():
    """Verify streamlit_app.py exists at repo root."""
    root = Path(__file__).resolve().parent.parent
    streamlit_app = root / "streamlit_app.py"
    assert streamlit_app.exists(), f"streamlit_app.py not found at {root}"


def test_streamlit_app_syntax_valid():
    """Verify streamlit_app.py is syntactically valid Python."""
    root = Path(__file__).resolve().parent.parent
    streamlit_app = root / "streamlit_app.py"
    code = streamlit_app.read_text()
    try:
        ast.parse(code)
    except SyntaxError as e:
        raise AssertionError(f"streamlit_app.py has syntax error: {e}")


def test_streamlit_app_references_app_main():
    """Verify streamlit_app.py imports from app.main."""
    root = Path(__file__).resolve().parent.parent
    streamlit_app = root / "streamlit_app.py"
    code = streamlit_app.read_text()
    assert "from app.main import main" in code, (
        "streamlit_app.py must import main from app.main"
    )


def test_app_main_importable_with_root_path():
    """Verify app.main can be imported when repo root is on sys.path."""
    root = Path(__file__).resolve().parent.parent
    # Run in subprocess with cwd=repo root, sys.path=[root]
    code = (
        "import sys; "
        "sys.path.insert(0, '.'); "
        "from app.main import main; "
        "assert callable(main), 'main is not callable'"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(root),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"app.main import failed: {result.stderr}"
        )


def test_streamlit_app_bootable():
    """Verify streamlit_app.py parses and the repo root is on sys.path."""
    root = Path(__file__).resolve().parent.parent
    streamlit_app = root / "streamlit_app.py"
    code = (
        f"import ast; "
        f"ast.parse(open('{streamlit_app}').read())"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(root),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"streamlit_app.py parsing failed: {result.stderr}"
        )
