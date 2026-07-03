"""
Streamlit entrypoint for XNAT-Interact (root launcher).

Launch:
    streamlit run streamlit_app.py

This file solves two Streamlit issues when launched from app/main.py:
1. Streamlit adds the entrypoint's dir to sys.path, not the repo root, so
   relative imports like `from app import state` fail.
2. Streamlit auto-discovers pages at ./pages/ adjacent to the entrypoint,
   conflicting with app/pages/ and bypassing the app's custom router.

Solution: launch from the repo root. Streamlit looks for ./pages/ (does not
exist), so the custom router in app/main.py takes over.
"""
import sys
from pathlib import Path

# Insert repo root onto sys.path so `from app import ...` works
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.main import main  # noqa: E402

if __name__ == "__main__":
    main()
