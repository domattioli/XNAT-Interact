"""
Streamlit entrypoint for the Guided (novice-safe) XNAT-Interact UI.

Launch:
    streamlit run streamlit_guided.py

This follows the same sys.path fix as streamlit_app.py to enable
relative imports and bypass Streamlit's auto-discovery of pages/.
"""
import sys
from pathlib import Path

# Insert repo root onto sys.path so `from app import ...` works
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from app.guided.main import main  # noqa: E402

if __name__ == "__main__":
    main()
