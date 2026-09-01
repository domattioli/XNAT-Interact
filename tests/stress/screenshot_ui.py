"""
Drive the live Streamlit app via Playwright, screenshot every screen.

Prereqs (caller sets): a live XNAT at XNAT_SERVER_URL with a seeded demo project,
streamlit running on :8501 against the same env. Writes PNGs to docs/site/assets/ui/.

Not a pytest test — a one-shot demo-asset generator. Run:
    python tests/stress/screenshot_ui.py
"""
from __future__ import annotations

import sys
from pathlib import Path

OUT = Path("docs/site/assets/ui")
OUT.mkdir(parents=True, exist_ok=True)
import os
APP = os.environ.get("DEMO_APP_URL", "http://localhost:8501")


def _settle(pg, ms: int = 1400) -> None:
    """Streamlit reruns are async; wait for the run-status spinner to clear."""
    pg.wait_for_load_state("networkidle")
    pg.wait_for_timeout(ms)


def _shot(pg, name: str) -> None:
    pg.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
    print(f"  shot {name}.png")


def _click_button(pg, label: str) -> bool:
    """Click a Streamlit st.button by its visible label. Returns True if found."""
    btn = pg.get_by_role("button", name=label, exact=True)
    if btn.count() == 0:
        btn = pg.get_by_role("button", name=label)
    if btn.count() == 0:
        print(f"  (button '{label}' not found)")
        return False
    btn.first.click()
    _settle(pg)
    return True


def main() -> int:
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": 1180, "height": 1500}, device_scale_factor=2)
        pg.goto(APP, timeout=60000)
        _settle(pg, 2500)

        # 1. Login screen (unauthenticated)
        _shot(pg, "01_login")

        # Expand the access checklist for a richer login shot
        exp = pg.get_by_text("Need access?", exact=False)
        if exp.count():
            try:
                exp.first.click()
                _settle(pg)
                _shot(pg, "02_login_checklist")
            except Exception as e:
                print(f"  (checklist expand skipped: {e})")

        # Fill credentials + connect
        for label in ("Username", "Hawk", "User"):
            f = pg.get_by_label(label, exact=False)
            if f.count():
                f.first.fill("admin")
                break
        pwd = pg.locator("input[type=password]")
        if pwd.count():
            pwd.first.fill("admin")
        _settle(pg, 600)
        if not _click_button(pg, "Connect"):
            _click_button(pg, "Log In")
        _settle(pg, 2500)
        _shot(pg, "03_after_login")

        # Walk each authenticated page via the sidebar nav buttons
        pages = [
            ("Browse", "04_browse"),
            ("Upload", "05_upload"),
            ("Batch Upload", "06_batch"),
            ("Download", "07_download"),
            ("Annotations", "08_annotations"),
            ("Learn (CLI ref)", "09_learn"),
            ("Onboarding / Access", "10_onboarding"),
        ]
        for label, name in pages:
            if _click_button(pg, label):
                _shot(pg, name)

        b.close()
    print("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
