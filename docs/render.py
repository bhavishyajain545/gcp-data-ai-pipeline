"""Render docs/architecture.html to docs/architecture.png (python docs/render.py)."""

from pathlib import Path

from playwright.sync_api import sync_playwright

HERE = Path(__file__).resolve().parent

with sync_playwright() as pw:
    browser = pw.chromium.launch()
    page = browser.new_page(viewport={"width": 1400, "height": 800}, device_scale_factor=2)
    page.goto((HERE / "architecture.html").as_uri())
    page.locator(".canvas").screenshot(path=str(HERE / "architecture.png"))
    browser.close()
print("wrote", HERE / "architecture.png")
