#!/usr/bin/env python3
"""
Build Lead Scraper as a standalone desktop app.

Prerequisites:
  pip install pyinstaller

Usage:
  python build.py

Output:
  dist/LeadScraper.app  (macOS)
  dist/LeadScraper.exe  (Windows)
"""

import os
import subprocess
import sys


def main():
    app_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(app_dir)

    # Check PyInstaller is installed
    try:
        import PyInstaller  # noqa: F401
    except ImportError:
        print("Installing PyInstaller...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pyinstaller"])

    print("Building Lead Scraper desktop app...")

    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--name", "LeadScraper",
        "--onefile",
        "--windowed" if sys.platform == "darwin" else "--noconsole",
        "--add-data", f"templates{os.pathsep}templates",
        "--add-data", f"static{os.pathsep}static",
        "--hidden-import", "flask",
        "--hidden-import", "crawl4ai",
        "web.py",
    ]

    subprocess.check_call(cmd)

    if sys.platform == "darwin":
        print("\nBuild complete: dist/LeadScraper.app")
        print("You can drag this to /Applications or share it with your team.")
    elif sys.platform == "win32":
        print("\nBuild complete: dist/LeadScraper.exe")
        print("Share this .exe with your team — no Python install needed.")
    else:
        print("\nBuild complete: dist/LeadScraper")

    print("\nNote: Recipients will still need Chromium for crawl4ai.")
    print("On first run, it will be downloaded automatically.")


if __name__ == "__main__":
    main()
