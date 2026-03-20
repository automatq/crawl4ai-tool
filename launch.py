#!/usr/bin/env python3
"""
Lead Scraper — One-click launcher

Double-click this file (or run `python launch.py`) to:
  1. Install dependencies if needed
  2. Start the web server
  3. Open your browser automatically
"""

import os
import subprocess
import sys


def main():
    app_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(app_dir)

    venv_dir = os.path.join(app_dir, ".venv")
    requirements = os.path.join(app_dir, "requirements.txt")

    # Determine Python and pip paths
    if sys.platform == "win32":
        venv_python = os.path.join(venv_dir, "Scripts", "python.exe")
        venv_pip = os.path.join(venv_dir, "Scripts", "pip.exe")
    else:
        venv_python = os.path.join(venv_dir, "bin", "python")
        venv_pip = os.path.join(venv_dir, "bin", "pip")

    # Create venv if it doesn't exist
    if not os.path.exists(venv_python):
        print("Setting up virtual environment (first run)...")
        subprocess.check_call([sys.executable, "-m", "venv", venv_dir])

    # Install/update dependencies
    print("Checking dependencies...")
    subprocess.check_call(
        [venv_pip, "install", "-q", "-r", requirements],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Install playwright browsers if needed (required by crawl4ai)
    try:
        subprocess.check_call(
            [venv_python, "-m", "playwright", "install", "chromium"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass  # May already be installed

    # Launch the web app
    print("Starting Lead Scraper...")
    web_py = os.path.join(app_dir, "web.py")
    subprocess.call([venv_python, web_py])


if __name__ == "__main__":
    main()
