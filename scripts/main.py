"""
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
File:    run.py
Author:  Frank Runfola
Date:    11/1/2025
-------------------------------------------------------------------------------
Description:
  One-command runner for the tiny Medallion pipeline.
  This is OPTIONAL — you can run the notebooks instead.
  But scripts are nice for CI or quick demos.
-------------------------------------------------------------------------------
Run from project root:
   cd "/c/Users/fr54938/Documents/DE/earthquake"
   python -m earthquake
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import annotations

from earthquake.app import main

if __name__ == "__main__":
    exit_code = main()  # run src/earthquake/app.py
    raise SystemExit(exit_code)
