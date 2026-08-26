"""Headless smoke test: run every dashboard page via streamlit AppTest, executing
its real queries against prod gold. Fails loudly on any page exception."""

import sys
from pathlib import Path

from streamlit.testing.v1 import AppTest

PAGES = [
    "Home.py",
    "pages/2_Delay_Map.py",
    "pages/3_Route_Explorer.py",
    "pages/4_Live_Map.py",
    "pages/5_Ops.py",
]

failed = False
for page in PAGES:
    script = Path(__file__).resolve().parents[1] / "dashboard" / "app" / page
    at = AppTest.from_file(str(script), default_timeout=120)
    at.run()
    if at.exception:
        failed = True
        print(f"FAIL {page}: {at.exception[0].message}")
    else:
        print(f"OK   {page}  (markdown={len(at.markdown)}, dataframes={len(at.dataframe)})")

sys.exit(1 if failed else 0)
