
import os
import pathlib
from dotenv import load_dotenv
import time

load_dotenv()

EOD_API = os.getenv("EOD_API")

# --- guard API ticker early ---
if not EOD_API or not isinstance(EOD_API, str):
    raise RuntimeError(
        "one or more api kays not found")

CACHE_DIR = pathlib.Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

today = time.strftime("%Y-%m-%d")
START = '2020-01-01'  # global start date for all fetches

# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True