
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

# Always create cache in the project root, regardless of where script is run from
PROJECT_ROOT = pathlib.Path(__file__).parent.resolve()
CACHE_DIR = PROJECT_ROOT / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# DATA DOWNLOAD PARAMS
params = {  'from': '2019-5-16', 
            'to': time.strftime("%Y-%m-%d"),
            'api_token': EOD_API,
            'max_age' : 12,   # hours
            'url': 'https://eodhd.com/api/eod/'
            }

# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True