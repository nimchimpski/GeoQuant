
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


today = time.strftime("%Y-%m-%d")
START = '2020-01-01'  # global start date for all fetches

# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True

params = {  'from': START, 
            'to': today,
            'api_token': EOD_API    }
url = f'https://eodhd.com/api/eod/'

MAX_AGE = 24