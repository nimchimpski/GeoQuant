
import os
import pathlib
from dotenv import load_dotenv
import time

from matplotlib import ticker
from dataclasses import dataclass

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
# params = {  'start': '2016-01-01', 
#             'end': time.strftime("%Y-%m-%d"),
#             'datasource': 'stooq',
#             'api_token': EOD_API,
#             'max_age' : 0,   # hours
# }
# STOOQ
params = {  'start': '2016-01-01', 
            'end': time.strftime("%Y%m%d"),
            'datasource': 'stooq',
}

# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True