
import os
import pathlib
import yaml

from dotenv import load_dotenv
import time
from matplotlib import ticker
from dataclasses import dataclass

# Always load .env from the config directory, regardless of CWD
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

STOOQ_API = os.environ.get("STOOQ_API")

print(f"STOOQ_API: {STOOQ_API}")


CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# Always create cache in the project root, regardless of where script is run from
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
CACHE_DIR =  pathlib.Path(__file__).parent / "cache"
CACHE_DIR.mkdir(exist_ok=True)


START = config["START"]
DATASOURCE = config["DATASOURCE"]
MAX_AGE = config["MAX_AGE"]
END = config.get("END", time.strftime("%Y-%m-%d"))

data_params = {
    'start': START,
    'datasource': DATASOURCE,
    'max_age': MAX_AGE,
    'end': END

}
# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True