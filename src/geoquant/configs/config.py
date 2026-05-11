
import os
import pathlib
import yaml
import pandas as pd
import logging
from dotenv import load_dotenv
import time
from matplotlib import ticker
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Always load .env from the config directory, regardless of CWD
from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

STOOQ_API = os.environ.get("STOOQ_API")
logger.debug(f"STOOQ_API: {STOOQ_API}")


CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"
with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

# Always create cache in the project root, regardless of where script is run from
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]
CACHE_DIR =  PROJECT_ROOT / "cache"
CACHE_DIR.mkdir(exist_ok=True)

data_params = {
    'start': pd.to_datetime(config["START"]),
    'datasource': config["DATASOURCE"],
    'max_age': config["MAX_AGE"],
    'end': pd.to_datetime(config.get("END") or time.strftime("%Y-%m-%d"))
}
# Module-level debug flag (no new function args). Set RISK_DEBUG=1 in env to enable verbose diagnostics.
DEBUG = True