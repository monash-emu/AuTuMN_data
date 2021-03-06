"""
This file imports OWID data and saves it to disk as a CSV.
"""
from pathlib import Path

import pandas as pd
from settings import INPUT_DATA_PATH, Countries

INPUT_DATA_PATH = Path(INPUT_DATA_PATH)

OWID_URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
OWID_DIRPATH = INPUT_DATA_PATH / "owid"
OWID_CSV_PATH = OWID_DIRPATH / "owid-covid-data.csv"

# Keep required countries due to large CSV filesize.

filter_iso3 = set(Countries.ISO3)


def fetch_owid_data() -> None:
    df = pd.read_csv(OWID_URL)
    df[df.iso_code.isin(filter_iso3)].to_csv(OWID_CSV_PATH)

    return None
