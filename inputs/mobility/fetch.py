"""
This file imports Google mobility data and saves it to disk as a CSV.
"""
from pathlib import Path

import pandas as pd
from settings import INPUT_DATA_PATH, Countries

GOOGLE_MOBILITY_URL = (
    "https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"
)

INPUT_DATA_PATH = Path(INPUT_DATA_PATH)

MOBILITY_DIRPATH = INPUT_DATA_PATH / "mobility"
MOBILITY_CSV_PATH = MOBILITY_DIRPATH / "Google_Mobility_Report.csv"

FB_MOVEMENT_2021 = MOBILITY_DIRPATH / "movement-range-2020.txt"
FB_MOVEMENT_2022 = MOBILITY_DIRPATH / "movement-range-2022.txt"


def fetch_mobility_data() -> None:
    df = pd.read_csv(GOOGLE_MOBILITY_URL)
    filter_mob = set(Countries.ISO2)
    df[df.country_region_code.isin(filter_mob)].to_csv(MOBILITY_CSV_PATH)

    filter_fb_mov = {"BTN"}
    for file in {FB_MOVEMENT_2021, FB_MOVEMENT_2022}:
        try:
            df = pd.read_csv(file)
        except:
            df = pd.read_csv(file, delimiter="\t")
        df[df.country.isin(filter_fb_mov)].to_csv(file)

    return None
