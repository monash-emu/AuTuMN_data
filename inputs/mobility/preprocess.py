import os

import pandas as pd
from db import Database
from settings import INPUT_DATA_PATH
from settings.constants import COVID_BASE_DATETIME
from utils.utils import create_date_index

from .fetch import (
    FB_MOVEMENT_2021,
    FB_MOVEMENT_2022,
    MOBILITY_CSV_PATH,
    MOBILITY_DIR_PATH,
)

NAN = float("nan")
MOBILITY_SUFFIX = "_percent_change_from_baseline"

DHHS_LGA_TO_CLUSTER = (
    MOBILITY_DIR_PATH / "LGA to Cluster mapping dictionary with proportions.csv"
)

COUNTRY_NAME_ISO3_MAP = {
    "Bolivia": "BOL",
    "The Bahamas": "BHS",
    "Côte d'Ivoire": "CIV",
    "Cape Verde": "CPV",
    "Hong Kong": "HKG",
    "South Korea": "KOR",
    "Laos": "LAO",
    "Moldova": "MDA",
    "Myanmar (Burma)": "MMR",
    "Russia": "RUS",
    "Taiwan": "TWN",
    "Tanzania": "TZA",
    "United States": "USA",
    "Venezuela": "VEN",
    "Vietnam": "VNM",
}


def preprocess_mobility(input_db: Database, country_df):
    """
    Read Google Mobility data from CSV into input database
    """
    mob_df = pd.read_csv(MOBILITY_CSV_PATH)

    # Drop all sub-region 2 data, too detailed.
    major_region_mask = mob_df["sub_region_2"].isnull() & mob_df["metro_area"].isnull()
    davao_mask = mob_df.metro_area == "Davao City Metropolitan Area"
    mob_df = mob_df[major_region_mask | davao_mask].copy()

    # These two regions are the same
    mob_df.loc[
        (mob_df.sub_region_1 == "National Capital Region"), "sub_region_1"
    ] = "Metro Manila"
    mob_df.loc[
        (mob_df.metro_area == "Davao City Metropolitan Area"), "sub_region_1"
    ] = "Davao City"
    mob_df.loc[
        (mob_df.sub_region_1 == "Federal Territory of Kuala Lumpur"), "sub_region_1"
    ] = "Kuala Lumpur"

    # Drop all rows that have NA values in 1 or more mobility columns.
    mob_cols = [c for c in mob_df.columns if c.endswith(MOBILITY_SUFFIX)]
    mask = False
    for c in mob_cols:
        mask = mask | mob_df[c].isnull()

    mob_df = mob_df[~mask].copy()
    for c in mob_cols:
        # Convert percent values to decimal: 1.0 being no change.
        mob_df[c] = mob_df[c].apply(lambda x: 1 + x / 100)

    # Drop unused columns, rename kept columns
    cols_to_keep = [*mob_cols, "country_region", "sub_region_1", "date"]
    cols_to_drop = [c for c in mob_df.columns if not c in cols_to_keep]
    mob_df = mob_df.drop(columns=cols_to_drop)
    mob_col_rename = {c: c.replace(MOBILITY_SUFFIX, "") for c in mob_cols}
    mob_df.rename(columns={**mob_col_rename, "sub_region_1": "region"}, inplace=True)

    # Convert countries to ISO3
    countries = mob_df["country_region"].unique().tolist()
    iso3s = {c: get_iso3(c, country_df) for c in countries}
    iso3_series = mob_df["country_region"].apply(lambda c: iso3s[c])
    mob_df.insert(0, "iso3", iso3_series)
    mob_df = mob_df.drop(columns=["country_region"])

    mob_df = mob_df.sort_values(["iso3", "region", "date"])
    input_db.dump_df("mobility", mob_df)

    # Facebook movement data
    df_list = []
    iso_filter = {"AUS", "PHL", "MYS", "VNM", "LKA", "IDN", "MYN", "BGD", "BTN"}
    for file in {FB_MOVEMENT_2021, FB_MOVEMENT_2022}:
        df = pd.read_csv(file)
        df_list.append(df)

    df = pd.concat(df_list)
    df = df[df["country"].isin(iso_filter)]
    df = df.sort_values(["country", "ds", "polygon_id"]).reset_index(drop=True)
    df = create_date_index(COVID_BASE_DATETIME, df, "ds")
    input_db.dump_df("movement", df)


def get_iso3(country_name: str, country_df):
    try:
        return country_df[country_df["country"] == country_name]["iso3"].iloc[0]
    except IndexError:
        return COUNTRY_NAME_ISO3_MAP[country_name]
