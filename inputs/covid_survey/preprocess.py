import pandas as pd
import os

from db import Database
from utils.utils import create_date_index

from settings import INPUT_DATA_PATH, COVID_BASE_DATETIME

from .fetch import COVID_SURVEY_PATH

CSV_FILES = [
    INPUT_DATA_PATH / "covid_survey" / file for file in COVID_SURVEY_PATH.glob('*')
]


MASKS = [file for file in CSV_FILES if "mask_" in file.name]
AVOID_CONTACT = [file for file in CSV_FILES if "avoid_contact" in file.name]

def preproc_csv(files):
    df = pd.concat(map(pd.read_csv, files))
    df.loc[:, "survey_date"] = pd.to_datetime(
        df["survey_date"], format="%Y%m%d", infer_datetime_format=False
    )

    create_date_index(COVID_BASE_DATETIME, df, "survey_date")
    return df


def preprocess_covid_survey(input_db: Database):
    df = get_mask()
    input_db.dump_df("survey_mask", df)

    df = get_avoid_contact()
    input_db.dump_df("survey_avoid_contact", df)


def get_mask():
    df = preproc_csv(MASKS)

    df = df[
        [
            "iso_code",
            "region",
            "date",
            "date_index",
            "sample_size",
            "percent_mc",
            "mc_se",
            "percent_mc_unw",
            "mc_se_unw",
        ]
    ]

    return df

def get_avoid_contact():
    df = preproc_csv(AVOID_CONTACT)
    df = df[
        [
            "iso_code",
            "region",
            "date",
            "date_index",
            "sample_size",
            "pct_avoid_contact",
            "avoid_contact_se",
            "pct_avoid_contact_unw",
            "avoid_contact_se_unw",
        ]
    ]

    return df
