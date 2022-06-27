import logging

from db import Database
from db.database import ParquetDatabase, get_database
from settings import INPUT_DATA_PATH
from utils.timer import Timer

from inputs.covid_mys.fetch import fetch_covid_mys_data
from inputs.covid_mys.preprocess import preprocess_covid_mys
from inputs.covid_survey.fetch import fetch_covid_survey_data
from inputs.covid_survey.preprocess import preprocess_covid_survey
from inputs.john_hopkins.fetch import fetch_john_hopkins_data
from inputs.mobility.fetch import fetch_mobility_data
from inputs.mobility.preprocess import preprocess_mobility
from inputs.owid.fetch import fetch_owid_data

logger = logging.getLogger(__name__)

_input_db = None

INPUT_DB_PATH = INPUT_DATA_PATH / "db"


def fetch_input_data():
    """
    Fetch input data from external sources,
    which can then be used to build the input database.
    """
    with Timer("Fetching mobility data."):
        fetch_mobility_data()

    with Timer("Fetching COVID MYS data."):
        fetch_covid_mys_data()

    with Timer("Fetching OWID data."):
        fetch_owid_data()

    with Timer("Fetching Covid survey data."):
        fetch_covid_survey_data()

    with Timer("Fetching John Hopkins data."):
        fetch_john_hopkins_data()


def get_input_db():
    global _input_db
    if _input_db:
        return _input_db
    else:
        _input_db = build_input_database()
        return _input_db


def build_input_database(rebuild: bool = False):
    """
    Builds the input database from scratch.
    If force is True, build the database from scratch and ignore any previous hashes.
    If force is False, do not build if it already exists,
    and crash if the built database hash does not match.

    If rebuild is True, then we force rebuild the database, but we don't write a new hash.

    Returns a Database, representing the input database.
    """
    if INPUT_DATA_PATH.exists() and not rebuild:
        input_db = get_database(INPUT_DB_PATH)
    else:
        logger.info("Building a new database.")
        input_db = ParquetDatabase(INPUT_DB_PATH)

        with Timer("Deleting all existing data."):
            input_db.delete_everything()

        with Timer("Ingesting COVID MYS data."):
            preprocess_covid_mys(input_db)

        with Timer("Ingesting COVID survey data"):
            preprocess_covid_survey(input_db)

    return input_db
