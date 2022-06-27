from utils.timer import Timer

from inputs.covid_mys.fetch import fetch_covid_mys_data
from inputs.covid_survey.fetch import fetch_covid_survey_data
from inputs.john_hopkins.fetch import fetch_john_hopkins_data
from inputs.mobility.fetch import fetch_mobility_data
from inputs.owid.fetch import fetch_owid_data


def fetch_input_data():
    """
    Fetch input data from external sources,
    which can then be used to build the input database.
    """
    with Timer("Fetching mobility data."):
        fetch_mobility_data()
    with Timer("Fetching COVID PHL data."):
        fetch_covid_mys_data()

    with Timer("Fetching OWID data."):
        fetch_owid_data()

    with Timer("Fetching Covid survey data."):
        fetch_covid_survey_data()

    with Timer("Fetching John Hopkins data."):
        fetch_john_hopkins_data()
