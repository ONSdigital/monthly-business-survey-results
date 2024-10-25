import warnings

import pandas as pd

from mbs_results.staging.convert_ni_uk import convert_ni_uk
from mbs_results.staging.create_missing_questions import create_missing_questions
from mbs_results.staging.dfs_from_spp import dfs_from_spp
from mbs_results.utilities.utils import (
    convert_column_to_datetime,
    read_colon_separated_file,
)


def create_mapper() -> dict:
    """
    placeholder function to create question and form mapping dict

    Returns
    -------
    dict
        dictionary containing question numebers and form id
    """
    mapper = {9: [40, 49], 10: [110]}
    warnings.warn("create_mapper is a placeholder function and needs to be defined")
    return mapper


def stage_dataframe(config: dict) -> pd.DataFrame:
    """
    wrapper function to stage and pre process the dataframe, ready to be passed onto the
    imputation wrapper (impute)

    Parameters
    ----------
    config : dict
        config containing paths and column names and file paths

    Returns
    -------
    _type_
        Combined dataframe containing response and contributor data. Missing questions
        have been created, data types enforced***. NI cell number have been converted
        to uk.

        *** current functionality broken but with refactoring this will be implemented
        Mapping function needs to be defined but is used in other functions
    """

    warnings.warn("Issues with memory when joining datasets occur in this function")
    print("Staging started")
    period = config["period"]
    reference = config["reference"]

    contributors, responses = dfs_from_spp(
        config["folder_path"] + config["mbs_file_name"],
        config["platform"],
        config["bucket"],
    )
    # contributors = enforce_datatypes(df, **config) #BROKEN
    # responses = enforce_datatypes(df, **config) #BROKEN
    finalsel = read_colon_separated_file(
        config["sample_path"], config["sample_column_names"], period=config[period]
    )

    # Temp convert d types while enforce datatypes is broken
    finalsel[period] = convert_column_to_datetime(finalsel[period])
    finalsel[reference] = finalsel[period].astype(str)
    contributors[period] = convert_column_to_datetime(contributors[period])
    contributors[reference] = contributors[period].astype(str)
    responses[period] = convert_column_to_datetime(responses[period])
    responses[reference] = responses[period].astype(str)

    # MEMORY ISSUES WITH JOIN?
    contributors = pd.merge(left=contributors, right=finalsel, on=[period, reference])

    mapper = create_mapper()  # Needs to be defined
    responses_with_missing = create_missing_questions(
        contributors_df=contributors,
        responses_df=responses,
        reference=reference,
        period=period,
        formid=config["form_id"],
        question_no=config["question_no"],
        mapper=mapper,
    )
    df = responses_with_missing.merge(contributors, on=[reference, period])
    df = convert_ni_uk(df, "cellnumber")
    print("Staging Completed")

    return df
