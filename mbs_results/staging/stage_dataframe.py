import pandas as pd

from mbs_results.staging.convert_ni_uk import convert_ni_uk
from mbs_results.staging.create_missing_questions import create_missing_questions
from mbs_results.staging.dfs_from_spp import dfs_from_spp
from mbs_results.utilities.utils import read_colon_separated_file


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
    contributors, responses = dfs_from_spp(
        config["folder_path"] + config["mbs_file_name"],
        config["platform"],
        config["bucket"],
    )
    # contributors = enforce_datatypes(df, **config) #BROKEN
    # responses = enforce_datatypes(df, **config) #BROKEN
    finalsel = read_colon_separated_file("sample_path", config["sample_column_names"])
    contributors_final_sel = pd.merge(
        left=contributors, right=finalsel, left_on="", right_on=""
    )
    mapper = create_mapper()  # Needs to be defined
    responses_with_missing = create_missing_questions(
        contributors_df=contributors_final_sel,
        responses_df=responses,
        reference=config["reference"],
        period=config["period"],
        formid=config["form_id"],
        question_no=config["question_no"],
        mapper=mapper,
    )
    df = responses_with_missing.merge(
        contributors_final_sel, on=[config["reference"], config["period"]]
    )
    df = convert_ni_uk(df, "cellnumber")

    return df
