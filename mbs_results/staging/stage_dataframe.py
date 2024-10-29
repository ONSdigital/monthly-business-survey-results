import warnings

import pandas as pd

# from mbs_results.staging.convert_ni_uk import convert_ni_uk
from mbs_results.staging.create_missing_questions import create_missing_questions
from mbs_results.staging.data_cleaning import enforce_datatypes  # run_live_or_frozen
from mbs_results.staging.dfs_from_spp import dfs_from_spp
from mbs_results.utilities.utils import read_colon_separated_file


def create_form_type_spp_column(
    contributors: pd.DataFrame, config: dict
) -> pd.DataFrame:
    idbr_to_spp_mapping = config["idbr_to_spp"]
    contributors["form_type_spp"] = contributors[config["form_id"]].map(
        idbr_to_spp_mapping
    )
    return contributors


def create_mapper() -> dict:
    """
    placeholder function to create question and form mapping dict

    Returns
    -------
    dict
        dictionary containing question numbers and form id.
    """
    mapper = {
        9: [40, 49],
        10: [110],
        11: [40, 49, 90],
        12: [40],
        13: [46, 47],
        14: [42, 43],
        15: [40],
        16: [40],
    }
    warnings.warn("create_mapper needs to be fully defined")
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

    print("Staging started")
    period = config["period"]
    reference = config["reference"]

    contributors, responses = dfs_from_spp(
        config["folder_path"] + config["mbs_file_name"],
        config["platform"],
        config["bucket"],
    )
    # TODO filter responses and contributors df to columns in config
    contributors = contributors[config["contributors_keep_cols"].keys()]
    contributors = enforce_datatypes(
        contributors, keep_columns=config["contributors_keep_cols"], **config
    )

    responses = responses[config["responses_keep_cols"].keys()]
    responses = enforce_datatypes(
        responses, keep_columns=config["responses_keep_cols"], **config
    )

    finalsel = read_colon_separated_file(
        config["sample_path"], config["sample_column_names"], period=config[period]
    )

    finalsel = finalsel[config["finalsel_keep_cols"].keys()]
    finalsel = enforce_datatypes(
        finalsel, keep_columns=config["finalsel_keep_cols"], **config
    )
    # Filter contributors files here to temp fix this overlap

    contributors = pd.merge(
        left=contributors,
        right=finalsel,
        on=[period, reference],
        suffixes=["_spp", "_finalsel"],
    )
    warnings.warn("Duplicate columns are created in this join, need to fix this")
    # TODO map on SPP form type
    #
    contributors = create_form_type_spp_column(contributors, config)
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

    df = responses_with_missing.merge(
        contributors, on=[reference, period], suffixes=["_res", "_con"]
    )
    print()
    # Add run live or frozen
    # df = run_live_or_frozen(df, ...)
    print("Staging Completed")

    return df


if __name__ == "__main__":
    from mbs_results.imputation.impute import impute
    from mbs_results.utilities.inputs import load_config

    config = load_config()
    df = stage_dataframe(config)
    # print(df[["formtype","form_type_spp"]])
    impute(df, config)
    # filter_col_spp = [col for col in df if col.endswith("_res")]
    # filter_col_finalsel = [col for col in df if col.endswith("_con")]
    # print(df.head())
    # for i in filter_col_spp:
    #     col_start = i.split("_")[0]
    #     print(col_start, df[i].equals(df[col_start + "_con"]))
