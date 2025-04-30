import logging
import os
import warnings

import pandas as pd

from mbs_results.staging.back_data import append_back_data
from mbs_results.staging.create_missing_questions import (
    create_mapper,
    create_missing_questions,
)
from mbs_results.staging.data_cleaning import (
    convert_annual_thousands,
    convert_cell_number,
    create_imputation_class,
    enforce_datatypes,
    filter_out_questions,
    run_live_or_frozen,
)
from mbs_results.staging.dfs_from_spp import get_dfs_from_spp
from mbs_results.utilities.constrains import constrain
from mbs_results.utilities.file_selector import find_files
from mbs_results.utilities.inputs import read_colon_separated_file
from mbs_results.utilities.utils import (
    convert_column_to_datetime,
    get_snapshot_alternate_path,
)

logger = logging.getLogger(__name__)


def create_form_type_spp_column(
    contributors: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    maps IDBR form types to SPP and creates column named "form_type_spp"

    Parameters
    ----------
    contributors : pd.DataFrame
        contributors dataframe from JSON snapshot
    config : dict
        main pipeline config containing "idbr_to_spp" mapping

    Returns
    -------
    pd.DataFrame
        contributors dataframe with "form_type_spp" column added
    """
    idbr_to_spp_mapping = config["idbr_to_spp"]
    contributors[config["form_id_spp"]] = contributors[config["form_id_idbr"]].map(
        idbr_to_spp_mapping
    )
    return contributors


def read_and_combine_colon_sep_files(config: dict) -> pd.DataFrame:
    """
    reads in and combined colon separated files from the specified folder path

    Parameters
    ----------
    folder_path : str
        folder path containing the colon separated files
    column_names : list
        list of column names in colon separated file
    config : dict
        main pipeline config containing period column name

    Returns
    -------
    pd.DataFrame
        combined colon separated files returned as one dataframe.
    """
    sample_files = find_files(
        file_path=config["folder_path"],
        file_prefix=config["sample_prefix"],
        current_period=config["current_period"],
        revision_window=config["revision_window"],
        config=config,
    )
    df = pd.concat(
        [
            read_colon_separated_file(
                filepath=f,
                column_names=config["sample_column_names"],
                keep_columns=config["finalsel_keep_cols"],
                period=config["period"],
                import_platform=config["platform"],
                bucket_name=config["bucket"],
            )
            for f in sample_files
        ],
        ignore_index=True,
    )
    return df


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
    pd.DataFrame
        Combined dataframe containing response and contributor data. Missing questions
        have been created, data types enforced. NI cell number have been converted
        to uk.
    """

    print("Staging started")
    period = config["period"]
    reference = config["reference"]

    snapshot_file_path = get_snapshot_alternate_path(config)

    contributors, responses = get_dfs_from_spp(
        snapshot_file_path + config["mbs_file_name"],
        config["platform"],
        config["bucket"],
    )

    # Filter columns and set data types
    contributors = contributors[config["contributors_keep_cols"]]
    contributors = enforce_datatypes(
        contributors, keep_columns=config["contributors_keep_cols"], **config
    )

    responses = responses[config["responses_keep_cols"]]
    responses = enforce_datatypes(
        responses, keep_columns=config["responses_keep_cols"], **config
    )

    finalsel = read_and_combine_colon_sep_files(config)

    # keep columns is applied in data reading from source, enforcing dtypes
    # in all columns of finalsel
    finalsel = enforce_datatypes(finalsel, keep_columns=list(finalsel), **config)

    # Filter contributors files here to temp fix this overlap

    contributors = pd.merge(
        left=contributors,
        right=finalsel,
        on=[period, reference],
        suffixes=["_spp", "_finalsel"],
        how="outer",
    )
    # Should raise warning for left only or right only joins (missing in other df)
    #

    contributors = create_form_type_spp_column(contributors, config)
    mapper = create_mapper()  # Needs to be defined

    responses_with_missing = create_missing_questions(
        contributors_df=contributors,
        responses_df=responses,
        reference=reference,
        period=period,
        formid=config["form_id_spp"],
        question_no=config["question_no"],
        mapper=mapper,
    )

    df = responses_with_missing.drop(columns=config["form_id_spp"]).merge(
        contributors, on=[reference, period], suffixes=["_res", "_con"], how="left"
    )

    df = append_back_data(df, config)

    snapshot_name = config["mbs_file_name"].split(".")[0]

    df = filter_out_questions(
        df=df,
        column=config["question_no"],
        questions_to_filter=config["filter_out_questions"],
        save_full_path=config["output_path"]
        + snapshot_name
        + "_filter_out_questions.csv",
        **config,
    )

    df = drop_derived_questions(
        df,
        config["question_no"],
        config["form_id_spp"],
    )

    warnings.warn("add live or frozen after fixing error marker column in config")
    df = run_live_or_frozen(
        df,
        config["target"],
        status=config["status"],
        state=config["state"],
        error_values=[201],
    )

    df[config["auxiliary_converted"]] = df[config["auxiliary"]].copy()
    df = convert_annual_thousands(df, config["auxiliary_converted"])

    print("Staging Completed")
    return df


def drop_derived_questions(
    df: pd.DataFrame, question_no: str, form_type_spp: str
) -> pd.DataFrame:
    """
    drops rows containing derived questions based on spp form type

    Parameters
    ----------
    df : pd.DataFrame
        original dataframe with spp form type column
    question_no : str
        column name for question number
    form_type_spp : str
        column name for spp form type
    Returns
    -------
    pd.DataFrame
        _description_
    """

    question_dict = {
        13: 40,
        14: 40,
        15: 46,
        16: 42,
    }

    for formid, question_number in question_dict.items():
        filtered_df = df.loc[
            (df[question_no] == question_number) & (df[form_type_spp] == formid)
        ]
        if not filtered_df.empty:
            warnings.warn(
                f"Derived question dataframe {question_number} for "
                + f"formid {formid} was found in the staged dataframe. "
                + "Dropping"
            )
        df = df.drop(
            df[
                (df[question_no] == question_number) & (df[form_type_spp] == formid)
            ].index
        )
    return df


def start_of_period_staging(
    imputation_output: pd.DataFrame, config: dict
) -> pd.DataFrame:
    """
    Stages the imputation output at the start of a period based on the provided
    configuration.

    Parameters
    ----------
    imputation_output : pd.DataFrame
        The DataFrame containing the imputation output.
    config : dict
        A dictionary containing configuration parameters. Expected keys include:
        - "current_period": The current period to filter the imputation output.
        - "finalsel_keep_cols": List of columns to keep in the final selection.
        - "sample_path": Path to the sample files.
        - "sample_column_names": Column names for the sample files.
        - "idbr_to_spp": Mapping from IDBR to SPP.
        - "form_id_spp": Column name for form ID SPP.
        - "form_id_idbr": Column name for form ID IDBR.
        - "reference": Reference column name.
        - "period": Period column name.
        - "question_no": Question number column name.

    Returns
    -------
    pd.DataFrame
        The staged imputation output DataFrame with missing questions created and
        merged with final selection.
    """

    # Derive period_selected as next month of current period
    input_dataframe = imputation_output.copy()
    current_period = pd.to_datetime(config["current_period"], format="%Y%m")

    period_selected = current_period + pd.DateOffset(months=1)

    # Saving in the dictionary so it can easilly be accessed
    config["period_selected"] = int(period_selected.strftime("%Y%m"))

    if config["current_period"] in imputation_output["period"].unique():

        imputation_output = imputation_output.loc[
            imputation_output["period"] == config["current_period"]
        ]
        logging.info(
            "Setting current_period to the period for SE outputs. Overwriting SEconfig"
        )
        imputation_output["period"] = convert_column_to_datetime(
            imputation_output["period"]
        ) + pd.DateOffset(months=1)

        keep_columns = [
            "period",
            "reference",
            "adjustedresponse",
            "response",
            "construction_link",
            "questioncode",
            "imputation_flags_adjustedresponse",
            "imputation_class",
            "flag_construction_matches_count",
        ]
        imputation_output = imputation_output[keep_columns].rename(
            columns={"imputation_class": "imputation_class_prev_period"}
        )

        config["current_period"] = config["selective_editing_period"]

        finalsel = read_and_combine_colon_sep_files(config)
        finalsel = enforce_datatypes(
            finalsel, keep_columns=config["finalsel_keep_cols"] + ["period"], **config
        )

        idbr_to_spp_mapping = config["idbr_to_spp"]
        finalsel[config["form_id_spp"]] = (
            finalsel[config["form_id_idbr"]].astype(str).map(idbr_to_spp_mapping)
        )

        mapper = create_mapper()
        imputation_output_with_missing = create_missing_questions(
            contributors_df=finalsel,
            responses_df=imputation_output,
            reference=config["reference"],
            period=config["period"],
            formid=config["form_id_spp"],
            question_no=config["question_no"],
            mapper=mapper,
        )
        imputation_output_with_missing[config["question_no"]] = (
            imputation_output_with_missing[config["question_no"]].astype(int)
        )

        imputation_output_with_missing = imputation_output_with_missing.drop(
            columns=config["form_id_spp"]
        ).merge(
            finalsel,
            on=[config["period"], config["reference"]],
            suffixes=["_imputation_output", "_finalsel"],
            how="right",
        )

        imputation_output_with_missing["period"] = (
            imputation_output_with_missing["period"].dt.strftime("%Y%m").astype(int)
        )

        imputation_output_with_missing[config["auxiliary_converted"]] = (
            imputation_output_with_missing[config["auxiliary"]].copy()
        )
        imputation_output_with_missing = convert_annual_thousands(
            imputation_output_with_missing, config["auxiliary_converted"]
        )

        # Keep only questions present in next period and not current period
        dropped_questions = imputation_output_with_missing[
            ~imputation_output_with_missing.apply(
                lambda row: any(
                    row[config["question_no"]] in questions
                    for form_id, questions in mapper.items()
                    if row[config["form_id_spp"]] == form_id
                ),
                axis=1,
            )
        ]
        dropped_questions.to_csv(
            config["output_path"]
            + "dropped_previous_period_"
            + f"se_period_{config['period_selected']}.csv"
        )

        # Keep only the rows that match the condition
        imputation_output_with_missing = imputation_output_with_missing[
            imputation_output_with_missing.apply(
                lambda row: any(
                    row[config["question_no"]] in questions
                    for form_id, questions in mapper.items()
                    if row[config["form_id_spp"]] == form_id
                ),
                axis=1,
            )
        ]

        imputation_output_with_missing = constrain(
            df=imputation_output_with_missing,
            period=config["period"],
            reference=config["reference"],
            target=config["target"],
            question_no=config["question_no"],
            spp_form_id=config["form_id_spp"],
            sic=config["sic"],
        )
        imputation_output_with_missing["imputed_and_derived_flag"] = (
            imputation_output_with_missing.apply(
                lambda row: (
                    "d"
                    if "sum" in str(row["constrain_marker"]).lower()
                    else row[f"imputation_flags_{config['target']}"]
                ),
                axis=1,
            )
        )
        imputation_output_with_missing.drop(
            columns=f"imputation_flags_{config['target']}", inplace=True
        )

        imputation_output_with_missing = new_questions_construction_link(
            imputation_output_with_missing, config
        )

        check_construction_links(imputation_output_with_missing, config)

        remove_derived_if_newly_sampled(
            imputation_output_with_missing, input_dataframe, config
        )

    return imputation_output_with_missing


def remove_derived_if_newly_sampled(
    df: pd.DataFrame, previous_period_df: pd.DataFrame, config: dict
):
    """
    Removes derived questions from the DataFrame if they belong to newly sampled
    business.
    This function identifies rows where the target value is 0.0, the
    `imputed_and_derived_flag` is set to "d", and the reference is not present
    in the previous period's DataFrame. For such rows, it sets the
    `imputed_and_derived_flag` and the target column to None.
    Parameters
    ----------
    df : pd.DataFrame
        The current period's DataFrame containing the data to be processed.
    previous_period_df : pd.DataFrame
        The DataFrame containing data from the previous period.
    config : dict
        A dictionary containing configuration details. It must include:
        - "reference": The column name used to identify references.
        - "target": The column name of the target variable.
    Returns
    -------
    pd.DataFrame
        The modified DataFrame with derived questions removed for newly
        sampled businesses.
    Notes
    -----
    - A business is considered newly sampled if its reference is not present
      in the `previous_period_df`.
    - The function logs the number of rows identified as newly sampled
      derived questions.
    """

    references_in_prev_period = previous_period_df[config["reference"]].unique()
    condition = (
        (df[config["target"]] == 0.0)
        & (df["imputed_and_derived_flag"] == "d")
        & ~(df[config["reference"]].isin(references_in_prev_period))
    )
    # Pull out all references where this happens
    num_rows = df.loc[condition].shape[0]
    logging.info(
        "number of rows which have been identified "
        f"as newly sampled derived questions, {num_rows}"
    )
    # Set the values to None for the specified columns
    df.loc[condition, ["imputed_and_derived_flag", config["target"]]] = None

    return df


def new_questions_construction_link(df, config):
    """
    Updates the 'construction_link' based on previous the previous period's imputation

    Parameters
    ----------
    df : pandas.DataFrame
        The input DataFrame containing the data to be processed.
    config : dict
        A dictionary containing configuration parameters. Expected keys include:
        - "cell_number" : str
            The column name for cell numbers.
        - "question_no" : str
            The column name for question numbers.
        - "period" : str
            The column name for the period.

    Returns
    -------
    pandas.DataFrame
        The transformed DataFrame with updated 'construction_link' values.
    """
    prev_period_imp_class = "imputation_class_prev_period"
    current_period_imp_class = "imputation_class"
    cell_no = config["cell_number"]
    question_no = config["question_no"]

    df = convert_cell_number(df, cell_no)
    df = create_imputation_class(df, cell_no, current_period_imp_class)

    df[prev_period_imp_class] = df[prev_period_imp_class].combine_first(
        df[current_period_imp_class]
    )

    # Update q49 construction link to be construction_link / matched_pairs
    bool_mask = (
        (df[question_no] == 49)
        & (df["flag_construction_matches_count"] != 0)
        & (df["flag_construction_matches_count"].notna())
    )
    df.loc[bool_mask, "construction_link"] = (
        df.loc[bool_mask, "construction_link"]
        / df.loc[bool_mask, "flag_construction_matches_count"]
    )

    num_inf_construction_links = (
        df.loc[df[question_no] == 49, "construction_link"].isin([float("inf")]).sum()
    )
    if num_inf_construction_links > 0:
        logging.warning(
            "Number of times construction link is inf: "
            + f"{num_inf_construction_links}"
        )

    df["construction_link"] = df.groupby(
        [config["period"], question_no, prev_period_imp_class]
    )["construction_link"].transform(lambda group: group.ffill().bfill())

    # Can drop after sign off, just for testing
    # df.drop(columns=[config["cell_number"]+"_prev_period", config["cell_number"],
    # "imputation_class"], inplace=True)

    return df


def check_construction_links(df: pd.DataFrame, config: dict):
    """
    checks construction links for q49. Raises warning and outputs dataframe if any
    construction links are greater than 1 for q49

    Parameters
    ----------
    df : pd.DataFrame
        main dataframe containing the data to be processed
    config : dict
        main config dictionary
    """
    logger.info("checking values for construction link for q49")
    df_large_construction_link = df.loc[
        (df["construction_link"] > 1)
        & df[config["question_no"]].astype(int).isin([49]),
        [config["reference"], "construction_link"],
    ]
    if not df_large_construction_link.empty:
        logger.warning(
            "number of records with construction link > 1 for q49: "
            + f"{df_large_construction_link.shape[0]}"
        )
        output_file = os.path.join(
            config["output_path"],
            f"q49_references_con_link_greater_1_{config['current_period']}.csv",
        )
        df_large_construction_link.to_csv(
            output_file,
            index=False,
        )
        logger.info(
            f"references with construction link > 1 for q49 saved to {output_file}"
        )
