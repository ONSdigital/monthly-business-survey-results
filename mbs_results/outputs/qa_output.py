import pandas as pd

from mbs_results.utilities.utils import unpack_dates_and_comments


def produce_qa_output(
    additional_outputs_df: pd.DataFrame,
    split_qa_by_period: bool,
    **config,
) -> pd.DataFrame:
    """Produces an output with required columns, and with total weight and
    weighted adjusted value calculated.

     Parameters
    ----------
    config : dict
        The config as a dictionary.
    post_win_df : pd.DataFrame
        Dataframe containing the required columns, following the
        outlier_detection module.
    """

    target = config["target"]

    additional_outputs_df = unpack_dates_and_comments(
        df=additional_outputs_df,
        reformat_questions=config["filter_out_questions"],
        question_no_plaintext=config["question_no_plaintext"],
        config=config,
    )

    requested_columns = [
        config["reference"],
        config["period"],
        config["sic"],
        "classification",
        config["cell_number"],
        config["auxiliary"],
        "froempment",
        "formtype",
        "imputed_and_derived_flag",  # response_type
        config["question_no"],
        f"{target}",
        "status",  # error_mkr
        "design_weight",
        "calibration_factor",
        "outlier_weight",
        "total weight (A*G*O)",
        "weighted adjusted value",
        f"imputation_flags_{target}",
        "imputation_class",
        f"f_link_{target}",
        f"default_link_f_match_{target}",
        f"b_link_{target}",
        f"default_link_b_match_{target}",
        "construction_link",
        "flag_construction_matches_count",
        "default_link_flag_construction_matches",
        "constrain_marker",  # these not requested but useful
        # column names for counts depends if a filter was applied in mbs
        f"b_match_{target}_count",
        f"f_match_{target}_count",
        f"b_match_filtered_{target}_count",
        f"f_match_filtered_{target}_count",
        "runame1",
        config["pound_thousand_col"],
        "entref",
        "start_date",
        "end_date",
        "comments",
    ]

    # Check if column names specified in config, if not, use above as default
    cols_from_config = []

    for col in requested_columns:
        cols_from_config.append(config.get(col, col))

    additional_outputs_df["total weight (A*G*O)"] = (
        additional_outputs_df[config["design_weight"]]
        * additional_outputs_df[config["calibration_factor"]]
        * additional_outputs_df["outlier_weight"]
    )

    additional_outputs_df["weighted adjusted value"] = (
        additional_outputs_df[config["target"]]
        * additional_outputs_df["total weight (A*G*O)"]
    )
    additional_outputs_df = additional_outputs_df[
        additional_outputs_df.columns.intersection(cols_from_config)
    ]

    if split_qa_by_period:
        output = {}
        for period, df_period in additional_outputs_df.groupby(config["period"]):
            output[str(int(period))] = df_period.reset_index(drop=True)
    else:
        output = additional_outputs_df
    return output
