import pandas as pd

from mbs_results.utilities.outputs import split_by_period


def produce_qa_output(
    additional_outputs_df: pd.DataFrame,
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

    additional_outputs_df = additional_outputs_df[
        additional_outputs_df.columns.intersection(cols_from_config)
    ]

    condition_to_split = config["split_qa_output_by_period"] or (
        "qa_output" in config.get("split_output_by_period", [])
    )

    output = split_by_period(
        additional_outputs_df, config["period"], condition_to_split
    )

    return output
