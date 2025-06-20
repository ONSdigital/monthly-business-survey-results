import pandas as pd


def produce_qa_output(config: dict, post_win_df: pd.DataFrame) -> pd.DataFrame:
    """Produces an output with required columns, and with total weight and weighted adjusted value calculated.

     Parameters
    ----------
    config : dict
        The config as a dictionary.
    post_win_df : pd.DataFrame
        Dataframe containing the required columns, following the outlier_detection module.
    """

    target = config["target"]

    requested_columns = [
        "reference",
        "period",
        "sic",
        "classification",
        "cell_number",
        "auxiliary",
        "froempment",
        "formtype",
        "imputed_and_derived_flag",  # response_type
        "question_no",
        "target",
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
    ]

    if not config["filter"]:
        count_variables = [f"b_match_{target}_count", f"f_match_{target}_count"]
    else:
        count_variables = [
            f"b_match_filtered_{target}_count",
            f"f_match_filtered_{target}_count",
        ]

    requested_columns += count_variables

    # Check if column names specified in config, if not, use above as default
    cols_from_config = []

    for col in requested_columns:
        cols_from_config.append(config.get(col, col))

    post_win_df["total weight (A*G*O)"] = (
        post_win_df[config["design_weight"]]
        * post_win_df[config["calibration_factor"]]
        * post_win_df["outlier_weight"]
    )

    post_win_df["weighted adjusted value"] = (
        post_win_df[config["target"]] * post_win_df["total weight (A*G*O)"]
    )

    missing_columns = [
        col for col in cols_from_config if col not in post_win_df.columns
    ]

    for i in range(0, len(missing_columns), 3):
        print("Missing columns:", missing_columns[i : i + 3])

    return post_win_df[cols_from_config]
