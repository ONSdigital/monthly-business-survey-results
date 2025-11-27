import pandas as pd


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

    reformat_questions = config["filter_out_questions"]
    question_no_plaintext = config["question_no_plaintext"]

    date_comments_df = additional_outputs_df.copy().loc[
        additional_outputs_df[config["question_no"]].isin(reformat_questions),
        [
            config["period"],
            config["reference"],
            config["question_no"],
            target,
        ],
    ]
    # Converting to string ready for this to become column names
    date_comments_df[config["question_no"]] = date_comments_df[
        config["question_no"]
    ].astype(str)
    # Converting questions 11, 12, 146 to columns renaming to text based on config
    date_comments_df = (
        date_comments_df.pivot_table(
            index=[config["period"], config["reference"]],
            columns=config["question_no"],
            values=target,
            aggfunc="first",
        )
        .reset_index()
        .rename(columns=question_no_plaintext)
    )

    additional_outputs_df = additional_outputs_df.loc[
        ~additional_outputs_df[config["question_no"]].isin(reformat_questions)
    ]
    additional_outputs_df[target] = additional_outputs_df[target].astype(float)

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
        f"{target}_pounds_thousands",
        "entref",
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

    additional_outputs_df = pd.merge(
        additional_outputs_df,
        date_comments_df,
        on=[config["period"], config["reference"]],
        how="left",
        suffixes=("", "_y"),
    )

    return additional_outputs_df
