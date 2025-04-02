import pandas as pd

from mbs_results.outlier_detection.winsorisation import winsorise
from mbs_results.utilities.constrains import (
    replace_outlier_weights,
    update_derived_weight_and_winsorised_value,
)


def join_l_values(df, l_values_path, classification_values_path, config):
    """Read l values, classifications and drop duplicates and period"""

    l_values = pd.read_csv(
        l_values_path, dtype={"question_no": "int64", "classification": "str"}
    )

    # Merge on classification SIC map (merge on SIC to get classsificaion on df -> )
    classification_values = pd.read_csv(classification_values_path, dtype=str)

    df = pd.merge(
        df,
        classification_values,
        left_on="frosic2007",
        right_on="sic_5_digit",
        how="left",
    )
    # left on question frocsic .-> Change to left on question_no
    # and classication from above
    df = pd.merge(
        df,
        l_values,
        how="left",
        left_on=[config["question_no"], "classification"],
        right_on=["question_no", "classification"],
    )
    df.drop(columns=["question_no"], inplace=True)

    return df


def detect_outlier(df, config):
    """
    # Todo: docstrings
    """
    pre_win = join_l_values(
        df, config["l_values_path"], config["classification_values_path"], config
    )

    post_win = pre_win.groupby(config["question_no"]).apply(
        lambda df: winsorise(
            df,
            "calibration_group",
            config["period"],
            config["auxiliary_converted"],
            config["census"],
            "design_weight",
            "calibration_factor",
            config["target"],
            "l_value",
        )
    )

    # Remove groupby leftovers
    post_win.reset_index(drop=True, inplace=True)

    post_win = update_derived_weight_and_winsorised_value(
        post_win,
        config["reference"],
        config["period"],
        config["question_no"],
        config["form_id_spp"],
        "outlier_weight",
        config["target"],
    )

    # Replace outlier weights
    post_win = replace_outlier_weights(
        post_win,
        config["reference"],
        config["period"],
        config["question_no"],
        "outlier_weight",
        config["manual_outlier_path"],
    )

    return post_win
