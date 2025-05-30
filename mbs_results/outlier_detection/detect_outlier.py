import pandas as pd

from mbs_results.outlier_detection.winsorisation import winsorise
from mbs_results.utilities.constrains import (
    replace_with_manual_outlier_weights,
    update_derived_weight_and_winsorised_value,
)
from mbs_results.utilities.inputs import read_csv_wrapper


def join_l_values(df, l_values_path, classification_values_path, config):
    """Read l values, classifications and drop duplicates and period"""

    l_value_question_no = config["l_value_question_no"]

    l_values = read_csv_wrapper(
        l_values_path,
        config["platform"],
        config["bucket"],
        dtype={"classification": "str"},
    )

    # Merge on classification SIC map (merge on SIC to get classsificaion on df -> )
    # SIC is now called from config
    classification_values = read_csv_wrapper(
        classification_values_path, config["platform"], config["bucket"], dtype=str
    )
    df = pd.merge(
        df,
        classification_values,
        left_on=config["sic"],
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
        right_on=[l_value_question_no, "classification"],
    )
    df.drop(columns=l_value_question_no, inplace=True)

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

    # Replace outlier weights
    post_win = replace_with_manual_outlier_weights(
        post_win,
        config["reference"],
        config["period"],
        config["question_no"],
        "outlier_weight",
        config,
    )
    post_win = update_derived_weight_and_winsorised_value(
        post_win,
        config["reference"],
        config["period"],
        config["question_no"],
        config["form_id_spp"],
        "outlier_weight",
        config["target"],
    )

    return post_win
