import pandas as pd

from mbs_results.outlier_detection.winsorisation import winsorise
from mbs_results.utilities.constrains import calculate_derived_outlier_weights


def join_l_values(df, l_values_path, classification_values_path):
    """Read l values, classifications and drop duplicates and period"""

    l_values = pd.read_csv(l_values_path)
    l_values["classification"] = l_values["classification"].astype(str)
    l_values["question_no"] = l_values["question_no"].astype("int64")

    # l_values = l_values.drop_duplicates(['question_no','classification'])

    # l_values = l_values.drop(columns=["period"])

    # Merge on classification SIC map (merge on SIC to get classsificaion on df -> )
    classification_values = pd.read_csv(classification_values_path)
    classification_values["sic_5_digit"] = classification_values["sic_5_digit"].astype(
        str
    )
    classification_values["classification"] = classification_values[
        "classification"
    ].astype(str)

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
        left_on=["questioncode", "classification"],
        right_on=["question_no", "classification"],
    )

    return df


def detect_outlier(df, config):
    pre_win = join_l_values(
        df, config["l_values_path"], config["classification_values_path"]
    )

    post_win = pre_win.groupby("question_no").apply(
        lambda df: winsorise(
            df,
            "calibration_group",
            "period",
            "frotover",
            "sampled",
            "design_weight",
            "calibration_factor",
            "adjustedresponse",
            "l_value",
        )
    )
    post_win = calculate_derived_outlier_weights(
        post_win,
        "period",
        "reference",
        "adjustedresponse",
        "questioncode",
        "form_type_spp",
        "outlier_weight",
        "new_target_variable",
    )
    return post_win
