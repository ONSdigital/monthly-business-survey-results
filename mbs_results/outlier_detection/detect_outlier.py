import pandas as pd

from mbs_results.outlier_detection.winsorisation import winsorise


def join_l_values(df, l_values_path, classification_values_path):
    """Read l values, classifications and drop duplicates and period"""

    l_values = pd.read_csv(l_values_path)

    # l_values = l_values.drop_duplicates(['question_no','classification'])

    # l_values = l_values.drop(columns=["period"])

    # Merge on classification SIC map (merge on SIC to get classsificaion on df -> )
    classification_values = pd.read_csv(classification_values_path)

    print(list(classification_values))
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
        left_on=["question_no", "classification"],
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
            "adjusted_value",
            "l_value",
        )
    )

    # TODO Add constrains post winsorised
    return post_win
