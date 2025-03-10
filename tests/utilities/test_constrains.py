from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.utilities.constrains import (
    calculate_derived_outlier_weights,
    constrain,
    replace_values_index_based,
    sum_sub_df,
    update_derived_weight_and_winsorised_value,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/utilities/constrains")


def test_replace_values_index_base(filepath):

    df = pd.read_csv(filepath / "test_replace_values_index_based.csv", index_col=False)

    df = df.set_index(["question_no", "period", "reference"])

    df_in = df[["target"]].copy()

    df_expected = df[["expected", "constrain_marker"]].rename(
        columns={"expected": "target"}
    )
    df_expected.sort_index(inplace=True)

    replace_values_index_based(df_in, "target", 49, ">", 40)
    replace_values_index_based(df_in, "target", 90, ">=", 40)

    # Enforce dtypes, otherwise null==null fails
    df_in["constrain_marker"] = df_in["constrain_marker"].astype(str)
    df_expected["constrain_marker"] = df_expected["constrain_marker"].astype(str)

    assert_frame_equal(df_in, df_expected)


def test_sum_sub_df_46_47(filepath):

    df = pd.read_csv(filepath / "test_sum_sub_df.csv", index_col=False)

    df_in = (
        df.loc[df["question_no"] != 40]
        .drop(columns=["constrain_marker"])
        .set_index(["question_no", "period", "reference"])
    )

    expected_output = (
        df.loc[df["question_no"] == 40]
        .drop(columns=["question_no"])
        .reset_index(drop=True)
    )

    actual_ouput = (
        sum_sub_df(df_in, [46, 47]).sort_values(by="reference").reset_index(drop=True)
    )

    assert_frame_equal(actual_ouput, expected_output)


def test_constrain_functionality(filepath):
    df = pd.read_csv(
        filepath / "test_constrain.csv",
        index_col=False,
    )
    # Creating dummy columns needed for constrains, not used other than setting as index
    for col_name in ["cell_no", "frotover", "froempment", "frosic2007"]:
        df[col_name] = 1

    df["target"] = df["target"].astype(float)
    df["spp_form_id"] = df["spp_form_id"].astype(int)

    # Drop q40 rows for form 13 and 14 and q46 for form 15
    df_input = (
        df.drop(
            df[(df["question_no"] == 40) & (df["spp_form_id"].isin([13, 14]))].index
        )
        .drop(df[(df["question_no"] == 46) & (df["spp_form_id"].isin([15]))].index)
        .drop(
            columns=[
                "pre_derived_target",
                "expected_target",
                "pre_constrained_target",
                "constrain_marker",
            ]
        )
    )

    df_expected_output = df.drop(
        columns=["cell_no", "frotover", "froempment", "frosic2007", "target"]
    ).rename(columns={"expected_target": "target"})
    df_expected_output["target"] = df_expected_output["target"].astype(float)

    df_output = constrain(
        df_input,
        "period",
        "reference",
        "target",
        "question_no",
        "spp_form_id",
    )

    # Dropping dummy columns as these are unchanged in function
    order = [
        "period",
        "reference",
        "spp_form_id",
        "question_no",
        "target",
        "pre_derived_target",
        "constrain_marker",
        "pre_constrained_target",
    ]

    df_output.drop(
        columns=["cell_no", "frotover", "froempment", "frosic2007"], inplace=True
    )
    df_output = df_output[order].sort_values(by=order).reset_index(drop=True)

    df_expected_output = (
        df_expected_output[order].sort_values(by=order).reset_index(drop=True)
    )
    df_expected_output["spp_form_id"] = df_expected_output["spp_form_id"].astype(
        "int64"
    )
    assert_frame_equal(df_output, df_expected_output)


def test_calculate_derived_outlier_weights(filepath):
    df = pd.read_csv(
        filepath / "derived-questions-winsor.csv",
        index_col=False,
    )
    df["target_variable"] = df["target_variable"].astype(float)
    df["new_target_variable"] = df["new_target_variable"].astype(float)
    # Drop q40 rows
    df_input = df.drop(df[df["question_no"] == 40].index)
    df_input.drop(
        columns=["default_o_weight", "constrain_marker", "post_wins_marker"],
        inplace=True,
    )

    df_output = calculate_derived_outlier_weights(
        df_input,
        "period",
        "reference",
        "target_variable",
        "question_no",
        "spp_form_id",
        "outlier_weight",
        "new_target_variable",
    )

    sorting_by = ["reference", "period", "question_no", "spp_form_id"]
    input_col_order = df.columns
    df_output = (
        df_output[input_col_order].sort_values(by=sorting_by).reset_index(drop=True)
    )
    df = df.sort_values(by=sorting_by).reset_index(drop=True)

    assert_frame_equal(df, df_output)


def test_calculate_derived_outlier_weights_missing(filepath):
    df = pd.read_csv(
        filepath / "derived-questions-winsor-missing.csv",
        index_col=False,
    )
    df["target_variable"] = df["target_variable"].astype(float)
    df["new_target_variable"] = df["new_target_variable"].astype(float)
    # Drop q40 rows
    df_input = df.drop(df[df["question_no"] == 40].index)
    df_input.drop(
        columns=["default_o_weight", "constrain_marker", "post_wins_marker"],
        inplace=True,
    )
    # Manually change the input data to be missing one value in
    # new_target_variable . data is present in dataset to compare against
    df_input.loc[
        (df_input["reference"] == 101)
        & (df_input["period"] == 202401)
        & (df_input["question_no"] == 46)
        & (df_input["spp_form_id"] == 13),
        "new_target_variable",
    ] = None

    df_output = calculate_derived_outlier_weights(
        df_input,
        "period",
        "reference",
        "target_variable",
        "question_no",
        "spp_form_id",
        "outlier_weight",
        "new_target_variable",
    )

    sorting_by = ["reference", "period", "question_no", "spp_form_id"]
    input_col_order = df.columns
    df_output = (
        df_output[input_col_order].sort_values(by=sorting_by).reset_index(drop=True)
    )
    df = df.sort_values(by=sorting_by).reset_index(drop=True)

    assert_frame_equal(df, df_output)


scenarios = [
    "no_further_processing_required",
    "outlier_identified_example_1",
    "outlier_identified_example_2",
]


@pytest.mark.parametrize("base_file_name", scenarios)
def test_update_derived_weight_and_winsorised_value(filepath, base_file_name):

    df_in = pd.read_csv(filepath / Path(base_file_name + ".csv"))
    df_expected = pd.read_csv(filepath / Path(base_file_name + "_expected.csv"))
    df_actual = update_derived_weight_and_winsorised_value(
        df_in,
        "reference",
        "period",
        "questioncode",
        "spp_form_id",
        "outlier_weight",
        "winsorised_value",
    )

    assert_frame_equal(df_actual, df_expected)
