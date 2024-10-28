from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.utilities.constrains import (
    calculate_derived_outlier_weights,
    replace_values_index_based,
    sum_sub_df,
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
    df_in['constrain_marker'] = df_in['constrain_marker'].astype(str)
    df_expected['constrain_marker'] = df_expected['constrain_marker'].astype(str)   

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
