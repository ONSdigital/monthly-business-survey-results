from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.pivot_imputation_value import (
    merge_counts,
    pivot_imputation_value,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/pivot_imputation_value")


@pytest.fixture(scope="class")
def count_data_input(filepath):
    return pd.read_csv(filepath / "count_data_input.csv", index_col=False)


@pytest.fixture(scope="class")
def merge_counts_output(filepath):
    return pd.read_csv(filepath / "merge_counts_output.csv", index_col=False)


@pytest.fixture(scope="class")
def pivot_imputation_value_output(filepath):
    return pd.read_csv(filepath / "pivot_imputation_value_output.csv", index_col=False)


class TestMergeCounts:
    def test_merge_counts(self, count_data_input, merge_counts_output):

        input_df = merge_counts_output.drop(columns=["f_count", "b_count", "c_count"])

        actual_output = merge_counts(
            input_df, count_data_input, "cell", "group", "date", "period", "identifier"
        )
        expected_output = merge_counts_output

        assert_frame_equal(actual_output, expected_output)


class TestPivotImputationValue:
    def test_pivot_imputation_value_filter(
        self, pivot_imputation_value_output, merge_counts_output
    ):

        expected_output = pivot_imputation_value_output.query("date == 202001")

        input_data = merge_counts_output.drop(columns=["identifier"])

        actual_output = pivot_imputation_value(
            input_data,
            "identifier",
            ["date", "sic", "cell", "question"],
            ["forward", "backward", "construction"],
            ["f_count", "b_count", "c_count"],
            "imputed_value",
            [202001],
        )

        assert_frame_equal(actual_output, expected_output)

    def test_pivot_imputation_value_no_filter(
        self, pivot_imputation_value_output, merge_counts_output
    ):

        expected_output = pivot_imputation_value_output

        input_data = merge_counts_output.drop(columns=["identifier"])
        input_data = input_data.query("date in [202001, 202002]")

        actual_output = pivot_imputation_value(
            input_data,
            "identifier",
            ["date", "sic", "cell", "question"],
            ["forward", "backward", "construction"],
            ["f_count", "b_count", "c_count"],
            "imputed_value",
        )

        assert_frame_equal(actual_output, expected_output)
