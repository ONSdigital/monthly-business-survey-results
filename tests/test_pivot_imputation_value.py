from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.pivot_imputation_value import merge_counts, pivot_imputation_value


@pytest.fixture(scope="class")
def filepath():
    return Path("tests")


@pytest.fixture(scope="class")
def merge_counts_input(filepath):
    input_df = pd.read_csv(
        filepath / "data" / "pivot_imputation_value_input.csv", index_col=False
    )
    count_df = pd.read_csv(
        filepath / "data" / "pivot_imputation_value_counts_input.csv", index_col=False
    )
    return (input_df, count_df)


@pytest.fixture(scope="class")
def merge_counts_output(filepath):
    return pd.read_csv(
        filepath / "data" / "pivot_imputation_value_input_2.csv", index_col=False
    )


@pytest.fixture(scope="class")
def pivot_imputation_value_output(filepath):
    return pd.read_csv(
        filepath / "data" / "pivot_imputation_value_output.csv", index_col=False
    )


class TestMergeCounts:
    def test_merge_counts(self, merge_counts_input, merge_counts_output):
        input_df, count_df = merge_counts_input
        actual_output = merge_counts(input_df, count_df)
        expected_output = merge_counts_output
        assert_frame_equal(actual_output, expected_output)


class TestPivotImputationValue:
    def test_pivot_imputation_value(
        self, pivot_imputation_value_output, merge_counts_output
    ):
        expected_output = pivot_imputation_value_output.reset_index(drop=True)
        expected_output["link_type"] = pd.Categorical(
            expected_output["link_type"], categories=["F", "B", "C"], ordered=True
        )

        input_data = merge_counts_output.drop(columns=["identifier"])

        actual_output = pivot_imputation_value(
            input_data,
            "identifier",
            "date",
            "sic",
            "cell",
            "forward",
            "backward",
            "construction",
            "question",
            "imputed_value",
            "f_count",
            "b_count",
        )

        actual_output["link_type"] = pd.Categorical(
            actual_output["link_type"], categories=["F", "B", "C"], ordered=True
        )

        assert_frame_equal(actual_output, expected_output)
