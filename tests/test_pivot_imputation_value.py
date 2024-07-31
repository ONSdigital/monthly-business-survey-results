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
def count_data_input(filepath):
    return pd.read_csv(filepath / "data" / "count_data_input.csv", index_col=False)


@pytest.fixture(scope="class")
def merge_counts_output(filepath):
    return pd.read_csv(filepath / "data" / "merge_counts_output.csv", index_col=False)


@pytest.fixture(scope="class")
def pivot_imputation_value_output(filepath):
    return pd.read_csv(
        filepath / "data" / "pivot_imputation_value_output.csv", index_col=False
    )


class TestMergeCounts:
    def test_merge_counts(self, count_data_input, merge_counts_output):

        input_df = merge_counts_output.drop(columns=["f_count", "b_count"])

        actual_output = merge_counts(
            input_df, count_data_input, "cell", "group", "date", "period", "identifier"
        )
        expected_output = merge_counts_output

        assert_frame_equal(actual_output, expected_output)


class TestPivotImputationValue:
    def test_pivot_imputation_value(
        self, pivot_imputation_value_output, merge_counts_output
    ):

        expected_output = pivot_imputation_value_output

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
