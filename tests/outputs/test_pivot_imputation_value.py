from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.pivot_imputation_value import (
    merge_counts,
    create_imputation_link_column,
    create_count_imps_column,
    format_imputation_link,
    create_imputation_link_output,
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
def create_imputation_link_column_input(filepath):
    return pd.read_csv(
        filepath / "create_imputation_link_column_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def create_imputation_link_column_output(filepath):
    return pd.read_csv(
        filepath / "create_imputation_link_column_output.csv", index_col=False
    )


@pytest.fixture(scope="class")
def format_imputation_link_input(filepath):
    return pd.read_csv(filepath / "format_imputation_link_input.csv", index_col=False)


@pytest.fixture(scope="class")
def format_imputation_link_output(filepath):
    return pd.read_csv(filepath / "format_imputation_link_output.csv", index_col=False)


class TestMergeCounts:
    def test_merge_counts(self, count_data_input, merge_counts_output):

        input_df = merge_counts_output.drop(columns=["f_count", "b_count", "c_count"])

        actual_output = merge_counts(
            input_df, count_data_input, "cell", "group", "date", "period", "identifier"
        )
        expected_output = merge_counts_output

        assert_frame_equal(actual_output, expected_output)


class TestCreateImputationLinkColumn:
    def test_create_imputation_link_column(
        self, create_imputation_link_column_input, create_imputation_link_column_output
    ):

        input_df = create_imputation_link_column_input

        expected_output = create_imputation_link_column_output

        actual_output = create_imputation_link_column(
            create_imputation_link_column_input
        )

        assert_frame_equal(actual_output, expected_output)


class TestCreateCountImpsColumn:
    def test_create_count_imps_column(self):

        input_df = pd.DataFrame(
            {"cell_no": ["A", "B", "B", "C", "C", "C"], "reference": [1, 2, 3, 4, 5, 6]}
        )

        expected_output = pd.DataFrame(
            {
                "cell_no": ["A", "B", "B", "C", "C", "C"],
                "reference": [1, 2, 3, 4, 5, 6],
                "count_imps": [1, 2, 2, 3, 3, 3],
            }
        )

        actual_output = create_count_imps_column(input_df)

        assert_frame_equal(actual_output, expected_output)


class TestFormatImputationLink:
    def test_format_imputation_link_output(
        self, format_imputation_link_input, format_imputation_link_output
    ):

        input_df = format_imputation_link_input

        expected_output = format_imputation_link_output

        actual_output = format_imputation_link(input_df)

        assert_frame_equal(actual_output, expected_output)


class TestFormatCreateImputationLinkOutput:
    def test_format_create_imputation_link_output(
        self, create_imputation_link_column_input, format_imputation_link_output
    ):

        input_df = create_imputation_link_column_input

        expected_output = format_imputation_link_output

        actual_output = create_imputation_link_output(input_df)

        assert_frame_equal(actual_output, expected_output)
