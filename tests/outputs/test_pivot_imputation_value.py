from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.pivot_imputation_value import (
    merge_counts,
    create_imputation_link
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
def create_imputation_link_input(filepath):
    return pd.read_csv(filepath / "create_imputation_link_input.csv", index_col=False)
  
@pytest.fixture(scope="class")
def create_imputation_link_output(filepath):
    return pd.read_csv(filepath / "create_imputation_link_output.csv", index_col=False)


class TestMergeCounts:
    def test_merge_counts(self, count_data_input, merge_counts_output):

        input_df = merge_counts_output.drop(columns=["f_count", "b_count", "c_count"])

        actual_output = merge_counts(
            input_df, count_data_input, "cell", "group", "date", "period", "identifier"
        )
        expected_output = merge_counts_output

        assert_frame_equal(actual_output, expected_output)

class TestCreateImputationLink:
  def test_create_imputation_link(self, create_imputation_link_input, create_imputation_link_output):
    
    input_df = create_imputation_link_input
    
    expected_output = create_imputation_link_output
    
    actual_output = create_imputation_link(create_imputation_link_input)
    
    assert_frame_equal(actual_output, expected_output)