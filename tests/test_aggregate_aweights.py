from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.aggregate_aweight import aggregate_aweight


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation")


@pytest.fixture(scope="class")
def aweights_test_input(filepath):
    return pd.read_csv(filepath / "aggregate_aweights_input.csv", index_col=False)


@pytest.fixture(scope="class")
def aweights_test_output(filepath):
    return pd.read_csv(filepath / "aggregate_aweights_output.csv", index_col=False)


class TestAggregateAWeights:
    def test_aggregate_aweights(self, aweights_test_input, aweights_test_output):
        expected_output = aweights_test_output

        input_data = aweights_test_input

        actual_output = aggregate_aweight(
            input_data,
            "reference",
            "imp_class",
            "period",
            "a_weight",
            202402,
        )

        assert_frame_equal(actual_output, expected_output)
