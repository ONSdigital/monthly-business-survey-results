from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.aggregate_aweights_by_class import aggregate_aweights_by_class


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation")


@pytest.fixture(scope="class")
def aweights_test_input(filepath):
    return pd.read_csv(
        filepath / "aggregate_aweights_by_class_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def aweights_test_output(filepath):
    return pd.read_csv(
        filepath / "aggregate_aweights_by_class_output.csv", index_col=False
    )


class TestAggregateAWeightsByClass:
    def test_aggregate_aweights_by_class(
        self, aweights_test_input, aweights_test_output
    ):
        expected_output = aweights_test_output

        input_data = aweights_test_input

        actual_output = aggregate_aweights_by_class(
            input_data,
            "imp_class",
            "period",
            "a_weight",
            202402,
        )

        assert_frame_equal(actual_output, expected_output)
