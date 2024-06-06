from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from src.estimation import calculate_a_weight


@pytest.fixture(scope="class")
def a_weight_data():
    return pd.read_csv(Path("tests") / "data" / "estimation_a_weights.csv")


class TestEstimation:
    def test_calculate_a_weights(self, a_weight_data):
        expected_output = a_weight_data

        input_data = expected_output.drop(columns=["a_weight"])
        actual_output = calculate_a_weight(
            input_data,
            "period",
            "strata",
            "sampled",
        )

        assert_frame_equal(actual_output, expected_output)
