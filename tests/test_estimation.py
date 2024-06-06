from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from src.estimation import calculate_a_weight, calculate_calibration_factor


@pytest.fixture(scope="class")
def filepath():
    return Path("tests") / "data" / "estimation"


class TestEstimation:
    def test_calculate_a_weights(self, filepath):
        expected_output = pd.read_csv(filepath / "a_weights.csv")

        input_data = expected_output.drop(columns=["a_weight"])
        actual_output = calculate_a_weight(
            input_data,
            "period",
            "strata",
            "sampled",
        )

        assert_frame_equal(actual_output, expected_output)

    @pytest.mark.parametrize(
        "csv,group",
        [
            ("calibration_factor_separate.csv", "strata"),
            ("calibration_factor_combined.csv", "calibration_group"),
        ],
    )
    def test_calculate_calibration_factor(self, filepath, csv, group):
        expected_output = pd.read_csv(filepath / csv)

        input_data = expected_output.drop(columns=["calibration_factor"])
        actual_output = calculate_calibration_factor(
            input_data,
            "period",
            group,
            "sampled",
            "auxiliary",
        )

        assert_frame_equal(actual_output, expected_output)
        assert_series_equal(expected_output["a_weight"], actual_output["a_weight"])
