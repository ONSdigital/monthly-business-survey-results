import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.estimation.calculate_estimation_weights import (
    calculate_calibration_factor,
    calculate_design_weight,
)


@pytest.fixture(scope="class")
def data_dir(estimation_data_dir):
    return estimation_data_dir / "calculate_estimation_weights"


class TestEstimation:
    def test_calculate_design_weights(self, data_dir):
        expected_output = pd.read_csv(data_dir / "design_weights.csv")

        input_data = expected_output.drop(columns=["design_weight"])
        actual_output = calculate_design_weight(
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
    def test_calculate_calibration_factor(self, data_dir, csv, group):
        expected_output = pd.read_csv(data_dir / csv)

        input_data = expected_output.drop(columns=["calibration_factor"])
        actual_output = calculate_calibration_factor(
            input_data, "period", group, "sampled", "auxiliary", "design_weight"
        )

        assert_frame_equal(actual_output, expected_output)
