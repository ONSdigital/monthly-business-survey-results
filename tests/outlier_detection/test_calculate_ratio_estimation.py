from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outlier_detection.calculate_ratio_estimation import (
    calculate_ratio_estimation,
)


@pytest.fixture(scope="class")
def ratio_estimation_test_data():
    return pd.read_csv(
        Path("tests")
        / "data"
        / "outlier_detection"
        / "calculate_ratio_estimation"
        / "ratio_estimation_data.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


@pytest.fixture(scope="class")
def ratio_estimation_test_output():
    return pd.read_csv(
        Path("tests")
        / "data"
        / "outlier_detection"
        / "calculate_ratio_estimation"
        / "ratio_estimation_data_output.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


class TestRatioEstimation:
    def test_calculate_ratio_estimation(
        self, ratio_estimation_test_output, ratio_estimation_test_data
    ):
        expected_output = ratio_estimation_test_output[
            [
                "aux",
                "is_census",
                "a_weight",
                "g_weight",
                "target_variable",
                "predicted_unit_value",
                "l_value",
                "nw_ag_flag",
                "ratio_estimation_treshold",
            ]
        ]

        input_data = ratio_estimation_test_data[
            [
                "aux",
                "is_census",
                "a_weight",
                "g_weight",
                "target_variable",
                "predicted_unit_value",
                "l_value",
                "nw_ag_flag",
            ]
        ]

        actual_output = calculate_ratio_estimation(
            input_data,
            "aux",
            "is_census",
            "a_weight",
            "g_weight",
            "target_variale",
            "predicted_unit_value",
            "l_value",
            "nw_ag_flag",
        )

        assert_frame_equal(actual_output, expected_output)
