import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outlier_detection.calculate_predicted_unit_value import (
    calculate_predicted_unit_value,
)


@pytest.fixture(scope="class")
def predicted_unit_value_test_data(outlier_data_dir):
    return pd.read_csv(
        outlier_data_dir
        / "calculate_predicted_unit_value"
        / "predicted_unit_value_data.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


@pytest.fixture(scope="class")
def predicted_unit_value_test_output(outlier_data_dir):
    return pd.read_csv(
        outlier_data_dir
        / "calculate_predicted_unit_value"
        / "predicted_unit_value_output.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


class TestPredictedUnitValue:
    def test_calculate_predicted_unit_value(
        self, predicted_unit_value_test_data, predicted_unit_value_test_output
    ):
        expected_output = predicted_unit_value_test_output[
            [
                "group",
                "period",
                "aux",
                "is_census",
                "a_weight",
                "target_variable",
                "nw_ag_flag",
                "predicted_unit_value",
            ]
        ]
        input_data = predicted_unit_value_test_data[
            [
                "group",
                "period",
                "aux",
                "is_census",
                "a_weight",
                "target_variable",
                "nw_ag_flag",
            ]
        ]

        actual_output = calculate_predicted_unit_value(
            input_data,
            "group",
            "period",
            "aux",
            "is_census",
            "a_weight",
            "target_variable",
            "nw_ag_flag",
        )

        assert_frame_equal(actual_output, expected_output)
