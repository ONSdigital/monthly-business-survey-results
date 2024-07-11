from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.calculate_predicted_unit_value import calculate_predicted_unit_value


@pytest.fixture(scope="class")
def predicted_unit_value_test_data():
    return pd.read_csv(
        Path("tests") / "data" / "winsorisation" / "predicted_unit_value.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


@pytest.fixture(scope="class")
def predicted_unit_value_test_output():
    return pd.read_csv(
        "tests/data/winsorisation/predicted_unit_value_output.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


class TestPredictedUnitValue:
    def test_calculate_predicted_unit_value(
        self, predicted_unit_value_test_output, predicted_unit_value_test_data
    ):
        expected_output = predicted_unit_value_test_output[
            [
                "period",
                "strata",
                "aux",
                "sampled",
                "a_weight",
                "target_variable",
                "nw_ag_flag",
                "predicted_unit_value",
            ]
        ]
        input_data = predicted_unit_value_test_data[
            [
                "period",
                "strata",
                "aux",
                "sampled",
                "a_weight",
                "target_variable",
                "nw_ag_flag",
                "predicted_unit_value",
            ]
        ]
        input_data = input_data.drop(columns=["predicted_unit_value"])
        actual_output = calculate_predicted_unit_value(
            input_data,
            "period",
            "strata",
            "aux",
            "sampled",
            "a_weight",
            "target_variale",
            "nw_ag_flag",
        )

        assert_frame_equal(actual_output, expected_output)
