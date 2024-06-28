from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.predictive_variable import shift_by_strata_period


@pytest.fixture(scope="class")
def predictive_variable_test_data():
    return load_and_format(Path("tests") / "predictive_variable.csv")


class TestPredictiveVariable:
    def test_predictive_variable_forward(self, predictive_variable_test_data):
        expected_output = predictive_variable_test_data[
            ["identifier", "period", "group", "question", "other", "f_predictive"]
        ]
        input_data = expected_output.drop(columns="f_predictive")
        actual_output = shift_by_strata_period(
            input_data, "question", "period", "group", "identifier", 1, "f_predictive"
        )
        actual_output.sort_index(ascending=True, inplace=True)
        assert_frame_equal(actual_output, expected_output)

    def test_predictive_variable_backward(self, predictive_variable_test_data):

        expected_output = predictive_variable_test_data[
            ["identifier", "period", "group", "question", "other", "b_predictive"]
        ]
        input_data = expected_output.drop(columns="b_predictive")
        actual_output = shift_by_strata_period(
            input_data, "question", "period", "group", "identifier", -1, "b_predictive"
        )
        actual_output.sort_index(ascending=True, inplace=True)
        assert_frame_equal(actual_output, expected_output)
