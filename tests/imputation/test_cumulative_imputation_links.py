from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.imputation.cumulative_imputation_links import get_cumulative_links


@pytest.fixture(scope="class")
def cumulative_links_test_data():
    return load_and_format(Path("tests") / "cumulative_links.csv")


class TestCumulativeLinks:
    def test_get_cumulative_links_forward(self, cumulative_links_test_data):
        input_data = cumulative_links_test_data.drop(
            columns=[
                "cumulative_backward_imputation_link",
                "cumulative_forward_imputation_link",
                "imputation_group",
            ]
        )

        expected_output = cumulative_links_test_data.drop(
            columns=["imputation_group", "cumulative_backward_imputation_link"]
        )

        actual_output = get_cumulative_links(
            input_data,
            "f",
            "strata",
            "reference",
            "target",
            "period",
            "forward_imputation_link",
            1,
        )

        actual_output = actual_output.drop(
            columns=["imputation_group", "missing_value"]
        )

        assert_frame_equal(actual_output, expected_output)

    def test_get_cumulative_links_backward(self, cumulative_links_test_data):
        input_data = cumulative_links_test_data.drop(
            columns=[
                "cumulative_backward_imputation_link",
                "cumulative_forward_imputation_link",
                "imputation_group",
            ]
        )

        expected_output = cumulative_links_test_data.drop(
            columns=["imputation_group", "cumulative_forward_imputation_link"]
        )

        actual_output = get_cumulative_links(
            input_data,
            "b",
            "strata",
            "reference",
            "target",
            "period",
            "backward_imputation_link",
            1,
        )

        actual_output = actual_output.drop(
            columns=["imputation_group", "missing_value"]
        )

        assert_frame_equal(actual_output, expected_output)
