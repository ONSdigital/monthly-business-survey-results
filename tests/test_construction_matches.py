from pathlib import Path

import pytest
from helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from src.construction_link import calculate_construction_link
from src.construction_matches import flag_construction_matches
from src.flag_and_count_matched_pairs import count_matches


@pytest.fixture(scope="class")
def construction_test_data():
    return load_and_format(Path("tests") / "construction_matches.csv")


class TestConstructionImputation:
    def test_construction_matches_flag(self, construction_test_data):
        expected_output = construction_test_data[
            [
                "target",
                "period",
                "auxiliary",
                "flag_construction_matches",
            ]
        ]

        input_data = expected_output.drop(columns=["flag_construction_matches"])
        actual_output = flag_construction_matches(
            input_data, "target", "period", "auxiliary"
        )

        assert_frame_equal(actual_output, expected_output)

    def test_construction_matches_count(self, construction_test_data):
        expected_output = construction_test_data[
            [
                "period",
                "flag_construction_matches",
                "strata",
                "count_construction_matches",
            ]
        ]

        input_data = expected_output.drop(columns=["count_construction_matches"])
        actual_output = count_matches(
            input_data,
            "flag_construction_matches",
            "period",
            "strata",
            "count_construction_matches",
        )

        assert_frame_equal(actual_output, expected_output)

    def test_construction_link(self, construction_test_data):
        expected_output = construction_test_data[
            [
                "target",
                "auxiliary",
                "flag_construction_matches",
                "strata",
                "period",
                "construction_link",
            ]
        ]

        input_data = expected_output.drop(columns=["construction_link"])
        actual_output = calculate_construction_link(
            input_data,
            "target",
            "auxiliary",
            "flag_construction_matches",
            "strata",
            "period",
        )

        assert_frame_equal(actual_output, expected_output)
