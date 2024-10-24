from pathlib import Path

import pytest
from pandas.testing import assert_frame_equal

from mbs_results.imputation.construction_matches import flag_construction_matches
from tests.helper_functions import load_and_format


@pytest.fixture(scope="class")
def construction_test_data():
    return load_and_format(
        Path("tests/data/imputation/construction_matches/construction_matches.csv")
    )


class TestConstructionMatches:
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
