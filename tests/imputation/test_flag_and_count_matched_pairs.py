from pathlib import Path

import pandas as pd  # noqa F401
import pytest
from tests.helper_functions import load_and_format
from pandas.testing import assert_frame_equal

from mbs_results.imputation.flag_and_count_matched_pairs import (
    count_matches,
    flag_matched_pair,
)

match_inputs = [
    load_and_format(
        Path("tests/data/imputation") / "flag_and_count_matched_pairs/flag_pairs_2_groups_expected_output.csv"
    ),
    load_and_format(
        Path("tests/data/imputation")
        / "flag_and_count_matched_pairs/flag_pairs_missing_rows_expected_output.csv"
    ),
]


@pytest.fixture(scope="class")
def count_test_data():
    return load_and_format(
        Path("tests/data/imputation") / "flag_and_count_matched_pairs/count_matches_input.csv"
    )


@pytest.fixture(scope="class")
def count_expected_output():
    return load_and_format(
        Path("tests/data/imputation") / "flag_and_count_matched_pairs/count_matches_expected_output.csv"
    )


@pytest.mark.parametrize("match_test_data", match_inputs)
class TestMatchedPair:
    def test_flag_matched_pair_forward(self, match_test_data):
        expected_output = match_test_data.drop(["b_match_target_variable"], axis=1)
        df_input = match_test_data[
            ["reference", "strata", "period", "target_variable"]
        ].sample(frac=1)
        df_output = flag_matched_pair(
            df_input, "f", "target_variable", "period", "reference", "strata"
        )
        assert_frame_equal(df_output, expected_output)

    def test_flag_matched_pair_backward(self, match_test_data):
        expected_output = match_test_data.drop(["f_match_target_variable"], axis=1)
        df_input = match_test_data[["reference", "strata", "period", "target_variable"]]
        df_output = flag_matched_pair(
            df_input, "b", "target_variable", "period", "reference", "strata"
        )
        assert_frame_equal(df_output, expected_output)


class TestCountMatches:
    def test_count_matches(self, count_test_data, count_expected_output):
        output = count_matches(count_test_data, ["flag_1", "flag_2"], "period", "group")
        assert_frame_equal(output, count_expected_output)
