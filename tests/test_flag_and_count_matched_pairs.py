import pandas as pd
import pytest

from pandas.testing import assert_frame_equal
from pathlib import Path

from src.flag_and_count_matched_pairs import flag_matched_pair
from helper_functions import load_and_format

filepath = Path('tests')/'test_data_matched_pair/expected_output.csv'

@pytest.fixture(scope="class")
def match_test_data():
    return load_and_format(Path('tests')/'test_data_matched_pair/expected_output.csv')


class TestMatchedPair:
    def test_flag_matched_pair_forward(self, match_test_data):
        expected_output = match_test_data.drop(['b_match'],axis = 1)
        df_input = match_test_data[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair(df_input,'f','target_variable','period', 'reference', 'strata')
        assert_frame_equal(df_output, expected_output)

    def test_flag_matched_pair_backward(self, match_test_data):
        expected_output = match_test_data.drop(['f_match'],axis = 1)
        df_input = match_test_data[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair(df_input,'b','target_variable','period', 'reference', 'strata')
        assert_frame_equal(df_output, expected_output)