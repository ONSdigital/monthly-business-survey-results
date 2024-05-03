import pandas as pd
import pytest
from src.flag_and_count_matched_pairs import flag_matched_pair_merge, count_matched_pair, flag_matched_pair_shift
from pandas.testing import assert_frame_equal

# Case 1 - two businesses, one missing value
# Case 2 - change in strata (sic) 
# Case 3 - Missing period for one business 


file_name_cases = [
    ('tests/case1_expected_output.csv'),
    ('tests/case2_expected_output.csv'),
    ('tests/case3_expected_output.csv')
    ]

def load_and_format(filename):
    df_loaded = pd.read_csv(filename)
    df_loaded['period'] = pd.to_datetime(df_loaded['period'], format='%Y%m')
    return df_loaded

pytestmark = pytest.mark.parametrize("expected_output_file",file_name_cases)

class TestClass:
    def test_flag_matched_pair_merge_forward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['f_matched_pair_count','b_matched_pair','b_matched_pair_count'],axis = 1,inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair_merge(df_input, 'f', 'target_variable', 'period', 'reference', 'strata')
        df_output.drop(['predictive_target_variable'],axis = 1, inplace=True)
        assert_frame_equal(df_output, df_expected_output)

    def test_flag_matched_pair_merge_backward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['f_matched_pair_count', 'f_matched_pair', 'b_matched_pair_count'], axis = 1, inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair_merge(df_input, 'b', 'target_variable', 'period', 'reference', 'strata')
        df_output.drop(['predictive_target_variable'],axis = 1, inplace=True)
        assert_frame_equal(df_output, df_expected_output)

    def test_count_matched_pair_forward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['b_matched_pair','b_matched_pair_count'],axis = 1,inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable', 'f_matched_pair']]
        df_output = count_matched_pair(df_input,'f_matched_pair','period','strata')
        assert_frame_equal(df_output, df_expected_output)

    def test_count_matched_pair_backward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['f_matched_pair','f_matched_pair_count'],axis = 1,inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable', 'b_matched_pair']]
        df_output = count_matched_pair(df_input,'b_matched_pair','period','strata')
        assert_frame_equal(df_output, df_expected_output)

    def test_flag_matched_pair_shift_forward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['f_matched_pair_count','b_matched_pair','b_matched_pair_count'],axis = 1,inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair_shift(df_input,'f','target_variable','period', 'reference', 'strata')
        df_output.drop(['predictive_target_variable'],axis=1,inplace=True)
        assert_frame_equal(df_output, df_expected_output)

    def test_flag_matched_pair_shift_backward(self, expected_output_file):
        df_expected_output = load_and_format(expected_output_file)
        df_expected_output.drop(['f_matched_pair_count','f_matched_pair','b_matched_pair_count'],axis = 1,inplace=True)
        df_input = df_expected_output[['reference', 'strata', 'period', 'target_variable']]
        df_output = flag_matched_pair_shift(df_input,'b','target_variable','period', 'reference', 'strata')
        df_output.drop(['predictive_target_variable'],axis=1,inplace=True)
        assert_frame_equal(df_output, df_expected_output)