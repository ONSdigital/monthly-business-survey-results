import pandas as pd
import pytest
from src.flag_and_count_matched_pairs import flag_matched_pair_merge, count_matched_pair, flag_matched_pair_shift

# Case 1 - two businesses, one missing value
# Case 2 - change in stratum (sic) 
# Case 3 - Missing period for one business 


file_name_cases = [
    ('case1_data.csv','case1_expected_output.csv'),
    ('case2_data.csv','case2_expected_output.csv'),
    ('case3_data.csv','case3_expected_output.csv')

    ]

@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_flag_matched_pair_merge_forward(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file).drop(['f_matched_pair_count','b_matched_pair','b_matched_pair_count'],axis = 1)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,'f')
    # df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)

@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_flag_matched_pair_merge_backward(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file).drop(['f_matched_pair_count','f_matched_pair','b_matched_pair_count'],axis = 1)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,'b')
    # df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)

@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_count_matched_pair_forward(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file).drop(['b_matched_pair','b_matched_pair_count'],axis = 1)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,'f')
    df_output = count_matched_pair(df_output,'f_matched_pair')
    # df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)


@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_count_matched_pair_backward(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file).drop(['f_matched_pair','f_matched_pair_count'],axis = 1)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,'b')
    df_output = count_matched_pair(df_output,'b_matched_pair')
    # df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)

