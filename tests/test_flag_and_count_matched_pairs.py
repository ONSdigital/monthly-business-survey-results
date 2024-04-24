import pandas as pd
import pytest
from src.flag_and_count_matched_pairs import flag_matched_pair_merge, count_matched_pair

file_name_cases = [
    ('case1_data.csv','case1_expected_output.csv'),
    ('case2_data.csv','case2_expected_output.csv')
    ]

@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_flag_matched_pair_merge(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file).drop('match_pair_count',axis = 1)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,1)
    df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)

@pytest.mark.parametrize("test_input_file , expected_output_file",file_name_cases)
def test_count_matched_pair(test_input_file,expected_output_file):
    df_expected_output = pd.read_csv(expected_output_file)
    df_input = pd.read_csv(test_input_file)
    df_output = flag_matched_pair_merge(df_input,1)
    df_output = count_matched_pair(df_output,'f_matched_pair')
    df_output.drop(['predictive_target_variable','predictive_period'],axis=1,inplace=True)
    assert df_output.equals(df_expected_output)


