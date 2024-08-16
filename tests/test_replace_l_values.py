from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from helper_functions import load_and_format

from mbs_results.replace_l_values import replace_l_values

@pytest.fixture(scope="class")
def replace_l_values_input_path():
    return load_and_format(
        Path("tests") / "data" / "winsorisation" / "replace_l_values_input.csv"
    )

class TestReplaceLValue:
    def test_overwrite_l_values(self, replace_l_values_input_path):
        df = replace_l_values_input_path
        df_input = df[["strata","question_no","period","l_value_input"]]
        df_input.rename(columns = {"l_value_input" : "l_value"},inplace=True)
        df_expected_output = df[["strata","question_no","period","l_value_output","replaced_l_value"]]
        df_expected_output.rename(columns = {"l_value_output" : "l_value"},inplace=True)
        replace_l_values_path = "tests/data/winsorisation/replace_l_values.csv"
        df_output = replace_l_values(
        df_input, "strata", "question_no", "l_value", replace_l_values_path
    )
        
        
        assert_frame_equal(df_expected_output, df_output)

