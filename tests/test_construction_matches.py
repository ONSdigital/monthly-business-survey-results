import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from src.construction_matches import flag_construction_matches

@pytest.fixture 
def case_1_data():
    return pd.read_csv('tests/case_1.csv')

def test_construction_matches(case_1_data):
    input_data = case_1_data.drop(labels=["flag_construction_matches"], axis=1)
    expected_output = case_1_data
    actual_output = flag_construction_matches(input_data, "auxiliary_variable", "period")

    assert_frame_equal(actual_output, expected_output)