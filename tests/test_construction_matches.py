import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from src.construction_matches import find_construction_matches

@pytest.fixture 
def input_data():
    return pd.read_csv('tests/01_C_input.csv')

def test_construction_matches(input_data):
       assert isinstance(find_construction_matches(input_data, period="1"), pd.DataFrame)