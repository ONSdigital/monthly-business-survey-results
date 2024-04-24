import pytest
import pandas as pd

@pytest.fixture 
def input_data():
    return pd.read_csv('tests/01_C_input.csv')

def test_construction_matched_pairs(input_data):
       assert find_construction_matched_pairs is not None