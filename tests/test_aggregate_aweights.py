from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

#import new function here

@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/estimation")
  

@pytest.fixture(scope="class")
def aweights_test_input(filepath):
    return pd.read_csv(filepath / "class_aweights_input.csv", index_col=False)
  
  
@pytest.fixture(scope="class")
def aweights_test_output(filepath):
    return pd.read_csv(filepath / "class_aweights_output.csv", index_col=False)
  

class TestAggregateAWeights:
    def test_construction_matches_flag(self, construction_test_data):
        expected_output = aweights_test_output

        input_data = aweights_test_input
        
        actual_output = #Function with arguments to go here

        assert_frame_equal(actual_output, expected_output)  