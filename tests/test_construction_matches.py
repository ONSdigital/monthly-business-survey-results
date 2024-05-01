import pytest
import pandas as pd

from pathlib import Path
from pandas.testing import assert_frame_equal

from src.construction_matches import flag_construction_matches

@pytest.fixture(scope="class")
def construction_test_data():
    dataframe = pd.read_csv(Path("tests")/"construction_matches.csv")
    dataframe["period"] = pd.to_datetime(dataframe["period"], format="%Y%m")
    return dataframe

class TestConstructionMatches:
    def test_construction_matches(self, construction_test_data):
        expected_output = construction_test_data[["target", "period", "auxiliary", "flag_construction_matches"]]
        input_data = expected_output.drop(labels=["flag_construction_matches"], axis=1)
        actual_output = flag_construction_matches(input_data, "target", "period", "auxiliary")
        assert_frame_equal(actual_output, expected_output)