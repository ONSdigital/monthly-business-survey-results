from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.growth_rates_output import get_growth_rates_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/growth_rates_output")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "growth_rates_input.csv", index_col=False)


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "growth_rates_output.csv", index_col=False)


class TestGrowthRatesOutput:
    def test_growth_rates_output(self, input_df, output_df):

        config = {
            "question_no": "questioncode",
            "cell_number": "cell_no",
            "period": "period",
            "target": "adjustedresponse",
        }

        expected_output = output_df

        actual_output = get_growth_rates_output(input_df, **config)

        assert_frame_equal(actual_output, expected_output)
