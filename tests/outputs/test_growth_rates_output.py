import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.growth_rates_output import get_growth_rates_output


@pytest.fixture(scope="class")
def input_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "growth_rates_output" / "growth_rates_input.csv",
        index_col=False,
    )


@pytest.fixture(scope="class")
def output_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "growth_rates_output" / "growth_rates_output.csv",
        index_col=False,
    )


class TestGrowthRatesOutput:
    def test_growth_rates_output(self, input_df, output_df):
        config = {
            "question_no": "questioncode",
            "cell_number": "cell_no",
            "period": "period",
            "target": "adjustedresponse",
            "filter_out_questions": [11, 12, 146],
        }

        expected_output = output_df

        actual_output = get_growth_rates_output(input_df, **config)

        assert_frame_equal(actual_output, expected_output)
