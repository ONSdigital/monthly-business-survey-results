from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.turnover_analysis import create_turnover_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/turnover_analysis")


@pytest.fixture(scope="class")
def additional_outputs_df_input_data(filepath):
    return pd.read_csv(filepath / "additional_outputs_df_input.csv", index_col=False)


@pytest.fixture(scope="class")
def turnover_analysis_output(filepath):
    return pd.read_csv(filepath / "turnover_analysis_output.csv", index_col=False)


class TestTurnoverAnalysis:
    def test_turnover_analysis(
        self,
        additional_outputs_df_input_data,
        turnover_analysis_output,
    ):
        expected_output = turnover_analysis_output

        actual_output = create_turnover_output(
            additional_outputs_df_input_data,
            202301,
            sic="frosic2007",
        )

        assert_frame_equal(actual_output, expected_output)
