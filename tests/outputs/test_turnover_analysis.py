from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.turnover_analysis import create_turnover_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/turnover_analysis")


@pytest.fixture(scope="class")
def cp_input_data(filepath):
    return pd.read_csv(filepath / "cp_input.csv", index_col=False)


@pytest.fixture(scope="class")
def qv_input_data(filepath):
    return pd.read_csv(filepath / "qv_input.csv", index_col=False)


@pytest.fixture(scope="class")
def finalsel_input_data(filepath):
    return pd.read_csv(filepath / "finalsel_input.csv", index_col=False)


@pytest.fixture(scope="class")
def winsorisation_input_data(filepath):
    return pd.read_csv(filepath / "winsorisation_input.csv", index_col=False)


@pytest.fixture(scope="class")
def turnover_analysis_output(filepath):
    return pd.read_csv(filepath / "turnover_analysis_output.csv", index_col=False)


class TestTurnoverAnalysis:
    def test_turnover_analysis(
        self,
        cp_input_data,
        qv_input_data,
        finalsel_input_data,
        winsorisation_input_data,
        turnover_analysis_output,
    ):
        expected_output = turnover_analysis_output

        actual_output = create_turnover_output(
            cp_input_data,
            qv_input_data,
            finalsel_input_data,
            winsorisation_input_data,
            "period_x",
            202301,
        )

        assert_frame_equal(actual_output, expected_output)
