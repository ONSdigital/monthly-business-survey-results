from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.imputes_and_constructed_output import (
    get_imputes_and_constructed_output,
)


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/imputes_and_constructed_output")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(
        filepath / "imputes_and_constructed_input.csv",
        index_col=False,
        dtype={"period": str},
    )


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "imputes_and_constructed_output.csv", index_col=False)


@pytest.fixture
def config():
    return {
        "state": "frozen",
        "period": "period",
        "reference": "reference",
        "question_no": "questioncode",
        "target": "adjustedresponse",
        "current_period": "202202",
    }


class TestGrowthRatesOutput:
    def test_state_not_frozen(sample_data, config):
        config["state"] = "active"
        result = get_imputes_and_constructed_output(sample_data, **config)
        assert result is None

    def test_imputes_and_constructed_output(self, input_df, output_df, config):

        expected_output = output_df

        actual_output = get_imputes_and_constructed_output(input_df, **config)

        assert_frame_equal(actual_output, expected_output)
