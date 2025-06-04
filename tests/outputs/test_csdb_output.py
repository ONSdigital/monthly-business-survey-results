from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.csdb_output import create_csdb_output


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/csdb_output")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "input_df.csv", index_col=False)


@pytest.fixture(scope="class")
def output_df(filepath):
    return pd.read_csv(filepath / "output_df.csv", index_col=False)


class TestCSDBOutput:
    def test_csdb_output(
        self,
        input_df,
        filepath,
        output_df,
    ):
        expected_output = output_df

        actual_output = create_csdb_output(
            additional_outputs_df=input_df,
            cdid_data_path=Path(filepath / "cdid_mapping.csv"),
        )

        assert_frame_equal(actual_output, expected_output)
