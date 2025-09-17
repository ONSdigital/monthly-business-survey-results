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
        config = {"platform": "network", "bucket": ""}
        expected_output = output_df
        input_df["classification"] = input_df["classification"].astype(float)
        input_df["questioncode"] = input_df["questioncode"].astype(int)
        actual_output = create_csdb_output(
            input_df,
            Path(filepath / "cdid_mapping.csv"),
            **config,
        )

        assert_frame_equal(actual_output, expected_output)
