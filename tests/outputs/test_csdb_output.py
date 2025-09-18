import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.csdb_output import create_csdb_output


@pytest.fixture(scope="class")
def input_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "csdb_output" / "input_df.csv", index_col=False
    )


@pytest.fixture(scope="class")
def output_df(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "csdb_output" / "output_df.csv", index_col=False
    )


class TestCSDBOutput:
    def test_csdb_output(
        self,
        input_df,
        outputs_data_dir,
        output_df,
    ):
        config = {"platform": "network", "bucket": ""}

        expected_output = output_df
        input_df["classification"] = input_df["classification"].astype(float)
        input_df["questioncode"] = input_df["questioncode"].astype(int)
        actual_output = create_csdb_output(
            additional_outputs_df=input_df,
            cdid_data_path=outputs_data_dir / "csdb_output" / "cdid_mapping.csv",
            **config
        )

        assert_frame_equal(actual_output, expected_output)
