import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.ocea_srs_outputs import produce_ocea_srs_outputs


@pytest.fixture(scope="class")
def ocea_srs_input(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "ocea_srs_outputs" / "ocea_srs_input.csv", index_col=False
    )


@pytest.fixture(scope="class")
def ocea_srs_output(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "ocea_srs_outputs" / "ocea_srs_output.csv", index_col=False
    )


class TestProduceOceaSrsOutputs:
    def test_produce_ocea_srs_outputs(self, ocea_srs_input, ocea_srs_output):
        config = {"sic": "frosic2007", "split_output_by_period": []}
        actual_output = produce_ocea_srs_outputs(ocea_srs_input, **config)

        expected_output = ocea_srs_output

        assert_frame_equal(actual_output, expected_output)
