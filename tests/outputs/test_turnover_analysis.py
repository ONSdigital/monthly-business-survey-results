import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outputs.turnover_analysis import create_turnover_output


@pytest.fixture(scope="class")
def additional_outputs_df_input_data(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "turnover_analysis" / "additional_outputs_df_input.csv",
        index_col=False,
    )


@pytest.fixture(scope="class")
def turnover_analysis_output_202301(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "turnover_analysis" / "turnover_analysis_output_202301.csv",
        index_col=False,
    )


@pytest.fixture(scope="class")
def turnover_analysis_output_202302(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir / "turnover_analysis" / "turnover_analysis_output_202302.csv",
        index_col=False,
    )


class TestTurnoverAnalysis:
    def test_turnover_analysis(
        self,
        additional_outputs_df_input_data,
        turnover_analysis_output_202301,
        turnover_analysis_output_202302,
    ):
        expected_output = {
            "202301": turnover_analysis_output_202301,
            "202302": turnover_analysis_output_202302,
        }

        actual_output = create_turnover_output(
            additional_outputs_df_input_data,
            sic="frosic2007",
        )

        assert_frame_equal(actual_output["202301"], expected_output["202301"])
        assert_frame_equal(actual_output["202302"], expected_output["202302"])
