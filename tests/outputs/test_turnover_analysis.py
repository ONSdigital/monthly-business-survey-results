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


@pytest.fixture(scope="class")
def turnover_analysis_output_combined(outputs_data_dir):
    return pd.read_csv(
        outputs_data_dir
        / "turnover_analysis"
        / "turnover_analysis_output_combined.csv",
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
        config = {
            "sic": "frosic2007",
            "split_turnover_output_by_period": True,
        }

        actual_output = create_turnover_output(
            additional_outputs_df_input_data,
            **config,
        )

        assert_frame_equal(actual_output["202301"], expected_output["202301"])
        assert_frame_equal(actual_output["202302"], expected_output["202302"])

    def test_turnover_analysis_no_split(
        self,
        additional_outputs_df_input_data,
        turnover_analysis_output_combined,
    ):
        config = {
            "sic": "frosic2007",
            "split_turnover_output_by_period": False,
        }

        actual_output = create_turnover_output(
            additional_outputs_df_input_data,
            **config,
        )

        assert_frame_equal(actual_output, turnover_analysis_output_combined)
