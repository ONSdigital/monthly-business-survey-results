import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.outlier_detection.winsorisation import winsorise


@pytest.fixture(scope="class")
def expected_output(outlier_data_dir):
    return pd.read_csv(
        outlier_data_dir / "test_winsorisation" / "winsorised_weight_data_output.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


class TestWinsorisedWeight:
    def test_winsorised_weight(self, expected_output):

        input_data = expected_output[
            [
                "group",
                "period",
                "aux",
                "is_census",
                "a_weight",
                "g_weight",
                "target_variable",
                "l_value",
            ]
        ]

        actual_output = winsorise(
            input_data,
            "group",
            "period",
            "aux",
            "is_census",
            "a_weight",
            "g_weight",
            "target_variable",
            "l_value",
        )

        actual_output = actual_output[expected_output.columns]

        assert_frame_equal(actual_output, expected_output)
