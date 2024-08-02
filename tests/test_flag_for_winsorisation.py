import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.flag_for_winsorisation import winsorisation_flag


@pytest.fixture(scope="class")
def winsorisation_flag_test_data():
    return pd.read_csv(
        "tests/data/winsorisation/flag_data.csv",
        low_memory=False,
        usecols=lambda c: not c.startswith("Unnamed:"),
    )


class TestWinsorisationFlag:
    def test_winsorisation_flag(self, winsorisation_flag_test_data):
        df_expected_output = winsorisation_flag_test_data.copy()
        df_input = df_expected_output.drop(columns=["nw_ag_flag"])
        df_input = df_input[
            [
                "a_weight",
                "g_weight",
            ]
        ]
        df_output = winsorisation_flag(
            df=df_input, a_weight="a_weight", g_weight="g_weight"
        )

        assert_frame_equal(df_output, df_expected_output)
