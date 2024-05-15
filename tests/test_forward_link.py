import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from src.forward_link import zerofy_values


class TestFilters:
    # based on 02_C_FI_input
    df = pd.DataFrame(
        data={
            "identifier": [20001, 20001, 20002, 20002, 20003, 20003, 20004, 20004],
            "date": [202001, 202002, 202001, 202002, 202001, 202002, 202001, 202002],
            "group": [100, 100, 100, 100, 100, 100, 100, 100],
            "question": [2536.0, 8283.0, 9113.0, 2970.0, 5644.0, 989.0, np.nan, np.nan],
            "other": [35, 35, 72, 72, 77, 77, 30, 30],
        }
    )

    def test_basic_filter(self):
        """Test a basic filter, filters questions with identifier different to 20001"""

        expected = pd.DataFrame(
            data={
                "identifier": [20001, 20001, 20002, 20002, 20003, 20003, 20004, 20004],
                "date": [
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                ],
                "group": [100, 100, 100, 100, 100, 100, 100, 100],
                "question": [0, 0, 9113.0, 2970.0, 5644.0, 989.0, np.nan, np.nan],
                "other": [35, 35, 72, 72, 77, 77, 30, 30],
            }
        )

        link_filter = "identifier != '20001'"

        df_copy = self.df.copy()

        zerofy_values(df_copy, "question", link_filter)

        assert_frame_equal(df_copy, expected)

    def test_basic_multiple_columns(self):
        """Test a basic filter in more than 1 column"""

        expected = pd.DataFrame(
            data={
                "identifier": [20001, 20001, 20002, 20002, 20003, 20003, 20004, 20004],
                "date": [
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                ],
                "group": [100, 100, 100, 100, 100, 100, 100, 100],
                "question": [0, 0, 9113.0, 2970.0, 5644.0, 989.0, np.nan, np.nan],
                "other": [0, 0, 72, 72, 77, 77, 30, 30],
            }
        )

        link_filter = "identifier != '20001'"

        df_copy = self.df.copy()

        zerofy_values(df_copy, ["question", "other"], link_filter)

        assert_frame_equal(df_copy, expected)

    def test_basic_multiple_values(self):
        """
        Test a filter in multiple values, filters questions which aren't
        in ('20001', '20002')
        """

        expected = pd.DataFrame(
            data={
                "identifier": [20001, 20001, 20002, 20002, 20003, 20003, 20004, 20004],
                "date": [
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                ],
                "group": [100, 100, 100, 100, 100, 100, 100, 100],
                "question": [0, 0, 0, 0, 5644.0, 989.0, np.nan, np.nan],
                "other": [35, 35, 72, 72, 77, 77, 30, 30],
            }
        )

        link_filter = "identifier not in ('20001', '20002')"

        df_copy = self.df.copy()

        zerofy_values(df_copy, "question", link_filter)

        assert_frame_equal(df_copy, expected)

    def test_multiple_filters(self):
        """
        Test multiple conditions, filters questions which aren't in date 202001
        and identifier in 20001 in the same time
        """

        expected = pd.DataFrame(
            data={
                "identifier": [20001, 20001, 20002, 20002, 20003, 20003, 20004, 20004],
                "date": [
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                    202001,
                    202002,
                ],
                "group": [100, 100, 100, 100, 100, 100, 100, 100],
                "question": [0, 8283.0, 9113.0, 2970.0, 5644.0, 989.0, np.nan, np.nan],
                "other": [35, 35, 72, 72, 77, 77, 30, 30],
            }
        )

        link_filter = "not(date == '202001' and identifier in ('20001'))"

        df_copy = self.df.copy()

        zerofy_values(df_copy, "question", link_filter)

        assert_frame_equal(df_copy, expected)
