import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal, assert_series_equal

from src.forward_link import calculate_imputation_link, mask_values


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

        mask_values(df_copy, "question", link_filter)

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

        mask_values(df_copy, ["question", "other"], link_filter)

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

        mask_values(df_copy, "question", link_filter)

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

        mask_values(df_copy, "question", link_filter)

        assert_frame_equal(df_copy, expected)


class TestLink:

    # from scenario 33_multi_variable_C_BI_R
    # We could parametrise this with more scenarios if needed
    df = pd.DataFrame(
        data={
            "identifier": [
                10001,
                10001,
                10001,
                10002,
                10002,
                10002,
                10001,
                10001,
                10001,
                10002,
                10002,
                10002,
                10005,
                10005,
                10005,
            ],
            "date": [
                202001,
                202002,
                202003,
                202001,
                202002,
                202003,
                202001,
                202002,
                202003,
                202001,
                202002,
                202003,
                202001,
                202002,
                202003,
            ],
            "group": [1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 1, 2, 2],
            "question": [
                547.0,
                362.0,
                895.0,
                381.0,
                573.0,
                214.0,
                961.0,
                267.0,
                314.0,
                555.0,
                628.0,
                736.0,
                np.nan,
                np.nan,
                100.0,
            ],
            "f_predictive_question": [
                np.nan,
                547.0,
                362.0,
                np.nan,
                381.0,
                573.0,
                np.nan,
                961.0,
                267.0,
                np.nan,
                555.0,
                628.0,
                np.nan,
                np.nan,
                np.nan,
            ],
            "b_predictive_question": [
                362.0,
                895.0,
                np.nan,
                573.0,
                214.0,
                np.nan,
                267.0,
                314.0,
                np.nan,
                628.0,
                736.0,
                np.nan,
                np.nan,
                100.0,
                np.nan,
            ],
            "f_matched_pair": [
                False,
                True,
                True,
                False,
                True,
                True,
                False,
                True,
                True,
                False,
                True,
                True,
                False,
                False,
                False,
            ],
            "b_matched_pair": [
                True,
                True,
                False,
                True,
                True,
                False,
                True,
                True,
                False,
                True,
                True,
                False,
                False,
                False,
                False,
            ],
        }
    )

    def test_forward_link(self):

        expected_f_link = pd.Series(
            [
                1.0,
                1.0075431034482758,
                1.186096256684492,
                1.0,
                1.0075431034482758,
                1.186096256684492,
                1.0,
                0.5903693931398417,
                1.1731843575418994,
                1.0,
                0.5903693931398417,
                1.1731843575418994,
                1.0,
                0.5903693931398417,
                1.1731843575418994,
            ]
        )

        f_link = calculate_imputation_link(
            self.df,
            ["group", "date"],
            "f_matched_pair",
            "question",
            "f_predictive_question",
        )

        assert_series_equal(f_link, expected_f_link)

    def test_backward_link(self):

        expected_b_link = pd.Series(
            [
                0.9925133689839573,
                0.8431018935978359,
                1.0,
                0.9925133689839573,
                0.8431018935978359,
                1.0,
                1.693854748603352,
                0.8523809523809524,
                1.0,
                1.693854748603352,
                0.8523809523809524,
                1.0,
                0.9925133689839573,
                0.8523809523809524,
                1.0,
            ]
        )

        b_link = calculate_imputation_link(
            self.df,
            ["group", "date"],
            "b_matched_pair",
            "question",
            "b_predictive_question",
        )

        assert_series_equal(b_link, expected_b_link)
