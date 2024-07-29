import pandas as pd
from pandas.testing import assert_frame_equal

from mbs_results.constrains import replace_values_index_based, sum_sub_df


class TestConstrains:
    def test_replace_values_index_base(self):

        df = pd.read_csv("tests/test_replace_values_index_based.csv", index_col=False)

        df = df.set_index(["question_no", "period", "reference"])

        df_in = df[["target"]].copy()

        df_expected = df[["expected", "constrain_marker"]].rename(
            columns={"expected": "target"}
        )
        df_expected.sort_index(inplace=True)

        replace_values_index_based(df_in, "target", 49, ">", 40)
        replace_values_index_based(df_in, "target", 90, ">=", 40)

        assert_frame_equal(df_in, df_expected)

    def test_sum_sub_df_46_47(self):

        df = pd.read_csv("tests/test_sum_sub_df.csv", index_col=False)

        df_in = (
            df.loc[df["question_no"] != 40]
            .drop(columns=["constrain_marker"])
            .set_index(["question_no", "period", "reference"])
        )

        expected_output = (
            df.loc[df["question_no"] == 40]
            .drop(columns=["question_no"])
            .reset_index(drop=True)
        )

        actual_ouput = (
            sum_sub_df(df_in, [46, 47])
            .sort_values(by="reference")
            .reset_index(drop=True)
        )

        assert_frame_equal(actual_ouput, expected_output)
