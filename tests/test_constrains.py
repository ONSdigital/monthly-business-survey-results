from pathlib import Path
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.constrains import replace_values_index_based


class TestConstrains:
    def test_replace_values_index_base(self):

        df = pd.read_csv("tests/test_replace_values_index_based.csv",index_col=False)

        df =  df.set_index(["question_no","period","reference"])

        df_in = df[['target']].copy()

        df_expected = df[['expected', 'constrain_marker']].rename(columns = {"expected":"target"})
        df_expected.sort_index(inplace=True)

        replace_values_index_based(df_in,"target",49,'>',40)
        replace_values_index_based(df_in,"target",90,'>=',40)

        assert_frame_equal(df_in, df_expected)

