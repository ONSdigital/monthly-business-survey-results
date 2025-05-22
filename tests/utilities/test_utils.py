import pandas as pd
from pandas.testing import assert_frame_equal

from mbs_results.utilities.inputs import read_colon_separated_file
from mbs_results.utilities.utils import compare_two_dataframes


def test_read_colon_separated_file():
    headers = ["int", "str", "float", "period"]
    expected = pd.DataFrame(
        {
            "int": [1, 2, 3],
            "str": ["A", "B", "C"],
            "float": [1.0, 2.0, 3.0],
            "period": [202401, 202401, 202401],
        }
    )

    actual = read_colon_separated_file(
        "tests/data/utilities/read_colon_separated_file/colon_sep_202401", headers
    )

    assert_frame_equal(actual, expected)


def test_compare_two_dataframes():
    df1 = pd.read_csv("tests/data/utilities/utils/compare_two_dataframes_input.csv")
    df2 = df1.copy()

    # Modifying some rows in df2
    df2.loc[0:4, "frotover"] = [1000, 2000, 3000, 4000, 5000]
    df2.loc[0:4, "imputation_flags_adjustedresponse"] = ["mc", "mc", "mc", "mc", "mc"]

    # Creating the diff based on modified rows
    df1_diff = df1.loc[0:4]
    df2_diff = df2.loc[0:4]

    df1_diff["version"] = "df1"
    df2_diff["version"] = "df2"

    expected_diff = pd.concat([df1_diff, df2_diff])
    expected_column_diff = ["imputation_flags_adjustedresponse", "frotover"]

    actual_diff, actual_column_diff = compare_two_dataframes(df1, df2)

    assert_frame_equal(expected_diff, actual_diff)

    assert expected_column_diff == actual_column_diff
