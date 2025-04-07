import pandas as pd
from pandas.testing import assert_frame_equal

from mbs_results.utilities.inputs import read_colon_separated_file


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
