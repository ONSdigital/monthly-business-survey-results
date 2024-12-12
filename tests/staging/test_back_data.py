from contextlib import nullcontext as does_not_raise

import pandas as pd
import pytest

from mbs_results.staging.back_data import is_back_data_date_ok

testdata = [
    (
        pd.Series(list(pd.date_range("2019-02-01", "2019-02-01", freq="MS")) * 5),
        pd.Timestamp("2019-03-01 00:00:00"),
        20203,
        13,
        does_not_raise,
    ),  # all good
    (
        pd.Series(pd.date_range("2020-03-01", "2020-05-01", freq="MS")),
        pd.Timestamp("2019-03-01 00:00:00"),
        20203,
        13,
        ValueError,
    ),  # back data many dates
    (
        pd.Series(list(pd.date_range("2020-01-01", "2020-01-01", freq="MS")) * 5),
        pd.Timestamp("2019-03-01 00:00:00"),
        20203,
        13,
        ValueError,
    ),  # first period not next month of back data
    (
        pd.Series(list(pd.date_range("2019-02-01", "2019-02-01", freq="MS")) * 5),
        pd.Timestamp("2019-03-01 00:00:00"),
        20203,
        6,
        ValueError,
    ),  # revision doesn't match with back data
]


# Parametrize the test case
@pytest.mark.parametrize(
    "back_data_period,first_period,current_period,revision_period,expected", testdata
)
def test_is_back_data_date_ok_raises_error(
    back_data_period, first_period, current_period, revision_period, expected
):
    with pytest.raises(ValueError):
        is_back_data_date_ok(
            back_data_period, first_period, current_period, revision_period
        )
