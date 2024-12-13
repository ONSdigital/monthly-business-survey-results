import json
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.back_data import is_back_data_date_ok
from mbs_results.utilities.utils import convert_column_to_datetime


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/staging/back_data")


testdata = [
    (  # all good
        pd.Series(list(pd.date_range("2022-02-01", "2022-02-01", freq="MS")) * 5),
        pd.Timestamp("2022-03-01 00:00:00"),
        202303,
        13,
        None,
    ),
    (  # back data many dates
        pd.Series(pd.date_range("2022-03-01", "2022-05-01", freq="MS")),
        pd.Timestamp("2022-03-01 00:00:00"),
        202303,
        13,
        ValueError,
    ),
    (  # first period not next month of back data
        pd.Series(list(pd.date_range("2022-01-01", "2022-01-01", freq="MS")) * 5),
        pd.Timestamp("2022-03-01 00:00:00"),
        202303,
        13,
        ValueError,
    ),
    (  # revision doesn't match with back data
        pd.Series(list(pd.date_range("2022-02-01", "2022-02-01", freq="MS")) * 5),
        pd.Timestamp("2022-03-01 00:00:00"),
        202303,
        6,
        ValueError,
    ),
]


# Parametrize the test case
@pytest.mark.parametrize(
    "back_data_period,first_period,current_period,revision_period,expected", testdata
)
def test_is_back_data_date_ok_raises_error(
    back_data_period, first_period, current_period, revision_period, expected
):
    """
    Testing dates, if no Value error is raised then we check if True is
    returned, we used None(no errors raised) for the good scenario
    """

    if expected:
        with pytest.raises(expected):
            is_back_data_date_ok(
                back_data_period, first_period, current_period, revision_period
            )
    else:
        assert is_back_data_date_ok(
            back_data_period, first_period, current_period, revision_period
        )


@patch("mbs_results.staging.back_data.read_back_data")
def test_append_back_data(mock_read_back_data, filepath):
    """
    Testing when appending staged data with back data, we are mocking
    the read_back_data function, also using a dummy config with
    """
    mocked_back_data = pd.read_csv(filepath / "test_back_data.csv")
    mocked_back_data["period"] = convert_column_to_datetime(mocked_back_data["period"])
    mock_read_back_data.return_value = mocked_back_data

    staged = pd.read_csv(filepath / "test_staged.csv")
    staged["period"] = convert_column_to_datetime(staged["period"])

    expected_output = pd.read_csv(filepath / "test_append_back_expected.csv")
    expected_output["period"] = convert_column_to_datetime(expected_output["period"])

    with open(filepath / "test_config.json", "r") as f:
        config = json.load(f)

    from mbs_results.staging.back_data import append_back_data

    actual_output = append_back_data(staged, config)

    mock_read_back_data.assert_called_once_with(config)

    assert_frame_equal(actual_output, expected_output)
