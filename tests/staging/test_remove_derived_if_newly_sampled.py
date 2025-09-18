import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from mbs_results.staging.stage_dataframe import remove_derived_if_newly_sampled


@pytest.fixture()
def previous_period_df():
    return pd.DataFrame(
        {
            "reference": [1, 2, 3, 5],
            "period": [202301, 202301, 202301, 202301],
            "target": [100, 200, 300, 0],
            "imputed_and_derived_flag": ["r", "d", "r", "r"],
        }
    )


@pytest.fixture()
def current_period_df():
    return pd.DataFrame(
        {
            "reference": [1, 3, 4, 5],
            "period": [202302, 202302, 202302, 202302],
            "target": [100, 200, 0, 0],
            "imputed_and_derived_flag": ["r", "d", "d", "d"],
        }
    )


@pytest.fixture()
def expected_df():
    return pd.DataFrame(
        {
            "reference": [1, 3, 4, 5],
            "period": [202302, 202302, 202302, 202302],
            "target": [100, 200, None, 0],
            "imputed_and_derived_flag": ["r", "d", None, "d"],
        }
    )


@pytest.fixture()
def mocked_config():
    return {
        "target": "target",
        "period": "period",
        "reference": "reference",
        "question_no": "questioncode",
    }


def test_remove_derived_if_newly_sampled(
    previous_period_df, current_period_df, expected_df, mocked_config
):
    previous_period = previous_period_df
    current_period = current_period_df
    expected = expected_df
    config = mocked_config

    actual = remove_derived_if_newly_sampled(
        df=current_period, previous_period_df=previous_period, config=config
    )

    assert_frame_equal(actual, expected, check_dtype=False)
