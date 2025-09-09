from pathlib import Path

import pandas as pd
import pytest

from mbs_results.utilities.pounds_thousands import create_pounds_thousands_column


@pytest.fixture(scope="class")
def filepath():
    return Path("tests/data/outputs/additional_output_pounds_thousands")


@pytest.fixture(scope="class")
def input_df(filepath):
    return pd.read_csv(filepath / "pounds_thousands_input_mbs.csv", index_col=False)


@pytest.fixture(scope="class")
def expected_df(filepath):
    return pd.read_csv(filepath / "pounds_thousands_expected_mbs.csv", index_col=False)


@pytest.fixture(scope="class")
def test_config():
    return [40, 42, 43, 46, 47, 48, 49, 90]


class TestCreatePoundsThounsandsColumn:
    def test_transform_matches_expected_column(
        self, input_df, expected_df, test_config
    ):

        df_out = create_pounds_thousands_column(
            input_df,
            question_col="questioncode",
            source_col="adjustedresponse",
            dest_col="adjustedresponse_pounds_thousands",
            questions_to_apply=test_config,
        )

        # compare only adjustedresponse_pounds_thousands, aligned by questioncode
        pd.testing.assert_series_equal(
            df_out["adjustedresponse_pounds_thousands"]
            .astype(str)
            .reset_index(drop=True),
            expected_df["adjustedresponse_pounds_thousands"]
            .astype(str)
            .reset_index(drop=True),
            check_names=False,
        )
