from unittest.mock import patch

import pandas as pd
import pytest

from mbs_results.estimation.create_population_counts import (
    calculate_turnover_sum_count,
    create_population_count_output,
    format_population_counts_mbs,
)


@pytest.fixture(scope="class")
def input_dataframe():
    return pd.DataFrame(
        {
            "frotover": [1, 2, 3, 4, 5],
            "reference": [1, 1, 2, 2, 3],
            "period": [1, 1, 1, 1, 1],
            "strata": ["A", "A", "B", "B", "C"],
            "is_sampled": [True, False, True, False, True],
        }
    )


@pytest.fixture(scope="class")
def expected_output_dataframe():
    return pd.DataFrame(
        {
            "period": [1, 1, 1],
            "strata": ["A", "B", "C"],
            "population_turnover_sum": [3, 7, 5],
            "population_count": [2, 2, 1],
            "sample_turnover_sum": [1, 3, 5],
            "sample_count": [1, 1, 1],
        }
    )


class TestTurnoverPopulationCounts:
    def test_calculate_turnover_sum_count(
        self, input_dataframe, expected_output_dataframe
    ):
        # producing output
        output = calculate_turnover_sum_count(
            input_dataframe, "period", "strata", "population"
        )

        # creating expected output by dropping unnecessary columns
        expected_output = expected_output_dataframe.copy().drop(
            columns=["sample_turnover_sum", "sample_count"]
        )
        pd.testing.assert_frame_equal(output, expected_output)

    @patch("pandas.DataFrame.to_csv")
    def test_create_population_count_output(
        self,
        mock_to_csv,
        input_dataframe,
        expected_output_dataframe,
    ):
        config = {
            "period": "period",
            "strata": "strata",
            "output_path": "",
            "platform": "network",
            "bucket": "",
        }
        # producing output
        output = create_population_count_output(input_dataframe, **config)

        pd.testing.assert_frame_equal(output, expected_output_dataframe)

    def test_format_population_counts_mbs(self):
        config = {
            "period": "period",
            "strata": "strata",
            "output_path": "tests/data/estimation/population_counts/",
            "platform": "network",
            "bucket": "",
            "current_period": "202202",
        }
        output_df, filename = format_population_counts_mbs(**config)

        expected = pd.read_csv(
            "tests/data/estimation/population_counts"
            + "/population_counts_formatted_output.csv"
        )

        pd.testing.assert_frame_equal(output_df, expected)
        assert filename == "mbs_population_counts_202202_202201.csv"
