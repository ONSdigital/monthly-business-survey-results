import pandas as pd

from mbs_results.estimation.create_population_counts import (
    calculate_turnover_sum_count,
    create_population_count_output,
)


def test_calculate_turnover_sum_count():
    # Creating input data
    df = pd.DataFrame(
        {
            "frotover": [1, 2, 3, 4, 5],
            "reference": [1, 1, 2, 2, 3],
            "period": [1, 1, 1, 1, 1],
            "strata": ["A", "A", "B", "B", "C"],
        }
    )

    # producing output
    output = calculate_turnover_sum_count(df, "period", "strata", "population")

    # creating expected output
    expected_output = pd.DataFrame(
        {
            "period": [1, 1, 1],
            "strata": ["A", "B", "C"],
            "population_turnover_sum": [3, 7, 5],
            "population_count": [2, 2, 1],
        }
    )
    pd.testing.assert_frame_equal(output, expected_output)


def test_create_population_count_output():
    # Creating input data
    df = pd.DataFrame(
        {
            "frotover": [1, 2, 3, 4, 5],
            "reference": [1, 1, 2, 2, 3],
            "period": [1, 1, 1, 1, 1],
            "strata": ["A", "A", "B", "B", "C"],
            "is_sampled": [True, False, True, False, True],
        }
    )

    # producing output
    output = create_population_count_output(df, "period", "strata")

    # creating expected output
    expected_output = pd.DataFrame(
        {
            "period": [1, 1, 1],
            "strata": ["A", "B", "C"],
            "population_turnover_sum": [3, 7, 5],
            "population_count": [2, 2, 1],
            "sample_turnover_sum": [1, 3, 5],
            "sample_count": [1, 1, 1],
        }
    )
    pd.testing.assert_frame_equal(output, expected_output)
