from pathlib import Path

import pandas as pd
import pytest
from helper_functions import does_not_raise

from mbs_results.estimation.validate_estimation import validate_estimation


@pytest.fixture()
def filepath():
    return Path("tests/data/validate_estimation")


@pytest.fixture()
def validate_estimation_data(filepath):
    return pd.read_csv(filepath / "validate_estimation.csv")


@pytest.mark.parametrize(
    "design_weight,calibration_factor,expectation",
    [
        ("no_missing", "no_missing", does_not_raise()),
        ("no_missing", "one_missing", pytest.raises(ValueError)),
        ("one_missing", "no_missing", pytest.raises(ValueError)),
        ("one_missing", "one_missing", pytest.raises(ValueError)),
        ("no_missing", "zeros", pytest.raises(ValueError)),
    ],
)
def test_validate_estimation(
    validate_estimation_data,
    design_weight,
    calibration_factor,
    expectation,
):
    non_sampled_strata = [5002]

    with expectation:
        validate_estimation(
            validate_estimation_data,
            design_weight,
            calibration_factor,
            "strata",
            "auxiliary",
            "region",
            "sampled",
            non_sampled_strata,
        )
