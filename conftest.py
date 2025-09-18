from pathlib import Path

import pytest


@pytest.fixture(scope="module")
def estimation_data_dir():
    return Path("tests/data/estimation")


@pytest.fixture(scope="module")
def imputation_data_dir():
    return Path("tests/data/imputation")


@pytest.fixture(scope="module")
def outlier_data_dir():
    return Path("tests/data/outlier_detection")


@pytest.fixture(scope="module")
def outputs_data_dir():
    return Path("tests/data/outputs")


@pytest.fixture(scope="module")
def staging_data_dir():
    return Path("tests/data/staging")


@pytest.fixture(scope="module")
def utilities_data_dir():
    return Path("tests/data/utilities")
