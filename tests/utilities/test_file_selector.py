import os
from unittest.mock import patch

import pytest

from mbs_results.utilities.file_selector import (
    find_files,
    generate_expected_periods,
    validate_files,
)


@pytest.fixture
def mock_config():
    """Fixture to provide a mock configuration dictionary for tests"""
    return {
        "population_prefix": "universe",
        "sample_prefix": "finalsel",
        "current_period": 201902,
        "revision_window": 5,
        "platform": "network",
        "folder_path": "tests/data/file_selector/",
    }


@patch("os.path.isfile")
def test_find_files_universe(mock_isfile, mock_config):
    """Test case where all expected files exits"""
    mock_isfile.return_value = True

    valid_files = find_files(
        file_path=mock_config["folder_path"],
        file_prefix=mock_config["population_prefix"],
        current_period=mock_config["current_period"],
        revision_window=mock_config["revision_window"],
        config=mock_config,
    )

    assert len(valid_files) == mock_config["revision_window"]
    assert all("universe023" in file for file in valid_files)


@patch("os.path.isfile")
def test_find_files_finalsel(mock_isfile, mock_config):
    """Test case where all expected finalsel files exist"""
    mock_isfile.return_value = True

    valid_files = find_files(
        file_path=mock_config["folder_path"],
        file_prefix=mock_config["sample_prefix"],
        current_period=mock_config["current_period"],
        revision_window=mock_config["revision_window"],
        config=mock_config,
    )

    assert len(valid_files) == mock_config["revision_window"]
    assert all("finalsel023" in file for file in valid_files)


def test_find_files_missing_universe(mock_config):
    """Test case where a universe file is missing by adding one to revision window"""

    with pytest.raises(
        FileNotFoundError, match="Missing universe file for periods: 201809"
    ):
        find_files(
            file_path=mock_config["folder_path"],
            file_prefix=mock_config["population_prefix"],
            current_period=mock_config["current_period"],
            revision_window=mock_config["revision_window"] + 1,
            config=mock_config,
        )


def test_find_files_missing_finalsel(mock_config):
    """Test case where a finalsel file is missing by adding two to revision window"""

    with pytest.raises(
        FileNotFoundError, match="Missing finalsel file for periods: 201808, 201809"
    ):
        find_files(
            file_path=mock_config["folder_path"],
            file_prefix=mock_config["sample_prefix"],
            current_period=mock_config["current_period"],
            revision_window=mock_config["revision_window"] + 2,
            config=mock_config,
        )


def test_generate_expected_periods():
    current_period = 202303
    revision_window = 3
    expected_periods = generate_expected_periods(current_period, revision_window)

    assert expected_periods == ["202303", "202302", "202301"]


@patch("os.path.isfile")
def test_validate_files(mock_isfile, mock_config):
    """Test the validate_files function"""
    mock_isfile.return_value = True
    file_dir = os.path.normpath("tests/data/file_selector")
    file_prefix = "universe023"
    expected_periods = ["201902", "201901", "201811", "201812", "201810"]

    expected_files = [
        os.path.normpath(os.path.join(file_dir, f"{file_prefix}_{period}"))
        for period in expected_periods
    ]
    valid_files = validate_files(
        file_path=file_dir,
        file_prefix=file_prefix,
        expected_periods=expected_periods,
        config=mock_config,
    )
    valid_files = [os.path.normpath(file) for file in valid_files]

    print(expected_files)
    print(valid_files)

    assert len(valid_files) == len(expected_periods)
    assert all(file in valid_files for file in expected_files)
