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
        "population_path": "tests/data/file_selector/universe023_*",
        "sample_path": "tests/data/file_selector/finalsel023_*",
        "current_period": 201810,
        "revision_window": 5,
    }


@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_universe(mock_join, mock_isfile, mock_config):
    """Test case where all expected files exits"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.return_value = True

    valid_files = find_files(mock_config, file_type="universe")

    assert len(valid_files) == mock_config["revision_window"]
    assert all("universe023" in file for file in valid_files)


@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_finalsel(mock_join, mock_isfile, mock_config):
    """Test case where all expected finalsel files exist"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.return_value = True  # Simulate all files exist

    valid_files = find_files(mock_config, file_type="finalsel")

    assert len(valid_files) == mock_config["revision_window"]
    assert all("finalsel023" in file for file in valid_files)


@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_missing_universe(mock_join, mock_isfile, mock_config):
    """Test case where a universe file is missing"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.side_effect = lambda path: "universe" not in path

    with pytest.raises(
        FileNotFoundError, match="Missing universe file for period: 201810"
    ):
        find_files(mock_config, file_type="universe")


@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_missing_finalsel(mock_join, mock_isfile, mock_config):
    """Test case where a finalsel file is missing"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.side_effect = lambda path: "finalsel" not in path

    with pytest.raises(
        FileNotFoundError, match="Missing finalsel file for period: 201810"
    ):
        find_files(mock_config, file_type="finalsel")


def test_generate_expected_periods():
    current_period = 202303
    revision_window = 3
    expected_periods = generate_expected_periods(current_period, revision_window)

    assert expected_periods == ["202303", "202304", "202305"]


@patch("os.path.isfile")
def test_validate_files(mock_isfile):
    """Test the validate_files function"""
    mock_isfile.return_value = True
    file_dir = "tests/data/file_selector"
    file_prefix = "universe023"
    expected_periods = ["201810", "201811", "201812", "201901", "201902"]
    file_type = "universe"

    expected_files = [
        os.path.normpath(f"{file_dir}/{file_prefix}_{period}")
        for period in expected_periods
    ]
    print("expected files:")
    print(expected_files)
    valid_files = validate_files(file_dir, file_prefix, expected_periods, file_type)
    valid_files = [os.path.normpath(file) for file in valid_files]
    print("\nvalid_files:")
    print(valid_files)

    assert len(valid_files) == len(expected_periods)
    assert all(file in valid_files for file in expected_files)
