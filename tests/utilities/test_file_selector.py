import pytest
from unittest.mock import patch
from mbs_results.utilities.file_selector import (
    find_files, generate_expected_periods
)


@pytest.fixture
def mock_config():
    """Fixture to provide a mock configuration dictionary for tests"""
    return {
        "population_path": "tests/data/file_selector/universe023_*",
        "sample_path": "tests/data/file_selector/finalsel023_*",
        "current_period": 201810,
        "revision_window": 5
    }


@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_success(mock_join, mock_isfile, mock_config):
    """Test case where all expected files exits"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.return_value = True  

    universe_files, finalsel_files = find_files(mock_config)

    assert len(universe_files) == mock_config["revision_window"]
    assert len(finalsel_files) == mock_config["revision_window"]
        

@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_missing_universe(mock_join, mock_isfile, mock_config):
    """Test case where a universe file is missing"""
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.side_effect = lambda path: "universe" not in path   

    with pytest.raises(
        FileNotFoundError, match="Missing universe file for period: 201810"
    ):
        find_files(mock_config)
            

@patch("os.path.isfile")
@patch("os.path.join")
def test_find_files_missing_finalsel(mock_join, mock_isfile, mock_config):
    mock_join.side_effect = lambda dir, file: f"{dir}/{file}"
    mock_isfile.side_effect = lambda path: "finalsel" not in path

    with pytest.raises(
        FileNotFoundError, match="Missing finalsel file for period: 201810"
    ):
        find_files(mock_config)
        
        
def test_generate_expected_periods():
    current_period = 202303
    revision_window = 3
    expected_periods = generate_expected_periods(current_period, revision_window)

    assert expected_periods == ["202303", "202304", "202305"]