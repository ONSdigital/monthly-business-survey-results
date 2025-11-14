import logging

import pytest

from mbs_results import logger
from mbs_results.utilities.merge_two_config_files import merge_two_config_files


@pytest.fixture(autouse=True)
def setup_test_logger():
    """Configure logger for testing to work with caplog."""
    # Store original handlers and propagate setting
    original_handlers = logger.handlers.copy()
    original_propagate = logger.propagate

    # Clear handlers and enable propagation for caplog
    logger.handlers.clear()
    logger.propagate = True

    yield

    # Restore original logger state
    logger.handlers.clear()
    logger.handlers.extend(original_handlers)
    logger.propagate = original_propagate


def test_merge_two_config_files(monkeypatch):
    def mock_load_config(path):
        if path == "config_user.json":
            return {"key1": "value1"}
        elif path == "mbs_results/configs/config_dev.json":
            return {"key2": "value2"}
        else:
            raise ValueError("Unknown config path")

    monkeypatch.setattr(
        "mbs_results.utilities.merge_two_config_files.load_config", mock_load_config
    )

    config = merge_two_config_files(
        config_user_path="config_user.json",
        config_dev_path="mbs_results/configs/config_dev.json",
    )

    assert config == {"key1": "value1", "key2": "value2"}


def test_merge_two_config_files_error_handling(monkeypatch, caplog):
    def mock_load_config(path):
        if path == "config_user.json":
            raise Exception("User config error")
        elif path == "mbs_results/configs/config_dev.json":
            return {"key2": "value2"}
        else:
            raise ValueError("Unknown config path")

    monkeypatch.setattr(
        "mbs_results.utilities.merge_two_config_files.load_config", mock_load_config
    )

    # Set log level and ensure propagation
    caplog.set_level(logging.ERROR)

    with caplog.at_level(logging.ERROR):
        config = merge_two_config_files(
            config_user_path="config_user.json",
            config_dev_path="mbs_results/configs/config_dev.json",
        )

    assert config == {"key2": "value2"}
    assert (
        "Error loading user config from config_user.json: User config error"
        in caplog.text
    )
