import logging

import pytest

from mbs_results.utilities.setup_logger import setup_logger


@pytest.fixture(autouse=True)
def setup_test_logger():
    """Configure logger for testing to work with caplog."""
    # Create a test logger
    test_logger = setup_logger(logger_file_path=None)

    # Store original state
    original_handlers = test_logger.handlers.copy()
    original_propagate = test_logger.propagate

    # Setup for testing
    test_logger.handlers.clear()
    test_logger.propagate = True
    test_logger.setLevel(logging.DEBUG)

    yield test_logger

    # Restore original state
    test_logger.handlers.clear()
    test_logger.handlers.extend(original_handlers)
    test_logger.propagate = original_propagate


def test_logger_info(caplog):
    logger = setup_logger(logger_file_path=None)
    with caplog.at_level(logging.DEBUG):
        logger.info("This is a test log message: info")

    assert "This is a test log message: info" in caplog.text


def test_logger_error(caplog):
    logger = setup_logger(logger_file_path=None)
    with caplog.at_level(logging.DEBUG):
        logger.error("This is a test log message: error")

    assert "This is a test log message: error" in caplog.text


def test_logger_debug(caplog):
    logger = setup_logger(logger_file_path=None)
    with caplog.at_level(logging.DEBUG):
        logger.debug("This is a test log message: debug")

    assert "This is a test log message: debug" in caplog.text


def test_logger_warning(caplog):
    logger = setup_logger(logger_file_path=None)
    with caplog.at_level(logging.DEBUG):
        logger.warning("This is a test log message: warning")

    assert "This is a test log message: warning" in caplog.text


def test_logger_critical(caplog):
    logger = setup_logger(logger_file_path=None)
    with caplog.at_level(logging.DEBUG):
        logger.critical("This is a test log message: critical")

    assert "This is a test log message: critical" in caplog.text
