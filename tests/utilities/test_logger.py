import pytest

from mbs_results import configure_logger_with_run_id, logger


@pytest.fixture(autouse=True)
def setup_test_logger(caplog):
    """
    Configure logger for testing and ensure integration with pytest's caplog.

    - Removes existing handlers
    - Sets up clean test configuration
    - Ensures logs go to caplog
    - Cleans up after test
    """
    # Create minimal test config
    test_config = {"run_id": "test-run", "platform": "network", "output_path": None}

    # Remove existing handlers
    logger.handlers.clear()

    # Ensure logger propagates to root (needed for caplog to work)
    logger.propagate = True

    # Configure with test settings
    configure_logger_with_run_id(test_config)

    yield

    # Cleanup
    logger.handlers.clear()
    logger.propagate = False


def test_logger_info(caplog):
    """Test INFO level logging."""
    logger.info("This is a test log message: info")
    assert "This is a test log message: info" in caplog.text


def test_logger_error(caplog):
    """Test ERROR level logging."""
    logger.error("This is a test log message: error")
    assert "This is a test log message: error" in caplog.text


def test_logger_debug(caplog):
    """Test DEBUG level logging."""
    logger.debug("This is a test log message: debug")
    assert "This is a test log message: debug" in caplog.text


def test_logger_warning(caplog):
    """Test WARNING level logging."""
    logger.warning("This is a test log message: warning")
    assert "This is a test log message: warning" in caplog.text


def test_logger_critical(caplog):
    """Test CRITICAL level logging."""
    logger.critical("This is a test log message: critical")
    assert "This is a test log message: critical" in caplog.text
