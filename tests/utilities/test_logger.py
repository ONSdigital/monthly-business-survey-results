import logging
from mbs_results import logger


def test_logger_info(caplog):
    with caplog.at_level(logging.DEBUG):
        logger.info("This is a test log message: info")

    assert "This is a test log message: info" in caplog.text


def test_logger_error(caplog):
    with caplog.at_level(logging.DEBUG):
        logger.error("This is a test log message: error")

    assert "This is a test log message: error" in caplog.text


def test_logger_debug(caplog):
    with caplog.at_level(logging.DEBUG):
        logger.debug("This is a test log message: debug")

    assert "This is a test log message: debug" in caplog.text


def test_logger_warning(caplog):
    with caplog.at_level(logging.DEBUG):
        logger.warning("This is a test log message: warning")

    assert "This is a test log message: warning" in caplog.text


def test_logger_critical(caplog):
    with caplog.at_level(logging.DEBUG):
        logger.critical("This is a test log message: critical")

    assert "This is a test log message: critical" in caplog.text
