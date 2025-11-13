import logging


def setup_logger(logger_name: str, logger_file_path: str) -> logging.Logger:
    """
    Sets up a logger with the specified name and file path.
    Call this function once at the start of the pipeline.

    Parameters:
        logger_name (str): The name of the logger.
        logger_file_path (str): The file path where the log file will be stored.

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(logger_name)

    # Avoid duplicate handlers if called multiple times or handlers already exist
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Create file handler which logs the messages to the file
        file_handler = logging.FileHandler(logger_file_path)
        file_handler.setLevel(logging.DEBUG)

        # Create console handler, which logs messages to the console/termina
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)

        # Create formatter and add it to the handlers
        logging_str = (
            "[%(asctime)s: %(name)s: %(levelname)s: %(module)s: "
            "%(funcName)s: %(lineno)d] %(message)s"
        )
        formatter = logging.Formatter(logging_str)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger
