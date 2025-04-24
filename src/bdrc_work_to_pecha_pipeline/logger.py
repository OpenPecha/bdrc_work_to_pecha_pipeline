"""
Logging configuration module for the bdrc_work_to_pecha_pipeline package.
Provides a centralized logging setup that can be imported and used across the project.
"""
import logging
import os
import sys
from pathlib import Path


def setup_logger():
    """
    Set up and configure the logger with both file and console handlers.
    Returns the configured root logger.
    """
    # Ensure log directory exists
    # Use absolute path for the project root
    base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent.parent
    log_dir = base_dir / "logs"  # Create a dedicated logs directory
    log_dir.mkdir(parents=True, exist_ok=True)

    # Set up the logger with absolute path
    log_file = log_dir / "pipeline.log"
    print(f"Log file will be created at: {log_file}")  # Print the log file location

    # Reset the root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    # Configure root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create file handler
    file_handler = logging.FileHandler(str(log_file))
    file_handler.setLevel(logging.INFO)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)

    # Create formatter and add it to the handlers
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


# Initialize the logger when the module is imported
logger = setup_logger()


def get_logger(name):
    """
    Get a logger with the specified name.
    This is a convenience function for getting a logger with the correct name.

    Args:
        name: The name for the logger, typically __name__ from the calling module

    Returns:
        A configured logger instance
    """
    return logging.getLogger(name)
