"""
Logging utilities for SnowMapper pipeline.

Provides standardized logging configuration with both console and file output.
"""

import logging
import os
import sys
from pathlib import Path


def setup_logger(
    name: str,
    log_dir: str = None,
    level: int = None,
    console: bool = True,
    file: bool = True,
    overwrite: bool = True
) -> logging.Logger:
    """
    Configure logger with console and/or file handlers.

    Args:
        name: Logger name (typically __name__ or script name)
        log_dir: Directory for log files (None = no file logging)
        level: Logging level. If None, reads from SNOWMAPPER_LOG_LEVEL env var
               or defaults to INFO
        console: Enable console output
        file: Enable file output (requires log_dir)
        overwrite: If True, overwrite log file each run. If False, append.

    Returns:
        Configured logging.Logger instance
    """
    # Determine log level from env var or parameter
    if level is None:
        env_level = os.environ.get('SNOWMAPPER_LOG_LEVEL', 'INFO').upper()
        level = getattr(logging, env_level, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Prevent duplicate handlers on repeated calls
    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if file and log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # Single log file per module, overwritten each run
        log_file = log_path / f"{name}.log"
        file_mode = 'w' if overwrite else 'a'

        file_handler = logging.FileHandler(log_file, mode=file_mode)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def get_log_dir(domain_path: str) -> str:
    """
    Get the log directory for a given domain path.

    Creates logs/ subdirectory within the domain directory.

    Args:
        domain_path: Path to domain directory (e.g., './D2000')

    Returns:
        Path to logs directory
    """
    log_dir = Path(domain_path) / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)
    return str(log_dir)


class TqdmLoggingHandler(logging.Handler):
    """
    Logging handler that works with tqdm progress bars.

    Writes log messages using tqdm.write() to avoid breaking progress bar display.
    """
    def __init__(self, level=logging.NOTSET):
        super().__init__(level)

    def emit(self, record):
        try:
            from tqdm import tqdm
            msg = self.format(record)
            tqdm.write(msg)
        except Exception:
            self.handleError(record)


def setup_logger_with_tqdm(
    name: str,
    log_dir: str = None,
    level: int = None,
    file: bool = True,
    overwrite: bool = True
) -> logging.Logger:
    """
    Configure logger that works with tqdm progress bars.

    Uses TqdmLoggingHandler for console output to prevent progress bar corruption.

    Args:
        name: Logger name
        log_dir: Directory for log files
        level: Logging level (reads SNOWMAPPER_LOG_LEVEL if None)
        file: Enable file output
        overwrite: If True, overwrite log file each run. If False, append.

    Returns:
        Configured logging.Logger instance
    """
    if level is None:
        env_level = os.environ.get('SNOWMAPPER_LOG_LEVEL', 'INFO').upper()
        level = getattr(logging, env_level, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Use tqdm-compatible handler for console
    tqdm_handler = TqdmLoggingHandler()
    tqdm_handler.setLevel(level)
    tqdm_handler.setFormatter(formatter)
    logger.addHandler(tqdm_handler)

    if file and log_dir:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # Single log file per module, overwritten each run
        log_file = log_path / f"{name}.log"
        file_mode = 'w' if overwrite else 'a'

        file_handler = logging.FileHandler(log_file, mode=file_mode)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
