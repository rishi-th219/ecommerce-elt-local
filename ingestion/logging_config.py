"""
Shared logging configuration for the ingestion package.

Every ingestion module logs through the standard library instead of print(), so
the same code produces readable output on a terminal and structured, timestamped
records in the Airflow task log. Verbosity is controlled with the INGESTION_LOG_LEVEL
environment variable (default INFO).
"""

import logging
import os
import sys

_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured once per process, writing to stdout.

    Airflow captures stdout from BashOperator tasks, so handlers are attached to
    the root logger only when nothing else has configured it -- this keeps the
    module importable from inside an Airflow worker without duplicating records.
    """
    level = os.getenv("INGESTION_LOG_LEVEL", "INFO").upper()

    root = logging.getLogger()
    if not root.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT))
        root.addHandler(handler)
        root.setLevel(level)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger
