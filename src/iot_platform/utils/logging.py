import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """
    Configure application-wide structured logging.

    For local development we use readable text logs.
    Later this can emit JSON directly to CloudWatch.
    """

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        stream=sys.stdout,
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    """Return a logger for the specified module."""

    return logging.getLogger(name)