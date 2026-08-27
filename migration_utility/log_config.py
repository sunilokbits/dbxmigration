"""Centralized logging configuration for the Migration Utility."""
import logging
import os
from logging.handlers import RotatingFileHandler

_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
_LOG_FILE = os.path.join(_LOG_DIR, "migration_utility.log")
_LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
_CONFIGURED = False


def setup_logging():
    """Configure root logger with console + rotating file handlers. Idempotent."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    _CONFIGURED = True

    os.makedirs(_LOG_DIR, exist_ok=True)

    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-8s  [%(name)s]  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Rotating file handler — 5 MB × 3 backups
    fh = RotatingFileHandler(_LOG_FILE, maxBytes=5_242_880, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, _LOG_LEVEL, logging.INFO))
    ch.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(fh)
    root.addHandler(ch)

    # Quieten noisy libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a named logger. Call setup_logging() first (done automatically in app.py)."""
    return logging.getLogger(name)
