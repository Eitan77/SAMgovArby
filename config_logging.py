"""Centralized logging configuration for SAMgovArby pipeline.

Features:
- --quiet: Suppress non-critical logs (show WARNING and ERROR only)
- --verbose: Show DEBUG logs
- --json: Output logs in JSON format for machine parsing
- LOGLEVEL env var: Override log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

Usage:
    from config_logging import setup_logging

    log = setup_logging(__name__, quiet=args.quiet, verbose=args.verbose, json_format=args.json)
    log.info("Starting pipeline...")
"""

import json
import logging
import os
import sys
from datetime import datetime


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def setup_logging(
    name: str,
    quiet: bool = False,
    verbose: bool = False,
    json_format: bool = False,
    level_override: str | None = None,
) -> logging.Logger:
    """Set up logging for a module.

    Args:
        name: Logger name (usually __name__)
        quiet: Suppress non-critical logs (WARNING and above)
        verbose: Show DEBUG logs
        json_format: Output JSON instead of text
        level_override: Override level via env var (LOGLEVEL)

    Returns:
        Configured logger instance
    """

    # Determine log level
    if level_override:
        level_str = level_override.upper()
    elif os.environ.get("LOGLEVEL"):
        level_str = os.environ["LOGLEVEL"].upper()
    elif quiet:
        level_str = "WARNING"
    elif verbose:
        level_str = "DEBUG"
    else:
        level_str = "INFO"

    level = getattr(logging, level_str, logging.INFO)

    # Get or create logger
    log = logging.getLogger(name)
    log.setLevel(level)

    # Remove any existing handlers to avoid duplicates
    log.handlers = []

    # Create handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    handler.flush = sys.stdout.flush  # Ensure logs flush immediately for GUI capture

    # Create formatter
    if json_format:
        formatter = JSONFormatter()
    else:
        if verbose:
            fmt = "%(asctime)s [%(levelname)s] %(name)s:%(lineno)d: %(message)s"
        else:
            fmt = "%(asctime)s [%(levelname)s] %(message)s"
        formatter = logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S")

    handler.setFormatter(formatter)
    log.addHandler(handler)

    return log


def add_verbosity_flags(parser):
    """Add standard verbosity flags to argparse parser.

    Args:
        parser: argparse.ArgumentParser instance
    """
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-critical logs (WARNING and above only)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Show DEBUG logs",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output logs in JSON format",
    )
