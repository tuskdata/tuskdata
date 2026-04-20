"""Logging configuration for Tusk.

Env vars (read at setup_logging time):
    TUSK_DEBUG — if truthy, default level becomes DEBUG
    TUSK_LOG_LEVEL — override: debug, info, warning, error, critical
    TUSK_LOG_FORMAT — "console" (default, colored) or "json"
"""

import logging
import os
import structlog

_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "warn": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}


def setup_logging(debug: bool = False) -> None:
    """Configure structlog for the application."""

    level_override = os.environ.get("TUSK_LOG_LEVEL", "").strip().lower()
    if level_override in _LEVELS:
        log_level = _LEVELS[level_override]
    else:
        log_level = logging.DEBUG if debug else logging.INFO

    fmt = os.environ.get("TUSK_LOG_FORMAT", "console").strip().lower()
    if fmt == "json":
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None) -> structlog.BoundLogger:
    """Get a logger instance"""
    logger = structlog.get_logger()
    if name:
        logger = logger.bind(component=name)
    return logger
