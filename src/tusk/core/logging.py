"""Logging configuration for Tusk"""

import logging
import structlog


def setup_logging(debug: bool = False) -> None:
    """Configure structlog for the application"""

    # Determine log level
    log_level = logging.DEBUG if debug else logging.INFO

    # Configure structlog
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(colors=True)
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
