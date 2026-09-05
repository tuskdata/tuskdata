"""Tusk - PostgreSQL admin and SQL studio with an AI copilot."""

# Resolve from the installed package metadata so version drift between
# pyproject.toml and __init__.py is impossible. Falls back to the literal
# only when running from a source tree that hasn't been installed yet
# (e.g. `python -m tusk` straight from a clone).
try:
    from importlib.metadata import PackageNotFoundError, version as _pkg_version
    try:
        __version__ = _pkg_version("tuskdata")
    except PackageNotFoundError:
        __version__ = "0.0.0+dev"
    del PackageNotFoundError, _pkg_version
except ImportError:  # pragma: no cover — Py < 3.8, not supported
    __version__ = "0.0.0+dev"
