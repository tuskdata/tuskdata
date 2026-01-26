"""Dependency checking utilities for optional features"""

from importlib.util import find_spec
from typing import Literal

# Feature to required packages mapping
FEATURE_DEPS = {
    "studio": ["litestar", "granian", "duckdb", "polars", "jinja2"],
    "postgres": ["psycopg"],
    "admin": ["apscheduler", "psutil"],
    "cluster": ["datafusion"],
}

# Install commands for each feature
INSTALL_HINTS = {
    "studio": 'uv pip install "tuskdata[studio]"',
    "postgres": 'uv pip install "tuskdata[postgres]"',
    "admin": 'uv pip install "tuskdata[admin]"',
    "cluster": 'uv pip install "tuskdata[cluster]"',
    "all": 'uv pip install "tuskdata[all]"',
}


def is_available(package: str) -> bool:
    """Check if a package is installed"""
    return find_spec(package) is not None


def check_feature(feature: Literal["studio", "postgres", "admin", "cluster"]) -> tuple[bool, list[str]]:
    """Check if all dependencies for a feature are available

    Returns: (all_available, missing_packages)
    """
    required = FEATURE_DEPS.get(feature, [])
    missing = [pkg for pkg in required if not is_available(pkg)]
    return len(missing) == 0, missing


def require_feature(feature: Literal["studio", "postgres", "admin", "cluster"], exit_on_missing: bool = True) -> bool:
    """Require a feature, optionally exiting with helpful message if missing

    Returns True if feature is available, False otherwise
    """
    available, missing = check_feature(feature)

    if not available:
        msg = f"\n❌ Missing dependencies for '{feature}' feature: {', '.join(missing)}\n"
        msg += f"\nInstall with:\n  {INSTALL_HINTS.get(feature, INSTALL_HINTS['all'])}\n"

        if exit_on_missing:
            import sys
            print(msg, file=sys.stderr)
            sys.exit(1)
        else:
            return False

    return True


def get_available_features() -> dict[str, bool]:
    """Get availability status of all features"""
    return {
        feature: check_feature(feature)[0]
        for feature in FEATURE_DEPS.keys()
    }


def print_feature_status():
    """Print status of all optional features"""
    status = get_available_features()

    print("\n📦 Tusk Feature Status:\n")
    for feature, available in status.items():
        icon = "✅" if available else "❌"
        print(f"  {icon} {feature}")

    missing = [f for f, a in status.items() if not a]
    if missing:
        print(f"\nInstall missing features with:")
        print(f'  uv pip install "tuskdata[all]"')
        print(f"\nOr install individually:")
        for f in missing:
            print(f'  uv pip install "tuskdata[{f}]"')
    print()
