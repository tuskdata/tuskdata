"""Post-download hooks — plugin-provided transformations.

Plugins can register hooks to process files after download.
Example: tusk-intel registers "intel:parse_dgii_rnc" to parse DGII CSV files.

Hook signature:
    async def my_hook(file_path: Path) -> Path:
        # Transform file, return output path
        return new_path
"""

from pathlib import Path

import structlog

log = structlog.get_logger("download_hooks")

# Global hook registry: {hook_id: async callable}
_hooks: dict[str, callable] = {}


def register_hook(hook_id: str, handler: callable):
    """Register a post-download hook.

    Args:
        hook_id: Unique hook identifier (e.g. "intel:parse_dgii_rnc")
        handler: Async function(file_path: Path) -> Path
    """
    _hooks[hook_id] = handler
    log.info("Post-download hook registered", hook=hook_id)


def get_hooks() -> dict[str, callable]:
    """Get all registered hooks."""
    return dict(_hooks)


async def run_post_hook(hook_id: str, file_path: Path) -> Path:
    """Execute a post-download hook.

    Args:
        hook_id: The hook to run
        file_path: Path to the downloaded file

    Returns:
        Path to the processed file (may be different from input)
    """
    handler = _hooks.get(hook_id)
    if handler is None:
        log.warning("Post-download hook not found", hook=hook_id)
        return file_path

    try:
        result = await handler(file_path)
        log.info("Post-download hook completed", hook=hook_id, output=str(result))
        return result if isinstance(result, Path) else file_path
    except Exception as e:
        log.error("Post-download hook failed", hook=hook_id, error=str(e))
        return file_path
