"""PostgreSQL extension management"""

import msgspec
from ..core.connection import ConnectionConfig
from ..engines.postgres import execute_query


class Extension(msgspec.Struct):
    """PostgreSQL extension info"""
    name: str
    installed_version: str | None = None
    default_version: str | None = None
    description: str | None = None
    is_installed: bool = False


async def get_extensions(config: ConnectionConfig) -> list[Extension]:
    """Get all available extensions with their installation status"""

    # Query to get all available extensions and their installation status
    sql = """
    SELECT
        a.name,
        i.extversion as installed_version,
        a.default_version,
        a.comment as description,
        CASE WHEN i.extname IS NOT NULL THEN true ELSE false END as is_installed
    FROM pg_available_extensions a
    LEFT JOIN pg_extension i ON a.name = i.extname
    ORDER BY
        CASE WHEN i.extname IS NOT NULL THEN 0 ELSE 1 END,
        a.name
    """

    result = await execute_query(config, sql)

    extensions = []
    for row in result.rows:
        extensions.append(Extension(
            name=row[0],
            installed_version=row[1],
            default_version=row[2],
            description=row[3],
            is_installed=row[4]
        ))

    return extensions


async def get_installed_extensions(config: ConnectionConfig) -> list[Extension]:
    """Get only installed extensions"""

    sql = """
    SELECT
        e.extname as name,
        e.extversion as installed_version,
        a.default_version,
        a.comment as description,
        true as is_installed
    FROM pg_extension e
    LEFT JOIN pg_available_extensions a ON e.extname = a.name
    ORDER BY e.extname
    """

    result = await execute_query(config, sql)

    extensions = []
    for row in result.rows:
        extensions.append(Extension(
            name=row[0],
            installed_version=row[1],
            default_version=row[2],
            description=row[3],
            is_installed=True
        ))

    return extensions


async def install_extension(config: ConnectionConfig, name: str, schema: str | None = None) -> bool:
    """Install an extension"""

    # Validate extension name (prevent SQL injection)
    if not name.replace("_", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid extension name: {name}")

    sql = f'CREATE EXTENSION IF NOT EXISTS "{name}"'
    if schema:
        # Validate schema name
        if not schema.replace("_", "").isalnum():
            raise ValueError(f"Invalid schema name: {schema}")
        sql += f' SCHEMA "{schema}"'

    await execute_query(config, sql)
    return True


async def uninstall_extension(config: ConnectionConfig, name: str, cascade: bool = False) -> bool:
    """Uninstall an extension"""

    # Validate extension name (prevent SQL injection)
    if not name.replace("_", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid extension name: {name}")

    sql = f'DROP EXTENSION IF EXISTS "{name}"'
    if cascade:
        sql += " CASCADE"

    await execute_query(config, sql)
    return True


async def get_extension_details(config: ConnectionConfig, name: str) -> dict:
    """Get detailed information about an extension"""

    # Validate extension name
    if not name.replace("_", "").replace("-", "").isalnum():
        raise ValueError(f"Invalid extension name: {name}")

    sql = f"""
    SELECT
        a.name,
        a.default_version,
        a.comment as description,
        e.extversion as installed_version,
        e.extrelocatable as relocatable,
        n.nspname as schema
    FROM pg_available_extensions a
    LEFT JOIN pg_extension e ON a.name = e.extname
    LEFT JOIN pg_namespace n ON e.extnamespace = n.oid
    WHERE a.name = '{name}'
    """

    result = await execute_query(config, sql)

    if not result.rows:
        return {"error": f"Extension '{name}' not found"}

    row = result.rows[0]
    return {
        "name": row[0],
        "default_version": row[1],
        "description": row[2],
        "installed_version": row[3],
        "relocatable": row[4],
        "schema": row[5],
        "is_installed": row[3] is not None
    }
