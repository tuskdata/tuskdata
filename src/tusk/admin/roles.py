"""PostgreSQL role/user management"""

import re
from datetime import datetime
import msgspec
from ..core.connection import ConnectionConfig
from ..engines.postgres import execute_query

# Valid role name: alphanumeric, underscores, hyphens (no SQL special chars)
_VALID_ROLE_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_-]*$')


def _validate_valid_until(value: str) -> bool:
    """Validate that valid_until is a proper timestamp string."""
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False


class Role(msgspec.Struct):
    """PostgreSQL role info"""
    name: str
    is_superuser: bool = False
    can_login: bool = False
    can_create_db: bool = False
    can_create_role: bool = False
    connection_limit: int = -1
    valid_until: str | None = None
    member_of: list[str] | None = None
    config: list[str] | None = None


async def get_roles(config: ConnectionConfig) -> list[Role]:
    """Get all roles in the database"""

    sql = """
    SELECT
        r.rolname as name,
        r.rolsuper as is_superuser,
        r.rolcanlogin as can_login,
        r.rolcreatedb as can_create_db,
        r.rolcreaterole as can_create_role,
        r.rolconnlimit as connection_limit,
        r.rolvaliduntil::text as valid_until,
        ARRAY(
            SELECT b.rolname
            FROM pg_catalog.pg_auth_members m
            JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
            WHERE m.member = r.oid
        ) as member_of,
        r.rolconfig as config
    FROM pg_catalog.pg_roles r
    WHERE r.rolname !~ '^pg_'
    ORDER BY r.rolcanlogin DESC, r.rolname
    """

    result = await execute_query(config, sql)

    roles = []
    for row in result.rows:
        roles.append(Role(
            name=row[0],
            is_superuser=row[1],
            can_login=row[2],
            can_create_db=row[3],
            can_create_role=row[4],
            connection_limit=row[5],
            valid_until=row[6],
            member_of=row[7] if row[7] else [],
            config=row[8] if row[8] else []
        ))

    return roles


async def create_role(
    config: ConnectionConfig,
    name: str,
    password: str | None = None,
    login: bool = True,
    superuser: bool = False,
    createdb: bool = False,
    createrole: bool = False,
    inherit: bool = True,
    connection_limit: int = -1,
    valid_until: str | None = None,
    in_roles: list[str] | None = None
) -> dict:
    """Create a new role"""

    # Validate role name (prevent SQL injection)
    if not _VALID_ROLE_RE.match(name) or len(name) > 63:
        return {"success": False, "error": f"Invalid role name: {name}"}

    options = []
    if login:
        options.append("LOGIN")
    else:
        options.append("NOLOGIN")

    if superuser:
        options.append("SUPERUSER")
    if createdb:
        options.append("CREATEDB")
    if createrole:
        options.append("CREATEROLE")
    if inherit:
        options.append("INHERIT")
    else:
        options.append("NOINHERIT")

    if connection_limit >= 0:
        options.append(f"CONNECTION LIMIT {int(connection_limit)}")

    if valid_until:
        if not _validate_valid_until(valid_until):
            return {"success": False, "error": "Invalid valid_until format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"}
        options.append(f"VALID UNTIL '{valid_until}'")

    sql = f'CREATE ROLE "{name}"'
    if options:
        sql += " WITH " + " ".join(options)

    try:
        await execute_query(config, sql)

        # Set password separately using parameterized query via psycopg
        # DDL can't use $1 params, but ALTER ROLE ... PASSWORD can use
        # encrypted password via psycopg's safe escaping
        if password:
            import psycopg
            dsn = config.dsn
            async with await psycopg.AsyncConnection.connect(dsn) as conn:
                # psycopg safely handles the password escaping
                await conn.execute(
                    f'ALTER ROLE "{name}" WITH PASSWORD %(pw)s',
                    {"pw": password},
                )

        # Grant membership in other roles
        if in_roles:
            for role in in_roles:
                if _VALID_ROLE_RE.match(role):
                    await execute_query(config, f'GRANT "{role}" TO "{name}"')

        return {"success": True, "message": f"Role '{name}' created successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def alter_role(
    config: ConnectionConfig,
    name: str,
    password: str | None = None,
    login: bool | None = None,
    superuser: bool | None = None,
    createdb: bool | None = None,
    createrole: bool | None = None,
    connection_limit: int | None = None,
    valid_until: str | None = None
) -> dict:
    """Alter an existing role"""

    # Validate role name
    if not _VALID_ROLE_RE.match(name) or len(name) > 63:
        return {"success": False, "error": f"Invalid role name: {name}"}

    options = []

    if login is not None:
        options.append("LOGIN" if login else "NOLOGIN")
    if superuser is not None:
        options.append("SUPERUSER" if superuser else "NOSUPERUSER")
    if createdb is not None:
        options.append("CREATEDB" if createdb else "NOCREATEDB")
    if createrole is not None:
        options.append("CREATEROLE" if createrole else "NOCREATEROLE")
    if connection_limit is not None:
        options.append(f"CONNECTION LIMIT {int(connection_limit)}")
    if valid_until:
        if not _validate_valid_until(valid_until):
            return {"success": False, "error": "Invalid valid_until format. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS"}
        options.append(f"VALID UNTIL '{valid_until}'")

    if not options and not password:
        return {"success": False, "error": "No changes specified"}

    try:
        if options:
            sql = f'ALTER ROLE "{name}" WITH ' + " ".join(options)
            await execute_query(config, sql)

        # Set password separately using parameterized query via psycopg
        if password:
            import psycopg
            dsn = config.dsn
            async with await psycopg.AsyncConnection.connect(dsn) as conn:
                await conn.execute(
                    f'ALTER ROLE "{name}" WITH PASSWORD %(pw)s',
                    {"pw": password},
                )

        return {"success": True, "message": f"Role '{name}' updated successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def drop_role(config: ConnectionConfig, name: str) -> dict:
    """Drop a role"""

    # Validate role name
    if not _VALID_ROLE_RE.match(name) or len(name) > 63:
        return {"success": False, "error": f"Invalid role name: {name}"}

    # Don't allow dropping certain system-like roles
    protected = ["postgres", "admin", "root"]
    if name.lower() in protected:
        return {"success": False, "error": f"Cannot drop protected role: {name}"}

    try:
        await execute_query(config, f'DROP ROLE IF EXISTS "{name}"')
        return {"success": True, "message": f"Role '{name}' dropped successfully"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def grant_role(config: ConnectionConfig, role: str, to_role: str) -> dict:
    """Grant membership in a role to another role"""

    # Validate role names
    if not _VALID_ROLE_RE.match(role) or not _VALID_ROLE_RE.match(to_role):
        return {"success": False, "error": "Invalid role name"}

    try:
        await execute_query(config, f'GRANT "{role}" TO "{to_role}"')
        return {"success": True, "message": f"Granted '{role}' to '{to_role}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def revoke_role(config: ConnectionConfig, role: str, from_role: str) -> dict:
    """Revoke membership in a role from another role"""

    # Validate role names
    if not _VALID_ROLE_RE.match(role) or not _VALID_ROLE_RE.match(from_role):
        return {"success": False, "error": "Invalid role name"}

    try:
        await execute_query(config, f'REVOKE "{role}" FROM "{from_role}"')
        return {"success": True, "message": f"Revoked '{role}' from '{from_role}'"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def get_role_grants(config: ConnectionConfig, name: str) -> dict:
    """Get detailed grants for a role"""

    # Validate role name
    if not _VALID_ROLE_RE.match(name) or len(name) > 63:
        return {"error": f"Invalid role name: {name}"}

    # Get database privileges (parameterized to prevent SQL injection)
    db_sql = """
    SELECT datname, has_database_privilege(%s, datname, 'CONNECT') as can_connect,
           has_database_privilege(%s, datname, 'CREATE') as can_create,
           has_database_privilege(%s, datname, 'TEMP') as can_temp
    FROM pg_database
    WHERE datistemplate = false
    ORDER BY datname
    """

    # Get schema privileges in current database
    schema_sql = """
    SELECT nspname,
           has_schema_privilege(%s, nspname, 'USAGE') as can_usage,
           has_schema_privilege(%s, nspname, 'CREATE') as can_create
    FROM pg_namespace
    WHERE nspname NOT LIKE 'pg_%%' AND nspname != 'information_schema'
    ORDER BY nspname
    """

    try:
        db_result = await execute_query(config, db_sql, params=(name, name, name))
        schema_result = await execute_query(config, schema_sql, params=(name, name))

        databases = []
        for row in db_result.rows:
            databases.append({
                "name": row[0],
                "can_connect": row[1],
                "can_create": row[2],
                "can_temp": row[3]
            })

        schemas = []
        for row in schema_result.rows:
            schemas.append({
                "name": row[0],
                "can_usage": row[1],
                "can_create": row[2]
            })

        return {
            "databases": databases,
            "schemas": schemas
        }
    except Exception as e:
        return {"error": str(e)}
