"""Authentication and authorization module"""

import hashlib
import secrets
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal
import sqlite3

import msgspec

from tusk.core.config import get_config

# Auth database path
AUTH_DB = Path.home() / ".tusk" / "users.db"


class User(msgspec.Struct):
    """User model"""
    id: str
    username: str
    email: str | None = None
    password_hash: str = ""
    display_name: str | None = None
    is_admin: bool = False
    is_active: bool = True
    created_at: str = ""
    last_login: str | None = None
    settings: dict = {}


class Group(msgspec.Struct):
    """Group model"""
    id: str
    name: str
    description: str | None = None
    permissions: list[str] = []
    created_at: str = ""


class Session(msgspec.Struct):
    """Session model"""
    id: str
    user_id: str
    created_at: str
    expires_at: str
    ip_address: str | None = None
    user_agent: str | None = None


# Permission definitions
PERMISSIONS = {
    # Connections
    "connections.view": "View connections list",
    "connections.create": "Create new connections",
    "connections.edit": "Edit existing connections",
    "connections.delete": "Delete connections",
    "connections.use": "Execute queries on connections",
    # Queries
    "queries.execute": "Execute SQL queries",
    "queries.save": "Save queries",
    "queries.export": "Export results",
    # Admin
    "admin.view": "View admin dashboard",
    "admin.processes": "View/kill processes",
    "admin.backup": "Create/restore backups",
    "admin.maintenance": "Run VACUUM/ANALYZE",
    "admin.extensions": "Manage extensions",
    # Data/ETL
    "data.view": "View data page",
    "data.transform": "Create pipelines",
    "data.export": "Export data",
    "data.import": "Import to databases",
    # Cluster
    "cluster.view": "View cluster dashboard",
    "cluster.submit": "Submit jobs",
    "cluster.cancel": "Cancel jobs",
    "cluster.manage": "Start/stop cluster",
    # System
    "system.users": "Manage users",
    "system.groups": "Manage groups",
    "system.settings": "System settings",
}

# Default groups with permissions
DEFAULT_GROUPS = {
    "administrators": {
        "name": "Administrators",
        "description": "Full system access",
        "permissions": list(PERMISSIONS.keys()),
    },
    "data_engineers": {
        "name": "Data Engineers",
        "description": "Full data access, limited admin",
        "permissions": [
            "connections.view", "connections.create", "connections.edit",
            "connections.delete", "connections.use",
            "queries.execute", "queries.save", "queries.export",
            "admin.view",
            "data.view", "data.transform", "data.export", "data.import",
            "cluster.view", "cluster.submit", "cluster.cancel",
        ],
    },
    "analysts": {
        "name": "Analysts",
        "description": "Query and analyze data",
        "permissions": [
            "connections.view", "connections.use",
            "queries.execute", "queries.save", "queries.export",
            "data.view", "data.transform", "data.export",
            "cluster.view", "cluster.submit",
        ],
    },
    "viewers": {
        "name": "Viewers",
        "description": "Read-only access",
        "permissions": [
            "connections.view",
            "queries.execute",
            "data.view",
            "cluster.view",
        ],
    },
}


def init_auth_db() -> None:
    """Initialize the auth database"""
    AUTH_DB.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    # Users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            password_hash TEXT NOT NULL,
            display_name TEXT,
            is_admin INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL,
            last_login TEXT,
            settings TEXT DEFAULT '{}'
        )
    """)

    # Groups table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            description TEXT,
            created_at TEXT NOT NULL
        )
    """)

    # Group permissions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS group_permissions (
            group_id TEXT NOT NULL,
            permission TEXT NOT NULL,
            PRIMARY KEY (group_id, permission),
            FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
    """)

    # User groups table (many-to-many)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_groups (
            user_id TEXT NOT NULL,
            group_id TEXT NOT NULL,
            added_at TEXT NOT NULL,
            added_by TEXT,
            PRIMARY KEY (user_id, group_id),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE
        )
    """)

    # Sessions table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            user_id TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            ip_address TEXT,
            user_agent TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
    """)

    # Audit log table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT,
            action TEXT NOT NULL,
            resource TEXT,
            details TEXT,
            ip_address TEXT,
            timestamp TEXT NOT NULL
        )
    """)

    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sessions_expires ON sessions(expires_at)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_log(user_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_log(timestamp)")

    conn.commit()
    conn.close()


def hash_password(password: str) -> str:
    """Hash a password using SHA-256 with salt (simple implementation)

    For production, use bcrypt. This is a simpler version to avoid
    additional dependencies.
    """
    salt = secrets.token_hex(16)
    hash_obj = hashlib.sha256((salt + password).encode())
    return f"{salt}${hash_obj.hexdigest()}"


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against its hash"""
    try:
        salt, stored_hash = password_hash.split("$")
        hash_obj = hashlib.sha256((salt + password).encode())
        return hash_obj.hexdigest() == stored_hash
    except (ValueError, AttributeError):
        return False


def generate_session_token() -> str:
    """Generate a secure session token"""
    return secrets.token_urlsafe(32)


def get_auth_mode() -> Literal["single", "multi"]:
    """Get the current auth mode from config"""
    config = get_config()
    return getattr(config, "auth_mode", "single")


def is_auth_enabled() -> bool:
    """Check if authentication is enabled"""
    return get_auth_mode() == "multi"


# User operations
def create_user(
    username: str,
    password: str,
    email: str | None = None,
    display_name: str | None = None,
    is_admin: bool = False,
) -> User:
    """Create a new user"""
    from uuid import uuid4

    init_auth_db()

    user_id = str(uuid4())
    now = datetime.now().isoformat()
    password_hash = hash_password(password)

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO users (id, username, email, password_hash, display_name, is_admin, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, username, email, password_hash, display_name, int(is_admin), now))
        conn.commit()
    finally:
        conn.close()

    return User(
        id=user_id,
        username=username,
        email=email,
        display_name=display_name,
        is_admin=is_admin,
        created_at=now,
    )


def get_user_by_id(user_id: str) -> User | None:
    """Get a user by ID"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()

        if not row:
            return None

        return User(
            id=row[0],
            username=row[1],
            email=row[2],
            password_hash=row[3],
            display_name=row[4],
            is_admin=bool(row[5]),
            is_active=bool(row[6]),
            created_at=row[7],
            last_login=row[8],
            settings=msgspec.json.decode(row[9]) if row[9] else {},
        )
    finally:
        conn.close()


def get_user_by_username(username: str) -> User | None:
    """Get a user by username"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
        row = cursor.fetchone()

        if not row:
            return None

        return User(
            id=row[0],
            username=row[1],
            email=row[2],
            password_hash=row[3],
            display_name=row[4],
            is_admin=bool(row[5]),
            is_active=bool(row[6]),
            created_at=row[7],
            last_login=row[8],
            settings=msgspec.json.decode(row[9]) if row[9] else {},
        )
    finally:
        conn.close()


def list_users() -> list[User]:
    """List all users"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM users ORDER BY username")
        rows = cursor.fetchall()

        return [
            User(
                id=row[0],
                username=row[1],
                email=row[2],
                password_hash="",  # Don't expose
                display_name=row[4],
                is_admin=bool(row[5]),
                is_active=bool(row[6]),
                created_at=row[7],
                last_login=row[8],
                settings={},
            )
            for row in rows
        ]
    finally:
        conn.close()


def update_user(user_id: str, **kwargs) -> bool:
    """Update a user's fields"""
    init_auth_db()

    allowed_fields = {"email", "display_name", "is_admin", "is_active", "settings"}
    updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

    if not updates:
        return False

    if "settings" in updates:
        updates["settings"] = msgspec.json.encode(updates["settings"]).decode()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [user_id]
        cursor.execute(f"UPDATE users SET {set_clause} WHERE id = ?", values)
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def update_password(user_id: str, new_password: str) -> bool:
    """Update a user's password"""
    init_auth_db()

    password_hash = hash_password(new_password)

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("UPDATE users SET password_hash = ? WHERE id = ?", (password_hash, user_id))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_user(user_id: str) -> bool:
    """Delete a user"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM users WHERE id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


# Session operations
def create_session(user_id: str, ip_address: str | None = None, user_agent: str | None = None) -> Session:
    """Create a new session for a user"""
    init_auth_db()

    session_id = generate_session_token()
    now = datetime.now()
    expires = now + timedelta(hours=24)  # 24 hour session

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO sessions (id, user_id, created_at, expires_at, ip_address, user_agent)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (session_id, user_id, now.isoformat(), expires.isoformat(), ip_address, user_agent))

        # Update last login
        cursor.execute("UPDATE users SET last_login = ? WHERE id = ?", (now.isoformat(), user_id))

        conn.commit()
    finally:
        conn.close()

    return Session(
        id=session_id,
        user_id=user_id,
        created_at=now.isoformat(),
        expires_at=expires.isoformat(),
        ip_address=ip_address,
        user_agent=user_agent,
    )


def get_session(session_id: str) -> Session | None:
    """Get a session by ID"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
        row = cursor.fetchone()

        if not row:
            return None

        session = Session(
            id=row[0],
            user_id=row[1],
            created_at=row[2],
            expires_at=row[3],
            ip_address=row[4],
            user_agent=row[5],
        )

        # Check if expired
        if datetime.fromisoformat(session.expires_at) < datetime.now():
            delete_session(session_id)
            return None

        return session
    finally:
        conn.close()


def delete_session(session_id: str) -> bool:
    """Delete a session"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def delete_user_sessions(user_id: str) -> int:
    """Delete all sessions for a user"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


def cleanup_expired_sessions() -> int:
    """Delete all expired sessions"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        now = datetime.now().isoformat()
        cursor.execute("DELETE FROM sessions WHERE expires_at < ?", (now,))
        conn.commit()
        return cursor.rowcount
    finally:
        conn.close()


# Authentication
def authenticate(username: str, password: str) -> User | None:
    """Authenticate a user with username and password"""
    user = get_user_by_username(username)

    if not user:
        return None

    if not user.is_active:
        return None

    if not verify_password(password, user.password_hash):
        return None

    return user


# Group operations
def create_group(name: str, description: str | None = None, permissions: list[str] | None = None) -> Group:
    """Create a new group"""
    from uuid import uuid4

    init_auth_db()

    group_id = str(uuid4())
    now = datetime.now().isoformat()
    permissions = permissions or []

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO groups (id, name, description, created_at)
            VALUES (?, ?, ?, ?)
        """, (group_id, name, description, now))

        # Add permissions
        for perm in permissions:
            cursor.execute("""
                INSERT INTO group_permissions (group_id, permission) VALUES (?, ?)
            """, (group_id, perm))

        conn.commit()
    finally:
        conn.close()

    return Group(
        id=group_id,
        name=name,
        description=description,
        permissions=permissions,
        created_at=now,
    )


def get_group(group_id: str) -> Group | None:
    """Get a group by ID"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM groups WHERE id = ?", (group_id,))
        row = cursor.fetchone()

        if not row:
            return None

        # Get permissions
        cursor.execute("SELECT permission FROM group_permissions WHERE group_id = ?", (group_id,))
        permissions = [r[0] for r in cursor.fetchall()]

        return Group(
            id=row[0],
            name=row[1],
            description=row[2],
            permissions=permissions,
            created_at=row[3],
        )
    finally:
        conn.close()


def list_groups() -> list[Group]:
    """List all groups"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM groups ORDER BY name")
        rows = cursor.fetchall()

        groups = []
        for row in rows:
            cursor.execute("SELECT permission FROM group_permissions WHERE group_id = ?", (row[0],))
            permissions = [r[0] for r in cursor.fetchall()]

            groups.append(Group(
                id=row[0],
                name=row[1],
                description=row[2],
                permissions=permissions,
                created_at=row[3],
            ))

        return groups
    finally:
        conn.close()


def add_user_to_group(user_id: str, group_id: str, added_by: str | None = None) -> bool:
    """Add a user to a group"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT OR IGNORE INTO user_groups (user_id, group_id, added_at, added_by)
            VALUES (?, ?, ?, ?)
        """, (user_id, group_id, datetime.now().isoformat(), added_by))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def remove_user_from_group(user_id: str, group_id: str) -> bool:
    """Remove a user from a group"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM user_groups WHERE user_id = ? AND group_id = ?", (user_id, group_id))
        conn.commit()
        return cursor.rowcount > 0
    finally:
        conn.close()


def get_user_groups(user_id: str) -> list[Group]:
    """Get all groups a user belongs to"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT g.* FROM groups g
            JOIN user_groups ug ON g.id = ug.group_id
            WHERE ug.user_id = ?
            ORDER BY g.name
        """, (user_id,))
        rows = cursor.fetchall()

        groups = []
        for row in rows:
            cursor.execute("SELECT permission FROM group_permissions WHERE group_id = ?", (row[0],))
            permissions = [r[0] for r in cursor.fetchall()]

            groups.append(Group(
                id=row[0],
                name=row[1],
                description=row[2],
                permissions=permissions,
                created_at=row[3],
            ))

        return groups
    finally:
        conn.close()


def get_user_permissions(user_id: str) -> set[str]:
    """Get all permissions for a user (from all their groups)"""
    user = get_user_by_id(user_id)

    if not user:
        return set()

    # Admins have all permissions
    if user.is_admin:
        return set(PERMISSIONS.keys())

    groups = get_user_groups(user_id)
    permissions = set()

    for group in groups:
        permissions.update(group.permissions)

    return permissions


def user_has_permission(user_id: str, permission: str) -> bool:
    """Check if a user has a specific permission"""
    return permission in get_user_permissions(user_id)


# Setup default groups
def setup_default_groups() -> None:
    """Create default groups if they don't exist"""
    init_auth_db()

    existing = {g.name.lower().replace(" ", "_"): g for g in list_groups()}

    for group_key, group_data in DEFAULT_GROUPS.items():
        if group_key not in existing:
            create_group(
                name=group_data["name"],
                description=group_data["description"],
                permissions=group_data["permissions"],
            )


# Setup first admin user
# Audit log operations
def log_audit(
    action: str,
    user_id: str | None = None,
    resource: str | None = None,
    details: str | None = None,
    ip_address: str | None = None,
) -> None:
    """Write an entry to the audit log"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        cursor.execute("""
            INSERT INTO audit_log (user_id, action, resource, details, ip_address, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, action, resource, details, ip_address, datetime.now().isoformat()))
        conn.commit()
    finally:
        conn.close()


def get_audit_logs(
    limit: int = 100,
    offset: int = 0,
    user_id: str | None = None,
    action: str | None = None,
) -> list[dict]:
    """Get audit log entries"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    try:
        query = """
            SELECT al.*, u.username
            FROM audit_log al
            LEFT JOIN users u ON al.user_id = u.id
            WHERE 1=1
        """
        params: list = []

        if user_id:
            query += " AND al.user_id = ?"
            params.append(user_id)
        if action:
            query += " AND al.action = ?"
            params.append(action)

        query += " ORDER BY al.timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor.execute(query, params)
        rows = cursor.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def get_audit_log_count(user_id: str | None = None, action: str | None = None) -> int:
    """Get total count of audit log entries"""
    init_auth_db()

    conn = sqlite3.connect(AUTH_DB)
    cursor = conn.cursor()

    try:
        query = "SELECT COUNT(*) FROM audit_log WHERE 1=1"
        params: list = []

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)
        if action:
            query += " AND action = ?"
            params.append(action)

        cursor.execute(query, params)
        return cursor.fetchone()[0]
    finally:
        conn.close()


def setup_admin_user(username: str = "admin", password: str = "admin") -> User | None:
    """Create admin user if no users exist"""
    init_auth_db()

    users = list_users()
    if users:
        return None  # Users already exist

    user = create_user(
        username=username,
        password=password,
        display_name="Administrator",
        is_admin=True,
    )

    # Add to administrators group
    groups = list_groups()
    admin_group = next((g for g in groups if g.name == "Administrators"), None)
    if admin_group:
        add_user_to_group(user.id, admin_group.id)

    return user
