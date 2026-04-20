"""Symmetric encryption for secrets-at-rest (connection passwords, etc.)

Key is stored at ~/.tusk/.key with 0600 permissions. Generated on first use.
Encrypted values are prefixed with ENC_PREFIX so we can distinguish from
plain-text legacy values during migration.
"""

import os
import stat
from pathlib import Path
from cryptography.fernet import Fernet, InvalidToken

TUSK_DIR = Path.home() / ".tusk"
KEY_FILE = TUSK_DIR / ".key"
ENC_PREFIX = "enc:v1:"


_cached: Fernet | None = None


def _get_fernet() -> Fernet:
    global _cached
    if _cached is not None:
        return _cached

    TUSK_DIR.mkdir(parents=True, exist_ok=True)

    if KEY_FILE.exists():
        key = KEY_FILE.read_bytes().strip()
    else:
        key = Fernet.generate_key()
        KEY_FILE.write_bytes(key)
        try:
            os.chmod(KEY_FILE, stat.S_IRUSR | stat.S_IWUSR)
        except OSError:
            pass

    _cached = Fernet(key)
    return _cached


def encrypt(value: str) -> str:
    """Encrypt a string and return a prefixed token."""
    if not value:
        return value
    token = _get_fernet().encrypt(value.encode("utf-8")).decode("ascii")
    return f"{ENC_PREFIX}{token}"


def decrypt(value: str) -> str:
    """Decrypt a prefixed token. If the value is not prefixed, return it as-is
    (backward compatibility for legacy plain-text values during migration)."""
    if not value or not value.startswith(ENC_PREFIX):
        return value
    token = value[len(ENC_PREFIX):]
    try:
        return _get_fernet().decrypt(token.encode("ascii")).decode("utf-8")
    except InvalidToken:
        return ""


def is_encrypted(value: str | None) -> bool:
    return bool(value) and value.startswith(ENC_PREFIX)
