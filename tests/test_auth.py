"""Tests for authentication — password hashing, user CRUD."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch

from tusk.core.auth import hash_password, verify_password, init_auth_db, create_user, get_user_by_username, delete_user


@pytest.fixture
def temp_auth_db(tmp_path):
    """Use a temporary database for auth tests."""
    db_path = tmp_path / "test_users.db"
    with patch("tusk.core.auth.AUTH_DB", db_path):
        init_auth_db()
        yield db_path


class TestPasswordHashing:
    """Test password hash/verify without database."""

    def test_hash_returns_string(self):
        h = hash_password("test123")
        assert isinstance(h, str)
        assert len(h) > 0

    def test_hash_is_not_plaintext(self):
        h = hash_password("test123")
        assert h != "test123"

    def test_verify_correct_password(self):
        h = hash_password("my_password")
        assert verify_password("my_password", h) is True

    def test_verify_wrong_password(self):
        h = hash_password("my_password")
        assert verify_password("wrong_password", h) is False

    def test_different_passwords_different_hashes(self):
        h1 = hash_password("password1")
        h2 = hash_password("password2")
        assert h1 != h2


class TestUserCRUD:
    """Test user creation and lookup (requires temp database)."""

    def test_create_user(self, temp_auth_db):
        user = create_user(username="testuser", password="pass123")
        assert user.username == "testuser"
        assert user.id is not None

    def test_create_admin_user(self, temp_auth_db):
        user = create_user(username="admin", password="pass123", is_admin=True)
        assert user.is_admin is True

    def test_get_user_by_username(self, temp_auth_db):
        create_user(username="findme", password="pass123")
        user = get_user_by_username("findme")
        assert user is not None
        assert user.username == "findme"

    def test_get_nonexistent_user(self, temp_auth_db):
        user = get_user_by_username("nonexistent")
        assert user is None

    def test_delete_user(self, temp_auth_db):
        user = create_user(username="deleteme", password="pass123")
        delete_user(user.id)
        assert get_user_by_username("deleteme") is None

    def test_duplicate_username_raises(self, temp_auth_db):
        create_user(username="unique", password="pass123")
        with pytest.raises(Exception):
            create_user(username="unique", password="pass456")
