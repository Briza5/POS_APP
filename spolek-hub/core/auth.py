"""
Authentication and token validation for Spolkový Hospodský Systém.
"""

from __future__ import annotations

import hashlib
import secrets
from typing import Optional

import duckdb
import streamlit as st


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Return SHA-256 hex digest of *password*."""
    return hashlib.sha256(password.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Admin authentication
# ---------------------------------------------------------------------------

def verify_admin(
    username: str,
    password: str,
    conn: duckdb.DuckDBPyConnection,
) -> Optional[dict]:
    """Verify admin credentials and return the admin row as a dict, or None.

    On success also updates ``last_login_at`` in the database.
    """
    pw_hash = hash_password(password)
    row = conn.execute(
        """
        SELECT admin_id, org_id, username, role
        FROM admin_users
        WHERE username = ? AND password_hash = ?
        """,
        [username, pw_hash],
    ).fetchone()

    if row is None:
        return None

    admin = {
        "admin_id": row[0],
        "org_id": row[1],
        "username": row[2],
        "role": row[3],
    }

    conn.execute(
        "UPDATE admin_users SET last_login_at = current_timestamp WHERE admin_id = ?",
        [admin["admin_id"]],
    )
    return admin


# ---------------------------------------------------------------------------
# Customer / table token validation
# ---------------------------------------------------------------------------

def validate_uid_token(
    uid_token: str,
    conn: duckdb.DuckDBPyConnection,
) -> Optional[dict]:
    """Return customer dict for *uid_token*, or None if invalid / inactive."""
    row = conn.execute(
        """
        SELECT customer_id, org_id, uid_token, display_name,
               phone, email, points_balance, total_spent,
               membership_type, member_since, is_active, notes
        FROM customers
        WHERE uid_token = ? AND is_active = true
        """,
        [uid_token],
    ).fetchone()

    if row is None:
        return None

    conn.execute(
        "UPDATE customers SET last_seen_at = current_timestamp WHERE uid_token = ?",
        [uid_token],
    )

    cols = [
        "customer_id", "org_id", "uid_token", "display_name",
        "phone", "email", "points_balance", "total_spent",
        "membership_type", "member_since", "is_active", "notes",
    ]
    return dict(zip(cols, row))


def validate_table_token(
    table_token: str,
    conn: duckdb.DuckDBPyConnection,
) -> Optional[dict]:
    """Return table dict for *table_token*, or None if invalid / inactive."""
    row = conn.execute(
        """
        SELECT table_id, org_id, table_number, description, qr_token, is_active
        FROM tables
        WHERE qr_token = ? AND is_active = true
        """,
        [table_token],
    ).fetchone()

    if row is None:
        return None

    cols = ["table_id", "org_id", "table_number", "description", "qr_token", "is_active"]
    return dict(zip(cols, row))


# ---------------------------------------------------------------------------
# Token generators
# ---------------------------------------------------------------------------

def generate_uid_token() -> str:
    """Return a 32-char cryptographically random hex token for customers."""
    return secrets.token_hex(16)


def generate_table_token() -> str:
    """Return a 32-char cryptographically random hex token for tables."""
    return secrets.token_hex(16)


# ---------------------------------------------------------------------------
# Streamlit session helpers
# ---------------------------------------------------------------------------

def is_admin_logged_in() -> bool:
    """Return True if an admin is stored in Streamlit session state."""
    return st.session_state.get("admin") is not None


def require_admin() -> None:
    """Stop rendering if no admin is logged in."""
    if not is_admin_logged_in():
        st.warning("Přihlaste se jako administrátor.")
        st.stop()


def get_current_org_id() -> str:
    """Return the org_id of the currently logged-in admin.

    Raises:
        RuntimeError: if no admin is in session state.
    """
    admin = st.session_state.get("admin")
    if admin is None:
        raise RuntimeError("Žádný přihlášený admin – org_id nelze zjistit.")
    return admin["org_id"]
