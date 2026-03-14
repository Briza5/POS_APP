"""Customer CRUD operations."""

from __future__ import annotations

from typing import Optional

import duckdb

from core.auth import generate_uid_token


def create_customer(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    display_name: str,
    phone: str = "",
    email: str = "",
    membership_type: str = "host",
    notes: str = "",
) -> dict:
    """Create a new customer and default permissions row.

    Returns the newly created customer as a dict.
    """
    import uuid
    customer_id = str(uuid.uuid4())
    uid_token = generate_uid_token()

    conn.execute(
        """
        INSERT INTO customers
            (customer_id, org_id, uid_token, display_name,
             phone, email, membership_type, notes, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, true)
        """,
        [customer_id, org_id, uid_token, display_name,
         phone or None, email or None, membership_type, notes or None],
    )
    conn.execute(
        """
        INSERT INTO customer_permissions (customer_id, org_id)
        VALUES (?, ?)
        """,
        [customer_id, org_id],
    )
    return get_customer_by_id(conn, customer_id)  # type: ignore[return-value]


def get_customer_by_uid(
    conn: duckdb.DuckDBPyConnection,
    uid_token: str,
) -> Optional[dict]:
    """Return customer dict for *uid_token* or None."""
    row = conn.execute(
        "SELECT * FROM customers WHERE uid_token = ?",
        [uid_token],
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(conn, row)


def get_customer_by_id(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
) -> Optional[dict]:
    """Return customer dict for *customer_id* or None."""
    row = conn.execute(
        "SELECT * FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    if row is None:
        return None
    return _row_to_dict(conn, row)


def list_customers(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    search: Optional[str] = None,
    active_only: bool = True,
) -> list[dict]:
    """Return customers for *org_id*, optionally filtered by *search* string."""
    query = "SELECT * FROM customers WHERE org_id = ?"
    params: list = [org_id]
    if active_only:
        query += " AND is_active = true"
    if search:
        query += " AND (display_name ILIKE ? OR phone ILIKE ? OR email ILIKE ?)"
        pattern = f"%{search}%"
        params += [pattern, pattern, pattern]
    query += " ORDER BY display_name"
    rows = conn.execute(query, params).fetchall()
    return [_row_to_dict(conn, r) for r in rows]


def update_customer(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    **kwargs,
) -> bool:
    """Update arbitrary customer fields. Returns True if a row was updated."""
    allowed = {
        "display_name", "phone", "email", "membership_type",
        "notes", "member_since", "is_active",
    }
    updates = {k: v for k, v in kwargs.items() if k in allowed}
    if not updates:
        return False
    set_clause = ", ".join(f"{k} = ?" for k in updates)
    values = list(updates.values()) + [customer_id]
    conn.execute(
        f"UPDATE customers SET {set_clause} WHERE customer_id = ?",
        values,
    )
    return True


def deactivate_customer(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
) -> bool:
    """Soft-delete a customer (is_active = false)."""
    conn.execute(
        "UPDATE customers SET is_active = false WHERE customer_id = ?",
        [customer_id],
    )
    return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(conn: duckdb.DuckDBPyConnection, row: tuple) -> dict:
    """Convert a customers table row to a dict using column names."""
    cols = [d[0] for d in conn.execute("DESCRIBE customers").fetchall()]
    return dict(zip(cols, row))
