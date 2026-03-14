"""Customer permission management."""

from __future__ import annotations

from decimal import Decimal

import duckdb


def can_customer_order(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> bool:
    row = conn.execute(
        "SELECT can_order FROM customer_permissions WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    ).fetchone()
    return bool(row[0]) if row else False


def can_customer_tab(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> bool:
    row = conn.execute(
        "SELECT can_tab FROM customer_permissions WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    ).fetchone()
    return bool(row[0]) if row else False


def get_credit_limit(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> Decimal:
    row = conn.execute(
        "SELECT credit_limit FROM customer_permissions WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    ).fetchone()
    return Decimal(str(row[0])) if row and row[0] is not None else Decimal("0")


def grant_order_permission(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
    granted_by_admin_id: str,
) -> None:
    conn.execute(
        """
        UPDATE customer_permissions
        SET can_order = true, granted_by = ?, granted_at = current_timestamp
        WHERE customer_id = ? AND org_id = ?
        """,
        [granted_by_admin_id, customer_id, org_id],
    )


def revoke_order_permission(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> None:
    conn.execute(
        "UPDATE customer_permissions SET can_order = false WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    )


def grant_tab_permission(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
    credit_limit: Decimal,
    granted_by_admin_id: str,
) -> None:
    conn.execute(
        """
        UPDATE customer_permissions
        SET can_tab = true, credit_limit = ?,
            granted_by = ?, granted_at = current_timestamp
        WHERE customer_id = ? AND org_id = ?
        """,
        [float(credit_limit), granted_by_admin_id, customer_id, org_id],
    )


def revoke_tab_permission(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> None:
    conn.execute(
        "UPDATE customer_permissions SET can_tab = false WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    )


def list_permitted_customers(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> list[dict]:
    """Return customers with any permission (can_order OR can_tab)."""
    rows = conn.execute(
        """
        SELECT c.customer_id, c.display_name, c.email, c.phone,
               p.can_order, p.can_tab, p.credit_limit, p.granted_at
        FROM customers c
        JOIN customer_permissions p
          ON c.customer_id = p.customer_id AND c.org_id = p.org_id
        WHERE c.org_id = ? AND c.is_active = true
          AND (p.can_order = true OR p.can_tab = true)
        ORDER BY c.display_name
        """,
        [org_id],
    ).fetchall()
    cols = [
        "customer_id", "display_name", "email", "phone",
        "can_order", "can_tab", "credit_limit", "granted_at",
    ]
    return [dict(zip(cols, r)) for r in rows]
