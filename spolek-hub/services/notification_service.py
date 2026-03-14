"""Notification creation and retrieval."""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Optional

import duckdb


def create_notification(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    target_type: str,
    notification_type: str,
    title: str,
    body: str = "",
    reference_id: Optional[str] = None,
    reference_type: Optional[str] = None,
    target_id: Optional[str] = None,
    target_role: Optional[str] = None,
) -> None:
    """Insert a notification row."""
    conn.execute(
        """
        INSERT INTO notifications
            (notification_id, org_id, target_type, target_id, target_role,
             notification_type, title, body, reference_id, reference_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(uuid.uuid4()), org_id, target_type, target_id, target_role,
            notification_type, title, body, reference_id, reference_type,
        ],
    )


def get_unread_notifications(
    conn: duckdb.DuckDBPyConnection,
    target_type: str,
    target_id: Optional[str] = None,
    target_role: Optional[str] = None,
    limit: int = 20,
) -> list[dict]:
    """Return unread notifications (newest first)."""
    query = """
        SELECT notification_id, org_id, target_type, target_id, target_role,
               notification_type, title, body, reference_id, reference_type,
               is_read, created_at
        FROM notifications
        WHERE is_read = false AND target_type = ?
    """
    params: list = [target_type]
    if target_id:
        query += " AND (target_id = ? OR target_id IS NULL)"
        params.append(target_id)
    if target_role:
        query += " AND (target_role = ? OR target_role IS NULL)"
        params.append(target_role)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    rows = conn.execute(query, params).fetchall()
    cols = [
        "notification_id", "org_id", "target_type", "target_id", "target_role",
        "notification_type", "title", "body", "reference_id", "reference_type",
        "is_read", "created_at",
    ]
    return [dict(zip(cols, r)) for r in rows]


def mark_as_read(conn: duckdb.DuckDBPyConnection, notification_id: str) -> None:
    """Mark a single notification as read."""
    conn.execute(
        """
        UPDATE notifications
        SET is_read = true, read_at = current_timestamp
        WHERE notification_id = ?
        """,
        [notification_id],
    )


def mark_all_read(
    conn: duckdb.DuckDBPyConnection,
    target_type: str,
    target_id: str,
) -> int:
    """Mark all unread notifications for *target_id* as read.

    Returns number of rows updated.
    """
    conn.execute(
        """
        UPDATE notifications
        SET is_read = true, read_at = current_timestamp
        WHERE target_type = ? AND target_id = ? AND is_read = false
        """,
        [target_type, target_id],
    )
    # DuckDB doesn't expose rowcount directly; return via count
    row = conn.execute(
        """
        SELECT COUNT(*) FROM notifications
        WHERE target_type = ? AND target_id = ? AND is_read = true
          AND read_at >= current_timestamp - INTERVAL '1 second'
        """,
        [target_type, target_id],
    ).fetchone()
    return row[0] if row else 0


def get_unread_count(
    conn: duckdb.DuckDBPyConnection,
    target_type: str,
    target_id: Optional[str] = None,
    target_role: Optional[str] = None,
) -> int:
    """Return count of unread notifications."""
    query = "SELECT COUNT(*) FROM notifications WHERE is_read = false AND target_type = ?"
    params: list = [target_type]
    if target_id:
        query += " AND (target_id = ? OR target_id IS NULL)"
        params.append(target_id)
    if target_role:
        query += " AND (target_role = ? OR target_role IS NULL)"
        params.append(target_role)
    row = conn.execute(query, params).fetchone()
    return row[0] if row else 0


# ---------------------------------------------------------------------------
# Domain-specific helpers
# ---------------------------------------------------------------------------

def notify_new_order(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    order_id: str,
    source: str,
    table_number: Optional[str],
    customer_name: Optional[str],
    amount: Decimal,
) -> None:
    source_label = {"customer": "zákazník", "table_qr": "stůl QR"}.get(source, source)
    where = f"Stůl {table_number}" if table_number else "bez stolu"
    who = customer_name or "Anonymní"
    create_notification(
        conn, org_id,
        target_type="admin",
        target_role="bartender",
        notification_type="new_order",
        title=f"🔔 Nová objednávka – {amount} Kč",
        body=f"{who} | {where} | zdroj: {source_label}",
        reference_id=order_id,
        reference_type="order",
    )


def notify_order_ready(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    order_id: str,
) -> None:
    row = conn.execute(
        "SELECT org_id FROM orders WHERE order_id = ?", [order_id]
    ).fetchone()
    if row is None:
        return
    create_notification(
        conn, row[0],
        target_type="customer",
        target_id=customer_id,
        notification_type="order_ready",
        title="✅ Objednávka připravena",
        body="Vaše objednávka je připravena k vyzvednutí.",
        reference_id=order_id,
        reference_type="order",
    )


def notify_order_accepted(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    order_id: str,
) -> None:
    row = conn.execute(
        "SELECT org_id FROM orders WHERE order_id = ?", [order_id]
    ).fetchone()
    if row is None:
        return
    create_notification(
        conn, row[0],
        target_type="customer",
        target_id=customer_id,
        notification_type="order_accepted",
        title="👍 Objednávka přijata",
        body="Vaše objednávka byla přijata a připravuje se.",
        reference_id=order_id,
        reference_type="order",
    )


def notify_order_rejected(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    order_id: str,
    reason: str,
) -> None:
    row = conn.execute(
        "SELECT org_id FROM orders WHERE order_id = ?", [order_id]
    ).fetchone()
    if row is None:
        return
    create_notification(
        conn, row[0],
        target_type="customer",
        target_id=customer_id,
        notification_type="order_rejected",
        title="❌ Objednávka zamítnuta",
        body=f"Důvod: {reason}",
        reference_id=order_id,
        reference_type="order",
    )


def notify_order_cancelled_by_customer(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    order_id: str,
) -> None:
    create_notification(
        conn, org_id,
        target_type="admin",
        target_role="bartender",
        notification_type="order_cancelled",
        title="🚫 Objednávka zrušena zákazníkem",
        body="Zákazník zrušil svou objednávku.",
        reference_id=order_id,
        reference_type="order",
    )


def notify_tab_closed(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    tab_id: str,
    total_paid: Decimal,
    points_earned: int,
) -> None:
    row = conn.execute(
        "SELECT org_id FROM tabs WHERE tab_id = ?", [tab_id]
    ).fetchone()
    if row is None:
        return
    create_notification(
        conn, row[0],
        target_type="customer",
        target_id=customer_id,
        notification_type="tab_closed",
        title="🧾 Účet uzavřen",
        body=f"Zaplaceno: {total_paid} Kč, připsáno: {points_earned} bodů",
        reference_id=tab_id,
        reference_type="tab",
    )
