"""Loyalty points business logic."""

from __future__ import annotations

import math
import uuid
from decimal import Decimal
from typing import Optional

import duckdb


def calculate_points_earned(amount: Decimal, points_per_czk: float) -> int:
    """Return integer points for *amount* CZK at *points_per_czk* rate."""
    return math.floor(float(amount) * points_per_czk)


def add_points(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    points: int,
    note: str,
    order_id: Optional[str] = None,
    tab_id: Optional[str] = None,
) -> int:
    """Credit *points* to *customer_id*.

    Returns:
        New points balance.
    """
    row = conn.execute(
        "SELECT points_balance FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Zákazník {customer_id!r} nenalezen.")

    new_balance = row[0] + points
    conn.execute(
        "UPDATE customers SET points_balance = ? WHERE customer_id = ?",
        [new_balance, customer_id],
    )
    conn.execute(
        """
        INSERT INTO loyalty_transactions
            (txn_id, customer_id, order_id, tab_id,
             txn_type, points_delta, balance_after, note)
        VALUES (?, ?, ?, ?, 'earn', ?, ?, ?)
        """,
        [str(uuid.uuid4()), customer_id, order_id, tab_id,
         points, new_balance, note],
    )
    return new_balance


def redeem_points(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    points: int,
    note: str,
    order_id: Optional[str] = None,
) -> int:
    """Deduct *points* from *customer_id*.

    Returns:
        New points balance.

    Raises:
        ValueError: if the customer has insufficient points.
    """
    row = conn.execute(
        "SELECT points_balance FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Zákazník {customer_id!r} nenalezen.")

    current = row[0]
    if current < points:
        raise ValueError(
            f"Nedostatek bodů: má {current}, požadováno {points}."
        )

    new_balance = current - points
    conn.execute(
        "UPDATE customers SET points_balance = ? WHERE customer_id = ?",
        [new_balance, customer_id],
    )
    conn.execute(
        """
        INSERT INTO loyalty_transactions
            (txn_id, customer_id, order_id, txn_type,
             points_delta, balance_after, note)
        VALUES (?, ?, ?, 'redeem', ?, ?, ?)
        """,
        [str(uuid.uuid4()), customer_id, order_id,
         -points, new_balance, note],
    )
    return new_balance


def adjust_points(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    delta: int,
    note: str,
    admin_id: str,
) -> int:
    """Manually add or subtract *delta* points (admin action).

    Returns:
        New points balance.
    """
    row = conn.execute(
        "SELECT points_balance FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Zákazník {customer_id!r} nenalezen.")

    new_balance = max(0, row[0] + delta)
    conn.execute(
        "UPDATE customers SET points_balance = ? WHERE customer_id = ?",
        [new_balance, customer_id],
    )
    conn.execute(
        """
        INSERT INTO loyalty_transactions
            (txn_id, customer_id, txn_type, points_delta, balance_after, note)
        VALUES (?, ?, 'adjust', ?, ?, ?)
        """,
        [str(uuid.uuid4()), customer_id, delta, new_balance,
         f"{note} [admin:{admin_id}]"],
    )
    return new_balance


def get_transaction_history(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    limit: int = 50,
) -> list[dict]:
    """Return loyalty transaction history for *customer_id* (newest first)."""
    rows = conn.execute(
        """
        SELECT txn_id, customer_id, order_id, tab_id, txn_type,
               points_delta, balance_after, note, created_at
        FROM loyalty_transactions
        WHERE customer_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        [customer_id, limit],
    ).fetchall()
    cols = [
        "txn_id", "customer_id", "order_id", "tab_id", "txn_type",
        "points_delta", "balance_after", "note", "created_at",
    ]
    return [dict(zip(cols, r)) for r in rows]


def get_available_rewards(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    points_balance: int,
) -> list[dict]:
    """Return active rewards the customer can afford with *points_balance*."""
    rows = conn.execute(
        """
        SELECT reward_id, name, description, cost_points, product_id, valid_until
        FROM rewards
        WHERE org_id = ? AND is_active = true
          AND (valid_until IS NULL OR valid_until >= current_date)
          AND cost_points <= ?
        ORDER BY cost_points
        """,
        [org_id, points_balance],
    ).fetchall()
    cols = ["reward_id", "name", "description", "cost_points", "product_id", "valid_until"]
    return [dict(zip(cols, r)) for r in rows]
