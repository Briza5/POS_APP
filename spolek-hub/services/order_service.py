"""Order creation and retrieval."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import duckdb

from services import notification_service
from services.permission_service import can_customer_order


@dataclass
class OrderItem:
    """A single line item in an order."""

    product_id: str
    quantity: int
    unit_price: Decimal
    points_earned: int = 0


# ---------------------------------------------------------------------------
# POS order (staff-created)
# ---------------------------------------------------------------------------

def create_pos_order(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    items: list[OrderItem],
    tab_id: str,
    customer_id: Optional[str] = None,
    payment_method: str = "cash",
    points_redeemed: int = 0,
    event_id: Optional[str] = None,
    table_number: Optional[str] = None,
    created_by: Optional[str] = None,
    note: Optional[str] = None,
    queue: bool = False,
) -> dict:
    """Create a POS order.

    Args:
        queue: If True, fulfillment_status='pending' (appears in order queue
               so staff can tick off delivery). If False (default),
               fulfillment_status='completed' immediately.

    Payment status depends on tab.payment_mode:
      - 'immediate' → 'unpaid' (pay via pay_single_order right after)
      - 'tab'       → 'unpaid' (pay when tab closes)

    Does NOT call add_points – that is handled by tab_service.

    Returns:
        The created order as a dict.
    """
    total = sum(Decimal(str(i.unit_price)) * i.quantity for i in items)

    # Resolve tab payment_mode
    tab_row = conn.execute(
        "SELECT payment_mode, table_number FROM tabs WHERE tab_id = ?",
        [tab_id],
    ).fetchone()
    if tab_row is None:
        raise ValueError(f"Tab {tab_id!r} nenalezen.")
    # table_number from tab if not explicitly provided
    if not table_number:
        table_number = tab_row[1]

    order_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn.execute(
        """
        INSERT INTO orders
            (order_id, org_id, tab_id, customer_id, event_id,
             table_number, source, created_at, total_amount,
             points_redeemed, payment_method,
             payment_status, fulfillment_status, note, created_by)
        VALUES (?, ?, ?, ?, ?, ?, 'pos', ?, ?, ?, ?,
                'unpaid', ?, ?, ?)
        """,
        [
            order_id, org_id, tab_id, customer_id, event_id,
            table_number, now, float(total),
            points_redeemed, payment_method,
            "pending" if queue else "completed", note, created_by,
        ],
    )

    _insert_items(conn, order_id, items)
    _deduct_inventory(conn, org_id, items)

    # Update tab total_amount
    conn.execute(
        "UPDATE tabs SET total_amount = total_amount + ? WHERE tab_id = ?",
        [float(total), tab_id],
    )

    return get_order(conn, order_id)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Customer order (app / QR)
# ---------------------------------------------------------------------------

def create_customer_order(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    items: list[OrderItem],
    customer_id: str,
    tab_id: str,
    source: str,
    table_number: Optional[str] = None,
    points_redeemed: int = 0,
    note: Optional[str] = None,
) -> dict:
    """Create a customer-submitted order (fulfillment_status='pending').

    Raises:
        PermissionError: if customer lacks can_order permission.
        ValueError: if customer already has an active fulfillment order.
    """
    if not can_customer_order(conn, customer_id, org_id):
        raise PermissionError("Zákazník nemá oprávnění objednávat přes app.")

    active = get_active_fulfillment_order(conn, customer_id)
    if active:
        raise ValueError(
            "Zákazník již má aktivní objednávku ve frontě. "
            "Počkejte na její dokončení."
        )

    total = sum(Decimal(str(i.unit_price)) * i.quantity for i in items)

    if not table_number:
        tab_row = conn.execute(
            "SELECT table_number FROM tabs WHERE tab_id = ?", [tab_id]
        ).fetchone()
        if tab_row:
            table_number = tab_row[0]

    order_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn.execute(
        """
        INSERT INTO orders
            (order_id, org_id, tab_id, customer_id,
             table_number, source, created_at, total_amount,
             points_redeemed, payment_status,
             fulfillment_status, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'unpaid', 'pending', ?)
        """,
        [
            order_id, org_id, tab_id, customer_id,
            table_number, source, now, float(total),
            points_redeemed, note,
        ],
    )

    _insert_items(conn, order_id, items)

    # Update tab total_amount
    conn.execute(
        "UPDATE tabs SET total_amount = total_amount + ? WHERE tab_id = ?",
        [float(total), tab_id],
    )

    # Notify staff
    customer_row = conn.execute(
        "SELECT display_name FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    customer_name = customer_row[0] if customer_row else None
    notification_service.notify_new_order(
        conn, org_id, order_id, source,
        table_number, customer_name, total,
    )

    return get_order(conn, order_id)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Getters
# ---------------------------------------------------------------------------

def get_order(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
) -> Optional[dict]:
    """Return order dict or None."""
    row = conn.execute("SELECT * FROM orders WHERE order_id = ?", [order_id]).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    order = dict(zip(cols, row))
    order["items"] = get_order_items(conn, order_id)
    return order


def get_order_items(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
) -> list[dict]:
    """Return line items for *order_id*."""
    rows = conn.execute(
        """
        SELECT oi.item_id, oi.product_id, p.name AS product_name,
               oi.quantity, oi.unit_price, oi.points_earned
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        WHERE oi.order_id = ?
        """,
        [order_id],
    ).fetchall()
    cols = ["item_id", "product_id", "product_name", "quantity", "unit_price", "points_earned"]
    return [dict(zip(cols, r)) for r in rows]


def get_active_fulfillment_order(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
) -> Optional[dict]:
    """Return an order in an active fulfillment state, or None."""
    row = conn.execute(
        """
        SELECT * FROM orders
        WHERE customer_id = ?
          AND fulfillment_status IN ('pending','accepted','in_progress','ready')
        ORDER BY created_at DESC
        LIMIT 1
        """,
        [customer_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    return dict(zip(cols, row))


def list_orders(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    customer_id: Optional[str] = None,
    tab_id: Optional[str] = None,
    payment_status: Optional[str] = None,
    fulfillment_status: Optional[str] = None,
    limit: int = 100,
) -> list[dict]:
    """Return orders filtered by optional criteria."""
    query = "SELECT * FROM orders WHERE org_id = ?"
    params: list = [org_id]
    if date_from:
        query += " AND DATE(created_at) >= ?"
        params.append(date_from)
    if date_to:
        query += " AND DATE(created_at) <= ?"
        params.append(date_to)
    if customer_id:
        query += " AND customer_id = ?"
        params.append(customer_id)
    if tab_id:
        query += " AND tab_id = ?"
        params.append(tab_id)
    if payment_status:
        query += " AND payment_status = ?"
        params.append(payment_status)
    if fulfillment_status:
        query += " AND fulfillment_status = ?"
        params.append(fulfillment_status)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    return [dict(zip(cols, r)) for r in rows]


def void_order(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
    reason: str,
    voided_by: str,
) -> bool:
    """Void an order: mark voided, deduct from tab total, return redeemed points.

    Returns:
        True if voided successfully.
    """
    row = conn.execute("SELECT * FROM orders WHERE order_id = ?", [order_id]).fetchone()
    if row is None:
        return False
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    order = dict(zip(cols, row))

    conn.execute(
        """
        UPDATE orders
        SET payment_status = 'voided', fulfillment_status = 'cancelled',
            rejection_reason = ?
        WHERE order_id = ?
        """,
        [reason, order_id],
    )

    # Deduct from tab total
    if order.get("tab_id"):
        conn.execute(
            "UPDATE tabs SET total_amount = total_amount - ? WHERE tab_id = ?",
            [float(order["total_amount"]), order["tab_id"]],
        )

    # Return redeemed points
    from services import loyalty_service as ls
    redeemed = order.get("points_redeemed", 0)
    if redeemed and redeemed > 0 and order.get("customer_id"):
        ls.add_points(
            conn, order["customer_id"], redeemed,
            note=f"Vrácení bodů za stornovanou objednávku #{order_id[:8]}",
            order_id=order_id,
        )

    return True


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _insert_items(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
    items: list[OrderItem],
) -> None:
    for item in items:
        conn.execute(
            """
            INSERT INTO order_items
                (item_id, order_id, product_id, quantity, unit_price, points_earned)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                str(uuid.uuid4()), order_id, item.product_id,
                item.quantity, float(item.unit_price), item.points_earned,
            ],
        )


def _deduct_inventory(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    items: list[OrderItem],
) -> None:
    """Deduct inventory for products with track_inventory=true."""
    for item in items:
        row = conn.execute(
            "SELECT track_inventory FROM products WHERE product_id = ?",
            [item.product_id],
        ).fetchone()
        if not row or not row[0]:
            continue
        wh_row = conn.execute(
            "SELECT warehouse_id FROM warehouses WHERE org_id = ? AND is_default = true LIMIT 1",
            [org_id],
        ).fetchone()
        if not wh_row:
            continue
        warehouse_id = wh_row[0]
        conn.execute(
            """
            UPDATE inventory
            SET quantity_on_hand = quantity_on_hand - ?
            WHERE warehouse_id = ? AND product_id = ?
            """,
            [item.quantity, warehouse_id, item.product_id],
        )
