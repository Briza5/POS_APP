"""Tab (účet/lístek) business logic."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import duckdb

from services import loyalty_service, notification_service
from services.permission_service import can_customer_tab


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class TabSummary:
    """Denormalised view of a tab for list/display use."""

    tab_id: str
    label: str
    customer_name: Optional[str]
    table_number: Optional[str]
    payment_mode: str
    status: str
    total_amount: Decimal
    total_paid: Decimal
    unpaid_amount: Decimal
    orders_count: int
    unpaid_orders: int
    opened_at: datetime
    closed_at: Optional[datetime]


# ---------------------------------------------------------------------------
# Open / create
# ---------------------------------------------------------------------------

def open_tab(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    payment_mode: str,
    customer_id: Optional[str] = None,
    table_number: Optional[str] = None,
    event_id: Optional[str] = None,
    label: Optional[str] = None,
    opened_by: Optional[str] = None,
) -> dict:
    """Open a new tab and return its dict.

    Raises:
        ValueError: if the customer already has an open tab, or lacks can_tab
                    permission when payment_mode='tab'.
    """
    # Validate: one open tab per customer
    if customer_id:
        existing = get_open_tab_for_customer(conn, customer_id, org_id)
        if existing:
            raise ValueError(
                f"Zákazník již má otevřený účet: {existing['label']!r}."
            )
        if payment_mode == "tab" and not can_customer_tab(conn, customer_id, org_id):
            raise ValueError(
                "Zákazník nemá oprávnění mít otevřený účet (can_tab=False)."
            )

    # Auto-generate label
    if not label:
        today_str = datetime.now(timezone.utc).strftime("%d.%m.%Y")
        if table_number:
            time_str = datetime.now(timezone.utc).strftime("%H:%M")
            label = f"Stůl {table_number} – {time_str}"
        else:
            # Count today's tabs for this org to get a sequential number
            row = conn.execute(
                """
                SELECT COUNT(*) FROM tabs
                WHERE org_id = ?
                  AND DATE(opened_at) = current_date
                """,
                [org_id],
            ).fetchone()
            n = (row[0] if row else 0) + 1
            label = f"Účet #{n} – {today_str}"

    tab_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    conn.execute(
        """
        INSERT INTO tabs
            (tab_id, org_id, customer_id, event_id, table_number,
             label, payment_mode, status, opened_at, opened_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?)
        """,
        [tab_id, org_id, customer_id, event_id, table_number,
         label, payment_mode, now, opened_by],
    )
    return get_tab(conn, tab_id)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Getters
# ---------------------------------------------------------------------------

def get_tab(conn: duckdb.DuckDBPyConnection, tab_id: str) -> Optional[dict]:
    """Return tab dict or None."""
    row = conn.execute(
        "SELECT * FROM tabs WHERE tab_id = ?",
        [tab_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute("DESCRIBE tabs").fetchall()]
    return dict(zip(cols, row))


def get_open_tab_for_customer(
    conn: duckdb.DuckDBPyConnection,
    customer_id: str,
    org_id: str,
) -> Optional[dict]:
    """Return the customer's open tab or None."""
    row = conn.execute(
        "SELECT * FROM tabs WHERE customer_id = ? AND org_id = ? AND status = 'open'",
        [customer_id, org_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute("DESCRIBE tabs").fetchall()]
    return dict(zip(cols, row))


def list_open_tabs(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> list[TabSummary]:
    """Return all open tabs, oldest first."""
    return list_tabs(conn, org_id, status="open")


def list_tabs(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[str] = None,
    customer_id: Optional[str] = None,
) -> list[TabSummary]:
    """Return tabs with aggregated order counts."""
    query = """
        SELECT
            t.tab_id,
            t.label,
            c.display_name  AS customer_name,
            t.table_number,
            t.payment_mode,
            t.status,
            t.total_amount,
            t.total_paid,
            (t.total_amount - t.total_paid) AS unpaid_amount,
            COUNT(o.order_id)               AS orders_count,
            COUNT(o.order_id) FILTER (
                WHERE o.payment_status = 'unpaid'
            )                               AS unpaid_orders,
            t.opened_at,
            t.closed_at
        FROM tabs t
        LEFT JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN orders o    ON o.tab_id = t.tab_id
                              AND o.payment_status != 'voided'
        WHERE t.org_id = ?
    """
    params: list = [org_id]
    if status:
        query += " AND t.status = ?"
        params.append(status)
    if customer_id:
        query += " AND t.customer_id = ?"
        params.append(customer_id)
    if date_from:
        query += " AND DATE(t.opened_at) >= ?"
        params.append(date_from)
    if date_to:
        query += " AND DATE(t.opened_at) <= ?"
        params.append(date_to)
    query += " GROUP BY t.tab_id, t.label, c.display_name, t.table_number, t.payment_mode, t.status, t.total_amount, t.total_paid, t.opened_at, t.closed_at"
    query += " ORDER BY t.opened_at"

    rows = conn.execute(query, params).fetchall()
    result = []
    for r in rows:
        result.append(TabSummary(
            tab_id=r[0],
            label=r[1],
            customer_name=r[2],
            table_number=r[3],
            payment_mode=r[4],
            status=r[5],
            total_amount=Decimal(str(r[6])),
            total_paid=Decimal(str(r[7])),
            unpaid_amount=Decimal(str(r[8])),
            orders_count=r[9],
            unpaid_orders=r[10],
            opened_at=r[11],
            closed_at=r[12],
        ))
    return result


def get_tab_orders(conn: duckdb.DuckDBPyConnection, tab_id: str) -> list[dict]:
    """Return all orders on the tab with their items."""
    orders = conn.execute(
        """
        SELECT order_id, org_id, tab_id, customer_id, source, created_at,
               total_amount, points_earned, points_redeemed, payment_method,
               payment_status, fulfillment_status, note, created_by
        FROM orders
        WHERE tab_id = ?
        ORDER BY created_at
        """,
        [tab_id],
    ).fetchall()
    order_cols = [
        "order_id", "org_id", "tab_id", "customer_id", "source", "created_at",
        "total_amount", "points_earned", "points_redeemed", "payment_method",
        "payment_status", "fulfillment_status", "note", "created_by",
    ]
    result = []
    for o in orders:
        od = dict(zip(order_cols, o))
        # Attach items
        items = conn.execute(
            """
            SELECT oi.item_id, oi.product_id, p.name, oi.quantity,
                   oi.unit_price, oi.points_earned
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            WHERE oi.order_id = ?
            """,
            [od["order_id"]],
        ).fetchall()
        item_cols = ["item_id", "product_id", "product_name", "quantity", "unit_price", "points_earned"]
        od["items"] = [dict(zip(item_cols, i)) for i in items]
        result.append(od)
    return result


def get_tab_summary(conn: duckdb.DuckDBPyConnection, tab_id: str) -> TabSummary:
    """Return a TabSummary for *tab_id*.

    Raises:
        ValueError: if tab not found.
    """
    summaries = list_tabs(conn, _get_tab_org(conn, tab_id))
    for s in summaries:
        if s.tab_id == tab_id:
            return s
    raise ValueError(f"Tab {tab_id!r} nenalezen.")


# ---------------------------------------------------------------------------
# Payment helpers
# ---------------------------------------------------------------------------

def calculate_change(
    amount_due: Decimal,
    amount_tendered: Decimal,
) -> dict:
    """Calculate change for a cash payment.

    Returns a dict with keys:
        amount_due, amount_tendered, amount_change, is_sufficient, shortfall
    """
    change = amount_tendered - amount_due
    return {
        "amount_due": amount_due,
        "amount_tendered": amount_tendered,
        "amount_change": change,
        "is_sufficient": change >= Decimal("0"),
        "shortfall": max(Decimal("0"), -change),
    }


# ---------------------------------------------------------------------------
# Close / void / reopen
# ---------------------------------------------------------------------------

def close_tab(
    conn: duckdb.DuckDBPyConnection,
    tab_id: str,
    amount_tendered: Decimal,
    payment_method: str = "cash",
    note: Optional[str] = None,
    closed_by: Optional[str] = None,
) -> dict:
    """Close a tab, record payment, award loyalty points.

    Returns:
        dict with keys: tab, payment, points_earned, amount_change, orders_settled

    Raises:
        ValueError: if tab is not open, or tendered < due.
    """
    tab = get_tab(conn, tab_id)
    if tab is None:
        raise ValueError(f"Tab {tab_id!r} nenalezen.")
    if tab["status"] != "open":
        raise ValueError(f"Tab má status {tab['status']!r}, nelze uzavřít.")

    # Calculate amount due from unpaid orders
    row = conn.execute(
        """
        SELECT COALESCE(SUM(total_amount), 0)
        FROM orders
        WHERE tab_id = ? AND payment_status = 'unpaid'
        """,
        [tab_id],
    ).fetchone()
    amount_due = Decimal(str(row[0]))

    change_info = calculate_change(amount_due, amount_tendered)
    if not change_info["is_sufficient"]:
        raise ValueError(
            f"Nedostatečná platba: chybí {change_info['shortfall']} Kč."
        )

    # Record payment
    payment_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(
        """
        INSERT INTO tab_payments
            (payment_id, tab_id, amount_due, amount_tendered,
             amount_change, payment_method, note, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            payment_id, tab_id, float(amount_due), float(amount_tendered),
            float(change_info["amount_change"]), payment_method, note, closed_by,
        ],
    )

    # Mark all unpaid orders as paid
    conn.execute(
        "UPDATE orders SET payment_status = 'paid' WHERE tab_id = ? AND payment_status = 'unpaid'",
        [tab_id],
    )
    orders_settled = conn.execute(
        "SELECT COUNT(*) FROM orders WHERE tab_id = ? AND payment_status = 'paid'",
        [tab_id],
    ).fetchone()[0]

    # Loyalty points (only for identified customers, completed+paid orders)
    points_earned = 0
    customer_id = tab.get("customer_id")
    if customer_id:
        # Get org points_per_czk
        org_row = conn.execute(
            "SELECT points_per_czk FROM organizations WHERE org_id = ?",
            [tab["org_id"]],
        ).fetchone()
        pts_rate = float(org_row[0]) if org_row else 1.0

        # Sum up points from all completed orders on this tab
        completed_orders = conn.execute(
            """
            SELECT order_id, total_amount FROM orders
            WHERE tab_id = ? AND payment_status = 'paid'
              AND fulfillment_status = 'completed'
              AND points_earned = 0
            """,
            [tab_id],
        ).fetchall()

        total_earned = 0
        for order_id, order_amount in completed_orders:
            pts = loyalty_service.calculate_points_earned(
                Decimal(str(order_amount)), pts_rate
            )
            if pts > 0:
                conn.execute(
                    "UPDATE orders SET points_earned = ? WHERE order_id = ?",
                    [pts, order_id],
                )
                total_earned += pts

        if total_earned > 0:
            loyalty_service.add_points(
                conn, customer_id, total_earned,
                note=f"Uzavření účtu: {tab['label']}",
                tab_id=tab_id,
            )
            conn.execute(
                "UPDATE customers SET total_spent = total_spent + ? WHERE customer_id = ?",
                [float(amount_due), customer_id],
            )
            points_earned = total_earned

    # Update tab
    conn.execute(
        """
        UPDATE tabs
        SET status = 'closed',
            closed_at = ?,
            closed_by = ?,
            total_paid = ?,
            total_points_earned = ?
        WHERE tab_id = ?
        """,
        [now, closed_by, float(amount_tendered - change_info["amount_change"]),
         points_earned, tab_id],
    )

    # Notify customer
    if customer_id:
        notification_service.notify_tab_closed(
            conn, customer_id, tab_id, amount_due, points_earned
        )

    return {
        "tab": get_tab(conn, tab_id),
        "payment": {"payment_id": payment_id, "amount_due": amount_due,
                    "amount_tendered": amount_tendered,
                    "amount_change": change_info["amount_change"]},
        "points_earned": points_earned,
        "amount_change": change_info["amount_change"],
        "orders_settled": orders_settled,
    }


def void_tab(
    conn: duckdb.DuckDBPyConnection,
    tab_id: str,
    reason: str,
    voided_by: str,
) -> None:
    """Void a tab – no points awarded."""
    tab = get_tab(conn, tab_id)
    if tab is None:
        raise ValueError(f"Tab {tab_id!r} nenalezen.")
    conn.execute(
        "UPDATE orders SET payment_status = 'voided' WHERE tab_id = ?",
        [tab_id],
    )
    conn.execute(
        """
        UPDATE tabs
        SET status = 'void', closed_at = current_timestamp, closed_by = ?
        WHERE tab_id = ?
        """,
        [voided_by, tab_id],
    )


def pay_single_order(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
    amount_tendered: Decimal,
    payment_method: str = "cash",
    closed_by: Optional[str] = None,
) -> dict:
    """Pay a single order (tab mode='immediate').

    Returns:
        dict with keys: change, points_earned

    Raises:
        ValueError: if order is not unpaid or tendered < due.
    """
    row = conn.execute(
        "SELECT * FROM orders WHERE order_id = ?",
        [order_id],
    ).fetchone()
    if row is None:
        raise ValueError(f"Objednávka {order_id!r} nenalezena.")

    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    order = dict(zip(cols, row))

    if order["payment_status"] != "unpaid":
        raise ValueError(
            f"Objednávka má status {order['payment_status']!r}, nelze zaplatit."
        )

    amount_due = Decimal(str(order["total_amount"]))
    change_info = calculate_change(amount_due, amount_tendered)
    if not change_info["is_sufficient"]:
        raise ValueError(
            f"Nedostatečná platba: chybí {change_info['shortfall']} Kč."
        )

    conn.execute(
        "UPDATE orders SET payment_status = 'paid' WHERE order_id = ?",
        [order_id],
    )
    # Update tab total_paid
    if order.get("tab_id"):
        conn.execute(
            "UPDATE tabs SET total_paid = total_paid + ? WHERE tab_id = ?",
            [float(amount_due), order["tab_id"]],
        )

    # Loyalty points
    points_earned = 0
    customer_id = order.get("customer_id")
    if customer_id and order["fulfillment_status"] == "completed":
        org_row = conn.execute(
            "SELECT points_per_czk FROM organizations WHERE org_id = ?",
            [order["org_id"]],
        ).fetchone()
        pts_rate = float(org_row[0]) if org_row else 1.0
        pts = loyalty_service.calculate_points_earned(amount_due, pts_rate)
        if pts > 0:
            loyalty_service.add_points(
                conn, customer_id, pts,
                note=f"Platba objednávky #{order_id[:8]}",
                order_id=order_id,
            )
            conn.execute(
                "UPDATE customers SET total_spent = total_spent + ? WHERE customer_id = ?",
                [float(amount_due), customer_id],
            )
            conn.execute(
                "UPDATE orders SET points_earned = ? WHERE order_id = ?",
                [pts, order_id],
            )
            points_earned = pts

    return {
        "change": change_info["amount_change"],
        "points_earned": points_earned,
    }


def reopen_tab(
    conn: duckdb.DuckDBPyConnection,
    tab_id: str,
    reopened_by: str,
) -> dict:
    """Reopen a closed tab (owner/manager only).

    Reverses payment status on orders and removes payment records.

    Returns:
        Updated tab dict.
    """
    tab = get_tab(conn, tab_id)
    if tab is None:
        raise ValueError(f"Tab {tab_id!r} nenalezen.")
    if tab["status"] not in ("closed", "void"):
        raise ValueError("Lze znovu otevřít jen uzavřený nebo stornovaný účet.")

    # Reverse points if any were awarded at close
    customer_id = tab.get("customer_id")
    pts_to_reverse = tab.get("total_points_earned", 0)
    if customer_id and pts_to_reverse and pts_to_reverse > 0:
        loyalty_service.adjust_points(
            conn, customer_id, -pts_to_reverse,
            note=f"Storno uzavření účtu: {tab['label']}",
            admin_id=reopened_by,
        )

    # Revert orders to unpaid
    conn.execute(
        "UPDATE orders SET payment_status = 'unpaid', points_earned = 0 WHERE tab_id = ?",
        [tab_id],
    )
    # Remove payment records
    conn.execute("DELETE FROM tab_payments WHERE tab_id = ?", [tab_id])

    # Reopen tab
    conn.execute(
        """
        UPDATE tabs
        SET status = 'open', closed_at = NULL, closed_by = NULL,
            total_paid = 0, total_points_earned = 0
        WHERE tab_id = ?
        """,
        [tab_id],
    )
    return get_tab(conn, tab_id)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Internal helper
# ---------------------------------------------------------------------------

def _get_tab_org(conn: duckdb.DuckDBPyConnection, tab_id: str) -> str:
    row = conn.execute("SELECT org_id FROM tabs WHERE tab_id = ?", [tab_id]).fetchone()
    if row is None:
        raise ValueError(f"Tab {tab_id!r} nenalezen.")
    return row[0]
