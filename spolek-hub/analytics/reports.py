"""
Analytics and reporting functions for Spolkový Hospodský Systém.

All functions return pd.DataFrame and use DuckDB SQL directly.
Only paid orders are counted in revenue figures unless noted otherwise.
"""

from __future__ import annotations

from datetime import date
from typing import Optional

import duckdb
import pandas as pd

from core.timezone import prague_offset_hours


def daily_summary(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    day: Optional[date] = None,
) -> pd.DataFrame:
    """Hourly breakdown for *day* (defaults to today).

    Returns columns: hour, orders_count, revenue, avg_order_value
    """
    day_str = (day or date.today()).isoformat()
    tz_offset = prague_offset_hours()
    rows = conn.execute(
        f"""
        SELECT
            EXTRACT(HOUR FROM created_at + INTERVAL '{tz_offset} hours')::INTEGER AS hour,
            COUNT(*)                                AS orders_count,
            COALESCE(SUM(total_amount), 0)::DOUBLE AS revenue,
            COALESCE(AVG(total_amount), 0)::DOUBLE AS avg_order_value
        FROM orders
        WHERE org_id = ?
          AND DATE(created_at + INTERVAL '{tz_offset} hours') = ?
          AND payment_status = 'paid'
        GROUP BY hour
        ORDER BY hour
        """,
        [org_id, day_str],
    ).fetchall()
    return pd.DataFrame(
        rows,
        columns=["hour", "orders_count", "revenue", "avg_order_value"],
    )


def weekly_revenue(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    weeks: int = 4,
) -> pd.DataFrame:
    """Weekly revenue aggregation for the last *weeks* weeks.

    Returns columns: week_start, revenue, orders_count,
                     unique_customers, tabs_closed
    """
    tz_offset = prague_offset_hours()
    rows = conn.execute(
        f"""
        SELECT
            DATE_TRUNC('week', o.created_at + INTERVAL '{tz_offset} hours')::DATE AS week_start,
            COALESCE(SUM(o.total_amount), 0)::DOUBLE        AS revenue,
            COUNT(o.order_id)                                AS orders_count,
            COUNT(DISTINCT o.customer_id)                    AS unique_customers,
            COUNT(DISTINCT t.tab_id) FILTER (
                WHERE t.status = 'closed'
            )                                                AS tabs_closed
        FROM orders o
        LEFT JOIN tabs t ON o.tab_id = t.tab_id
        WHERE o.org_id = ?
          AND o.payment_status = 'paid'
          AND o.created_at >= current_date - (? * INTERVAL '7 days')
        GROUP BY week_start
        ORDER BY week_start
        """,
        [org_id, weeks],
    ).fetchall()
    return pd.DataFrame(
        rows,
        columns=["week_start", "revenue", "orders_count",
                 "unique_customers", "tabs_closed"],
    )


def top_products(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
    limit: int = 10,
) -> pd.DataFrame:
    """Top products by revenue in the given date range (paid orders only).

    Returns columns: name, category, quantity_sold, revenue
    """
    rows = conn.execute(
        """
        SELECT
            p.name,
            p.category,
            SUM(oi.quantity)                            AS quantity_sold,
            SUM(oi.quantity * oi.unit_price)::DOUBLE    AS revenue
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        JOIN orders   o ON oi.order_id   = o.order_id
        WHERE o.org_id = ?
          AND o.payment_status = 'paid'
          AND DATE(o.created_at) BETWEEN ? AND ?
        GROUP BY p.name, p.category
        ORDER BY revenue DESC
        LIMIT ?
        """,
        [org_id, date_from, date_to, limit],
    ).fetchall()
    return pd.DataFrame(rows, columns=["name", "category", "quantity_sold", "revenue"])


def customer_stats(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> pd.DataFrame:
    """Per-customer statistics.

    Returns columns: display_name, total_spent, points_balance,
                     orders_count, tabs_count, last_order_date,
                     membership_type, can_order, can_tab
    """
    rows = conn.execute(
        """
        SELECT
            c.display_name,
            c.total_spent,
            c.points_balance,
            COUNT(DISTINCT o.order_id)  AS orders_count,
            COUNT(DISTINCT t.tab_id)    AS tabs_count,
            MAX(DATE(o.created_at))     AS last_order_date,
            c.membership_type,
            cp.can_order,
            cp.can_tab
        FROM customers c
        LEFT JOIN customer_permissions cp
               ON c.customer_id = cp.customer_id AND c.org_id = cp.org_id
        LEFT JOIN orders o
               ON o.customer_id = c.customer_id AND o.payment_status = 'paid'
        LEFT JOIN tabs t
               ON t.customer_id = c.customer_id AND t.org_id = c.org_id
        WHERE c.org_id = ? AND c.is_active = true
        GROUP BY c.display_name, c.total_spent, c.points_balance,
                 c.membership_type, cp.can_order, cp.can_tab
        ORDER BY c.total_spent DESC
        """,
        [org_id],
    ).fetchall()
    return pd.DataFrame(rows, columns=[
        "display_name", "total_spent", "points_balance",
        "orders_count", "tabs_count", "last_order_date",
        "membership_type", "can_order", "can_tab",
    ])


def tab_stats(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """Tab-level statistics in the given date range.

    Returns columns: label, customer_name, payment_mode,
                     total_amount, opened_at, closed_at,
                     duration_minutes, orders_count, points_earned
    """
    rows = conn.execute(
        """
        SELECT
            t.label,
            c.display_name                                     AS customer_name,
            t.payment_mode,
            t.total_amount,
            t.opened_at,
            t.closed_at,
            CASE
                WHEN t.closed_at IS NOT NULL
                THEN EXTRACT(EPOCH FROM (t.closed_at - t.opened_at)) / 60
                ELSE NULL
            END::INTEGER                                       AS duration_minutes,
            COUNT(o.order_id)                                  AS orders_count,
            t.total_points_earned                              AS points_earned
        FROM tabs t
        LEFT JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN orders    o ON o.tab_id = t.tab_id
                             AND o.payment_status != 'voided'
        WHERE t.org_id = ?
          AND DATE(t.opened_at) BETWEEN ? AND ?
        GROUP BY t.tab_id, t.label, c.display_name, t.payment_mode,
                 t.total_amount, t.opened_at, t.closed_at,
                 t.total_points_earned
        ORDER BY t.opened_at DESC
        """,
        [org_id, date_from, date_to],
    ).fetchall()
    return pd.DataFrame(rows, columns=[
        "label", "customer_name", "payment_mode",
        "total_amount", "opened_at", "closed_at",
        "duration_minutes", "orders_count", "points_earned",
    ])


def order_source_breakdown(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """Revenue breakdown by order source.

    Returns columns: source, orders_count, revenue, percentage
    """
    rows = conn.execute(
        """
        WITH totals AS (
            SELECT SUM(total_amount) AS grand_total
            FROM orders
            WHERE org_id = ? AND payment_status = 'paid'
              AND DATE(created_at) BETWEEN ? AND ?
        )
        SELECT
            o.source,
            COUNT(*)                        AS orders_count,
            SUM(o.total_amount)::DOUBLE     AS revenue,
            ROUND(
                SUM(o.total_amount) * 100.0 / NULLIF(t.grand_total, 0), 1
            )                               AS percentage
        FROM orders o, totals t
        WHERE o.org_id = ?
          AND o.payment_status = 'paid'
          AND DATE(o.created_at) BETWEEN ? AND ?
        GROUP BY o.source, t.grand_total
        ORDER BY revenue DESC
        """,
        [org_id, date_from, date_to, org_id, date_from, date_to],
    ).fetchall()
    return pd.DataFrame(rows, columns=["source", "orders_count", "revenue", "percentage"])


def order_status_funnel(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """Fulfillment status funnel for customer orders.

    Returns columns: fulfillment_status, orders_count, percentage
    """
    rows = conn.execute(
        """
        WITH totals AS (
            SELECT COUNT(*) AS total
            FROM orders
            WHERE org_id = ? AND source != 'pos'
              AND DATE(created_at) BETWEEN ? AND ?
        )
        SELECT
            o.fulfillment_status,
            COUNT(*)                                                    AS orders_count,
            ROUND(COUNT(*) * 100.0 / NULLIF(t.total, 0), 1)           AS percentage
        FROM orders o, totals t
        WHERE o.org_id = ?
          AND o.source != 'pos'
          AND DATE(o.created_at) BETWEEN ? AND ?
        GROUP BY o.fulfillment_status, t.total
        ORDER BY orders_count DESC
        """,
        [org_id, date_from, date_to, org_id, date_from, date_to],
    ).fetchall()
    return pd.DataFrame(rows, columns=["fulfillment_status", "orders_count", "percentage"])


def payment_mode_breakdown(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """Comparison of 'immediate' vs 'tab' payment modes.

    Returns columns: payment_mode, tabs_count, revenue,
                     avg_tab_value, percentage
    """
    rows = conn.execute(
        """
        WITH totals AS (
            SELECT SUM(total_amount) AS grand_total
            FROM tabs
            WHERE org_id = ? AND status = 'closed'
              AND DATE(opened_at) BETWEEN ? AND ?
        )
        SELECT
            t.payment_mode,
            COUNT(*)                        AS tabs_count,
            COALESCE(SUM(t.total_amount), 0)::DOUBLE AS revenue,
            COALESCE(AVG(t.total_amount), 0)::DOUBLE AS avg_tab_value,
            ROUND(
                SUM(t.total_amount) * 100.0 / NULLIF(tot.grand_total, 0), 1
            )                               AS percentage
        FROM tabs t, totals tot
        WHERE t.org_id = ?
          AND t.status = 'closed'
          AND DATE(t.opened_at) BETWEEN ? AND ?
        GROUP BY t.payment_mode, tot.grand_total
        ORDER BY revenue DESC
        """,
        [org_id, date_from, date_to, org_id, date_from, date_to],
    ).fetchall()
    return pd.DataFrame(rows, columns=[
        "payment_mode", "tabs_count", "revenue", "avg_tab_value", "percentage"
    ])


def inventory_status(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> pd.DataFrame:
    """Current inventory status with low-stock flags.

    Returns columns: name, category, quantity_on_hand, unit,
                     low_stock_alert, status
    """
    rows = conn.execute(
        """
        SELECT
            p.name,
            p.category,
            i.quantity_on_hand,
            i.unit,
            p.low_stock_alert,
            CASE
                WHEN p.low_stock_alert IS NULL      THEN 'OK'
                WHEN i.quantity_on_hand <= 0        THEN 'CRITICAL'
                WHEN i.quantity_on_hand
                     <= p.low_stock_alert           THEN 'LOW'
                ELSE 'OK'
            END AS status
        FROM inventory i
        JOIN products   p ON i.product_id   = p.product_id
        JOIN warehouses w ON i.warehouse_id = w.warehouse_id
        WHERE w.org_id = ? AND p.track_inventory = true
        ORDER BY status DESC, p.name
        """,
        [org_id],
    ).fetchall()
    return pd.DataFrame(rows, columns=[
        "name", "category", "quantity_on_hand", "unit", "low_stock_alert", "status"
    ])


def revenue_by_category(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
    date_from: str,
    date_to: str,
) -> pd.DataFrame:
    """Revenue breakdown by product category (paid orders only).

    Returns columns: category, revenue, orders_count, percentage
    """
    rows = conn.execute(
        """
        WITH totals AS (
            SELECT SUM(oi.quantity * oi.unit_price) AS grand_total
            FROM order_items oi
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.org_id = ? AND o.payment_status = 'paid'
              AND DATE(o.created_at) BETWEEN ? AND ?
        )
        SELECT
            COALESCE(p.category, 'Ostatní')        AS category,
            SUM(oi.quantity * oi.unit_price)::DOUBLE AS revenue,
            COUNT(DISTINCT o.order_id)               AS orders_count,
            ROUND(
                SUM(oi.quantity * oi.unit_price)
                * 100.0 / NULLIF(t.grand_total, 0), 1
            )                                       AS percentage
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        JOIN orders   o ON oi.order_id   = o.order_id,
        totals t
        WHERE o.org_id = ?
          AND o.payment_status = 'paid'
          AND DATE(o.created_at) BETWEEN ? AND ?
        GROUP BY category, t.grand_total
        ORDER BY revenue DESC
        """,
        [org_id, date_from, date_to, org_id, date_from, date_to],
    ).fetchall()
    return pd.DataFrame(rows, columns=["category", "revenue", "orders_count", "percentage"])
