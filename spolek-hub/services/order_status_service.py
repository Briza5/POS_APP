"""Order fulfillment status state machine."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Optional

import duckdb

from services import notification_service

# ---------------------------------------------------------------------------
# Valid fulfillment transitions
# ---------------------------------------------------------------------------

VALID_TRANSITIONS: dict[str, list[str]] = {
    "pending":     ["accepted", "rejected", "cancelled"],
    "accepted":    ["in_progress", "rejected", "cancelled"],
    "in_progress": ["ready", "cancelled"],
    "ready":       ["completed", "cancelled"],
    "completed":   [],   # terminal
    "cancelled":   [],   # terminal
    "rejected":    [],   # terminal
}


def transition_order(
    conn: duckdb.DuckDBPyConnection,
    order_id: str,
    new_fulfillment_status: str,
    actor_type: str,
    actor_id: str,
    reason: Optional[str] = None,
) -> dict:
    """Transition fulfillment_status of *order_id* to *new_fulfillment_status*.

    Raises:
        ValueError: if the transition is invalid or order not found.
    """
    row = conn.execute("SELECT * FROM orders WHERE order_id = ?", [order_id]).fetchone()
    if row is None:
        raise ValueError(f"Objednávka {order_id!r} nenalezena.")
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    order = dict(zip(cols, row))

    current = order["fulfillment_status"]
    allowed = VALID_TRANSITIONS.get(current, [])
    if new_fulfillment_status not in allowed:
        raise ValueError(
            f"Přechod {current!r} → {new_fulfillment_status!r} není povolen. "
            f"Povolené přechody: {allowed}"
        )

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    now_iso = now.isoformat()

    # Append to status_history
    try:
        history = json.loads(order.get("status_history") or "[]")
    except (json.JSONDecodeError, TypeError):
        history = []
    history.append({
        "from": current,
        "to": new_fulfillment_status,
        "actor_type": actor_type,
        "actor_id": actor_id,
        "reason": reason,
        "at": now_iso,
    })

    update_fields: dict = {
        "fulfillment_status": new_fulfillment_status,
        "status_history": json.dumps(history, ensure_ascii=False),
    }
    if new_fulfillment_status in ("rejected", "cancelled"):
        update_fields["rejection_reason"] = reason

    set_clause = ", ".join(f"{k} = ?" for k in update_fields)
    conn.execute(
        f"UPDATE orders SET {set_clause} WHERE order_id = ?",
        list(update_fields.values()) + [order_id],
    )

    customer_id = order.get("customer_id")

    # --- Notifications & side-effects ---
    if new_fulfillment_status == "accepted" and customer_id:
        notification_service.notify_order_accepted(conn, customer_id, order_id)

    elif new_fulfillment_status == "ready" and customer_id:
        notification_service.notify_order_ready(conn, customer_id, order_id)

    elif new_fulfillment_status in ("rejected", "cancelled"):
        # Return redeemed points
        redeemed = order.get("points_redeemed", 0)
        if redeemed and redeemed > 0 and customer_id:
            from services import loyalty_service
            loyalty_service.add_points(
                conn, customer_id, redeemed,
                note=f"Vrácení bodů za {new_fulfillment_status} objednávku",
                order_id=order_id,
            )

        # Deduct from tab total
        if order.get("tab_id"):
            conn.execute(
                "UPDATE tabs SET total_amount = total_amount - ? WHERE tab_id = ?",
                [float(order["total_amount"]), order["tab_id"]],
            )

        if new_fulfillment_status == "rejected" and customer_id:
            notification_service.notify_order_rejected(
                conn, customer_id, order_id, reason or ""
            )
        elif new_fulfillment_status == "cancelled" and actor_type == "customer":
            notification_service.notify_order_cancelled_by_customer(
                conn, order["org_id"], order_id
            )

    from services.order_service import get_order
    return get_order(conn, order_id)  # type: ignore[return-value]


def get_pending_orders(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> list[dict]:
    """Return orders awaiting staff action (pending or accepted), newest first."""
    rows = conn.execute(
        """
        SELECT o.*, c.display_name AS customer_name,
               t.label AS tab_label
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN tabs t ON o.tab_id = t.tab_id
        WHERE o.org_id = ?
          AND o.fulfillment_status IN ('pending', 'accepted', 'in_progress')
        ORDER BY o.created_at
        """,
        [org_id],
    ).fetchall()
    cols = [d[0] for d in conn.execute("DESCRIBE orders").fetchall()]
    cols += ["customer_name", "tab_label"]
    return [dict(zip(cols, r)) for r in rows]


def get_queue_summary(
    conn: duckdb.DuckDBPyConnection,
    org_id: str,
) -> dict:
    """Return a summary of the fulfillment queue."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE fulfillment_status = 'pending')     AS pending,
            COUNT(*) FILTER (WHERE fulfillment_status = 'accepted')    AS accepted,
            COUNT(*) FILTER (WHERE fulfillment_status = 'in_progress') AS in_progress,
            COUNT(*) FILTER (WHERE fulfillment_status = 'ready')       AS ready
        FROM orders
        WHERE org_id = ?
          AND fulfillment_status IN ('pending','accepted','in_progress','ready')
        """,
        [org_id],
    ).fetchone()
    return {
        "pending": row[0] if row else 0,
        "accepted": row[1] if row else 0,
        "in_progress": row[2] if row else 0,
        "ready": row[3] if row else 0,
        "total_active": sum(row) if row else 0,
    }
