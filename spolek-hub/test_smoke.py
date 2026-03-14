"""
Smoke tests for Spolkový Hospodský Systém.

Runs entirely in-memory (no file I/O, no GDrive).
Exits with code 1 on first failure.

Usage:
    python test_smoke.py
"""

from __future__ import annotations

import sys
import traceback


def _run() -> None:
    import duckdb

    from core.database import DatabaseManager, is_empty, seed_demo_data

    # ── 1. DB init + seed ────────────────────────────────────────────────
    print("1. DB init + seed …")
    db = DatabaseManager(":memory:")
    db.initialize()
    conn = db.get_connection()
    assert is_empty(conn), "DB should be empty before seed"
    seed_demo_data(conn)
    assert not is_empty(conn), "DB should not be empty after seed"

    # Grab org and one customer
    org_row = conn.execute("SELECT org_id FROM organizations LIMIT 1").fetchone()
    assert org_row, "No organization after seed"
    org_id = org_row[0]

    cust_row = conn.execute(
        "SELECT customer_id, uid_token FROM customers WHERE org_id = ? LIMIT 1",
        [org_id],
    ).fetchone()
    assert cust_row, "No customers after seed"
    customer_id, uid_token = cust_row

    # ── 2. open_tab (tab mode) ───────────────────────────────────────────
    print("2. open_tab (tab mode) …")
    from services.tab_service import open_tab, get_tab, calculate_change, get_open_tab_for_customer

    # Close any existing open tab from seed data for this customer
    existing_tab = get_open_tab_for_customer(conn, customer_id, org_id)
    if existing_tab:
        conn.execute(
            "UPDATE tabs SET status = 'closed', closed_at = current_timestamp WHERE tab_id = ?",
            [existing_tab["tab_id"]],
        )

    tab = open_tab(
        conn, org_id, payment_mode="tab",
        customer_id=customer_id,
        label="Smoke Tab A",
    )
    tab_id = tab["tab_id"]
    assert tab["status"] == "open", f"Expected open, got {tab['status']}"
    assert tab["payment_mode"] == "tab"

    # ── 3. create_pos_order × 2 ─────────────────────────────────────────
    print("3. create_pos_order × 2 …")
    from decimal import Decimal
    from services.order_service import OrderItem, create_pos_order

    prod_row = conn.execute(
        "SELECT product_id, price FROM products WHERE org_id = ? AND is_active = true LIMIT 1",
        [org_id],
    ).fetchone()
    assert prod_row, "No products after seed"
    product_id, price = prod_row[0], prod_row[1]

    items1 = [OrderItem(product_id=product_id, quantity=2, unit_price=Decimal(str(price)))]
    o1 = create_pos_order(conn, org_id, items1, tab_id=tab_id)
    assert o1["payment_status"] == "unpaid"

    items2 = [OrderItem(product_id=product_id, quantity=1, unit_price=Decimal(str(price)))]
    o2 = create_pos_order(conn, org_id, items2, tab_id=tab_id)

    tab_after = get_tab(conn, tab_id)
    expected_total = Decimal(str(price)) * 3
    assert tab_after["total_amount"] == expected_total, (
        f"Tab total mismatch: {tab_after['total_amount']} != {expected_total}"
    )

    # ── 4. calculate_change ──────────────────────────────────────────────
    print("4. calculate_change(145, 200) …")
    change = calculate_change(Decimal("145"), Decimal("200"))
    assert change["is_sufficient"] is True
    assert change["amount_change"] == Decimal("55"), f"Expected 55, got {change['amount_change']}"

    # ── 5. close_tab with points ─────────────────────────────────────────
    print("5. close_tab + verify points …")
    from services.tab_service import close_tab

    pts_before_row = conn.execute(
        "SELECT points_balance FROM customers WHERE customer_id = ?",
        [customer_id],
    ).fetchone()
    pts_before = pts_before_row[0] if pts_before_row else 0

    from core.config import load_first_org_context
    org_ctx = load_first_org_context(conn)

    close_tab(
        conn, tab_id,
        amount_tendered=Decimal(str(tab_after["total_amount"])) + 50,
        payment_method="cash",
    )

    closed_tab = get_tab(conn, tab_id)
    assert closed_tab["status"] == "closed", f"Expected closed, got {closed_tab['status']}"

    if org_ctx.modules.loyalty:
        pts_after_row = conn.execute(
            "SELECT points_balance FROM customers WHERE customer_id = ?",
            [customer_id],
        ).fetchone()
        pts_after = pts_after_row[0] if pts_after_row else 0
        assert pts_after >= pts_before, "Points should not decrease after tab close"

    # ── 6. immediate tab + pay_single_order ──────────────────────────────
    print("6. open_tab (immediate) + pay_single_order …")
    from services.tab_service import pay_single_order

    tab_imm = open_tab(
        conn, org_id, payment_mode="immediate",
        label="Smoke Immediate",
    )
    tab_imm_id = tab_imm["tab_id"]

    items3 = [OrderItem(product_id=product_id, quantity=1, unit_price=Decimal(str(price)))]
    o3 = create_pos_order(conn, org_id, items3, tab_id=tab_imm_id)
    order_id = o3["order_id"]

    result = pay_single_order(
        conn, order_id,
        amount_tendered=Decimal(str(price)) + 20,
        payment_method="card",
    )
    assert result["change"] >= Decimal("0"), f"Unexpected change: {result['change']}"
    paid_order = conn.execute(
        "SELECT payment_status FROM orders WHERE order_id = ?", [order_id]
    ).fetchone()
    assert paid_order and paid_order[0] == "paid", "Order should be paid"

    # ── 7. create_customer_order + full state transition ─────────────────
    print("7. create_customer_order + transition through all states …")
    from services.order_service import create_customer_order
    from services.order_status_service import transition_order, get_queue_summary

    # Grant order permission if not already
    existing_perm = conn.execute(
        "SELECT permission_id FROM customer_permissions WHERE customer_id = ? AND org_id = ?",
        [customer_id, org_id],
    ).fetchone()
    if existing_perm:
        conn.execute(
            "UPDATE customer_permissions SET can_order = true, can_tab = true WHERE customer_id = ? AND org_id = ?",
            [customer_id, org_id],
        )
    else:
        import uuid
        conn.execute(
            "INSERT INTO customer_permissions (permission_id, customer_id, org_id, can_order, can_tab, credit_limit) VALUES (?, ?, ?, true, true, 1000)",
            [str(uuid.uuid4()), customer_id, org_id],
        )

    # Open a fresh tab for the customer order
    tab_co = open_tab(
        conn, org_id, payment_mode="tab",
        customer_id=customer_id,
        label="Smoke Customer Order",
    )
    tab_co_id = tab_co["tab_id"]

    cust_items = [OrderItem(product_id=product_id, quantity=1, unit_price=Decimal(str(price)))]
    co = create_customer_order(
        conn, org_id, cust_items,
        customer_id=customer_id,
        tab_id=tab_co_id,
        source="customer",
    )
    assert co["fulfillment_status"] == "pending"

    for next_status in ["accepted", "in_progress", "ready", "completed"]:
        transition_order(
            conn, co["order_id"], next_status,
            actor_type="admin", actor_id="smoke_test",
        )

    final = conn.execute(
        "SELECT fulfillment_status FROM orders WHERE order_id = ?",
        [co["order_id"]],
    ).fetchone()
    assert final and final[0] == "completed", f"Expected completed, got {final}"

    # ── 8. get_queue_summary ─────────────────────────────────────────────
    print("8. get_queue_summary …")
    summary = get_queue_summary(conn, org_id)
    assert isinstance(summary, dict)
    assert "pending" in summary

    print()
    print("[OK] Vse v poradku / All tests passed")


if __name__ == "__main__":
    try:
        _run()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
