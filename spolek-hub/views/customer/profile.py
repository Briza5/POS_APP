"""Customer profile view – mobilní layout."""

from __future__ import annotations

import time
from typing import Optional

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import fmt as fmt_dt
from services.loyalty_service import get_available_rewards, get_transaction_history
from services.notification_service import get_unread_notifications, mark_as_read
from services.order_service import get_active_fulfillment_order, list_orders
from services.order_status_service import transition_order
from services.permission_service import can_customer_order
from services.tab_service import (
    get_open_tab_for_customer,
    get_tab_orders,
    list_tabs,
    open_tab,
)
from views.customer.order_menu import render_order_menu
from views.customer.rewards import render_rewards


def render_customer_profile(
    conn: duckdb.DuckDBPyConnection,
    uid_token: str,
    table_token: Optional[str] = None,
    org_ctx: Optional[OrgContext] = None,
) -> None:
    """Render the mobile customer profile page.

    Handles both ?uid=xyz and ?uid=xyz&table=abc URLs.
    """
    from core.auth import validate_uid_token, validate_table_token
    from core.config import load_first_org_context

    # Resolve customer
    customer = validate_uid_token(uid_token, conn)
    if customer is None:
        st.error("Neplatný nebo neaktivní odkaz.")
        st.stop()

    # Resolve table context
    table_info: Optional[dict] = None
    if table_token:
        table_info = validate_table_token(table_token, conn)

    # Resolve org context
    if org_ctx is None:
        org_ctx = load_first_org_context(conn)

    table_number = table_info["table_number"] if table_info else None

    # ── Customer polling (10s) ─────────────────────────────────────────
    now = time.time()
    if now - st.session_state.get("customer_last_poll", 0) > 10:
        st.session_state["customer_last_poll"] = now
        st.rerun()

    # ── Header ────────────────────────────────────────────────────────
    st.markdown(f"## Ahoj, {customer['display_name']}! 👋")
    if table_number:
        st.info(f"📍 Stůl {table_number}")

    # Points + progress
    balance = customer["points_balance"]
    rewards = get_available_rewards(conn, org_ctx.org_id, 99999)
    next_reward = next((r for r in rewards if r["cost_points"] > balance), None)

    col_pts, col_prog = st.columns([1, 2])
    col_pts.metric("🏆 Body", balance)
    if next_reward:
        progress = min(1.0, balance / next_reward["cost_points"])
        col_prog.progress(progress, text=f"Do '{next_reward['name']}': {next_reward['cost_points'] - balance} b")
    else:
        col_prog.success("Máte nárok na všechny odměny!")

    st.divider()

    # ── Active tab section ─────────────────────────────────────────────
    open_tab = get_open_tab_for_customer(conn, customer["customer_id"], org_ctx.org_id)
    if open_tab:
        with st.container(border=True):
            st.markdown(f"📋 **Váš otevřený účet: {open_tab['label']}**")
            tc1, tc2 = st.columns(2)
            tc1.metric("Celkem dosud", f"{open_tab['total_amount']} {org_ctx.currency}")
            tc2.metric("Nezaplaceno", f"{open_tab['total_amount'] - open_tab['total_paid']} {org_ctx.currency}")

            orders_on_tab = get_tab_orders(conn, open_tab["tab_id"])
            tc3, tc4 = st.columns(2)
            tc3.metric("Objednávek", len(orders_on_tab))

            with st.expander("Zobrazit detail účtu ▼"):
                for i, o in enumerate(orders_on_tab, 1):
                    pay_icon = "✅" if o["payment_status"] == "paid" else "⏳"
                    created = o["created_at"]
                    time_str = fmt_dt(created, "%H:%M")
                    st.write(f"{pay_icon} #{i} · {time_str} · {o['total_amount']} {org_ctx.currency}")
                    for item in o.get("items", []):
                        st.caption(f"  └ {item['product_name']} × {item['quantity']}")

        st.divider()

    # ── Active fulfillment order tracker ──────────────────────────────
    active_order = get_active_fulfillment_order(conn, customer["customer_id"])
    if active_order:
        _render_order_tracker(conn, org_ctx, customer, active_order)
        st.divider()

    # ── Order button ──────────────────────────────────────────────────
    can_order = can_customer_order(conn, customer["customer_id"], org_ctx.org_id)
    if can_order and not active_order:
        if st.button("📋 Přidat objednávku", use_container_width=True, type="primary"):
            st.session_state["show_order_menu"] = True

    if st.session_state.get("show_order_menu") and can_order and not active_order:
        # Resolve or create tab
        tab_id = _ensure_tab(conn, org_ctx, customer, table_number, table_info)
        render_order_menu(
            conn, customer, org_ctx, tab_id,
            table_number=table_number, source="customer",
        )
        if st.button("❌ Zavřít menu"):
            st.session_state["show_order_menu"] = False
            st.rerun()

    st.divider()

    # ── Tabs: body / odměny / objednávky / účty ───────────────────────
    t_pts, t_rewards, t_orders, t_tabs = st.tabs([
        "Moje body", "Odměny", "Historie objednávek", "Historie účtů"
    ])

    with t_pts:
        st.metric("Aktuální zůstatek", f"{balance} bodů")
        txns = get_transaction_history(conn, customer["customer_id"], limit=20)
        for t in txns:
            delta = t["points_delta"]
            color = "green" if delta > 0 else "red"
            sign = "+" if delta > 0 else ""
            created = t["created_at"]
            time_str = fmt_dt(created, "%d.%m %H:%M")
            st.markdown(f":{color}[{sign}{delta} b] &nbsp; {time_str} &nbsp; _{t.get('note') or t['txn_type']}_")

    with t_rewards:
        render_rewards(conn, customer, org_ctx)

    with t_orders:
        orders = list_orders(conn, org_ctx.org_id, customer_id=customer["customer_id"], limit=20)
        if orders:
            for o in orders:
                created = o["created_at"]
                time_str = fmt_dt(created, "%d.%m %H:%M")
                pay_icon = "✅" if o["payment_status"] == "paid" else "⏳"
                st.write(f"{pay_icon} {time_str} · {o['total_amount']} {org_ctx.currency} · {o['fulfillment_status']}")
        else:
            st.caption("Žádné objednávky.")

    with t_tabs:
        tabs_hist = list_tabs(conn, org_ctx.org_id, customer_id=customer["customer_id"])
        if tabs_hist:
            for tab_s in tabs_hist:
                icon = "📂" if tab_s.status == "open" else "✅"
                opened = fmt_dt(tab_s.opened_at, "%d.%m")
                st.write(f"{icon} {tab_s.label} · {opened} · {tab_s.total_amount} {org_ctx.currency}")
        else:
            st.caption("Žádné účty.")

    # ── Notifications ─────────────────────────────────────────────────
    notifs = get_unread_notifications(
        conn, "customer", target_id=customer["customer_id"], limit=5
    )
    if notifs:
        st.divider()
        st.subheader("🔔 Oznámení")
        for n in notifs:
            with st.container(border=True):
                st.markdown(f"**{n['title']}**")
                if n.get("body"):
                    st.caption(n["body"])
                if st.button("✓ Přečteno", key=f"notif_{n['notification_id']}"):
                    mark_as_read(conn, n["notification_id"])
                    if "sync" in st.session_state:
                        st.session_state["sync"].mark_dirty()
                    st.rerun()


# ---------------------------------------------------------------------------
# Fulfillment order tracker
# ---------------------------------------------------------------------------

_FULFILLMENT_STEPS = ["pending", "accepted", "in_progress", "ready", "completed"]
_STEP_LABELS = {
    "pending":     "⏳ Čeká na potvrzení",
    "accepted":    "👍 Přijato",
    "in_progress": "🔧 Připravuje se",
    "ready":       "✅ Připraveno k vyzvednutí",
    "completed":   "✔ Dokončeno",
    "cancelled":   "🚫 Zrušeno",
    "rejected":    "❌ Zamítnuto",
}


def _render_order_tracker(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    customer: dict,
    order: dict,
) -> None:
    """Show live fulfillment status tracker."""
    status = order["fulfillment_status"]
    label = _STEP_LABELS.get(status, status)

    with st.container(border=True):
        st.markdown(f"**Aktivní objednávka** · {label}")
        st.write(f"Celkem: {order['total_amount']} {org_ctx.currency}")

        # Progress steps
        if status in _FULFILLMENT_STEPS:
            idx = _FULFILLMENT_STEPS.index(status)
            progress = (idx + 1) / len(_FULFILLMENT_STEPS)
            st.progress(progress)

        # Cancel button (only if pending)
        if status == "pending":
            if st.button("🚫 Zrušit objednávku", use_container_width=True):
                transition_order(
                    conn, order["order_id"], "cancelled",
                    actor_type="customer",
                    actor_id=customer["customer_id"],
                )
                if "sync" in st.session_state:
                    st.session_state["sync"].mark_dirty()
                st.rerun()


# ---------------------------------------------------------------------------
# Tab resolver
# ---------------------------------------------------------------------------

def _ensure_tab(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    customer: dict,
    table_number: Optional[str],
    table_info: Optional[dict],
) -> str:
    """Return existing open tab_id or create a new one."""
    from services.tab_service import get_open_tab_for_customer

    existing = get_open_tab_for_customer(conn, customer["customer_id"], org_ctx.org_id)
    if existing:
        return existing["tab_id"]

    new_tab = open_tab(
        conn, org_ctx.org_id,
        payment_mode=org_ctx.default_tab_mode,
        customer_id=customer["customer_id"],
        table_number=table_number,
    )
    if "sync" in st.session_state:
        st.session_state["sync"].mark_dirty()
    return new_tab["tab_id"]
