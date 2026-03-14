"""Admin view – fronta objednávek."""

from __future__ import annotations

import time

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import fmt as fmt_dt
from services.order_status_service import (
    get_pending_orders,
    get_queue_summary,
    transition_order,
)

# Fulfillment status labels and next actions
_STATUS_LABEL = {
    "pending":     "⏳ Čeká",
    "accepted":    "👍 Přijato",
    "in_progress": "🔧 Připravuje se",
    "ready":       "✅ Připraveno",
    "completed":   "✔ Dokončeno",
    "cancelled":   "🚫 Zrušeno",
    "rejected":    "❌ Zamítnuto",
}

_NEXT_ACTIONS: dict[str, list[tuple[str, str]]] = {
    "pending":     [("👍 Přijmout", "accepted"), ("❌ Zamítnout", "rejected")],
    "accepted":    [("🔧 Zahájit přípravu", "in_progress"), ("❌ Zamítnout", "rejected")],
    "in_progress": [("✅ Připraveno", "ready")],
    "ready":       [("✔ Dokončeno", "completed")],
}


def render_order_queue(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the live order queue with polling every 15 s."""

    # ── Polling ────────────────────────────────────────────────────────
    now = time.time()
    last_poll = st.session_state.get("last_queue_poll", 0)
    if now - last_poll > 15:
        st.session_state["last_queue_poll"] = now
        st.rerun()

    # ── Sound alert on new pending ─────────────────────────────────────
    summary = get_queue_summary(conn, org_ctx.org_id)
    prev_pending = st.session_state.get("prev_pending_count", 0)
    if summary["pending"] > prev_pending:
        st.markdown(
            """
            <audio autoplay>
              <source src="https://www.soundjay.com/buttons/sounds/button-09.mp3" type="audio/mpeg">
            </audio>
            """,
            unsafe_allow_html=True,
        )
    st.session_state["prev_pending_count"] = summary["pending"]

    st.title("🔔 Fronta objednávek")

    # ── Summary badges ─────────────────────────────────────────────────
    sc1, sc2, sc3, sc4 = st.columns(4)
    sc1.metric("⏳ Čeká", summary["pending"])
    sc2.metric("👍 Přijato", summary["accepted"])
    sc3.metric("🔧 Připravuje se", summary["in_progress"])
    sc4.metric("✅ Připraveno", summary["ready"])

    st.divider()

    orders = get_pending_orders(conn, org_ctx.org_id)
    if not orders:
        st.success("Fronta je prázdná 🎉")
        return

    admin = st.session_state.get("admin", {})
    actor_id = admin.get("admin_id", "unknown")

    for order in orders:
        status = order["fulfillment_status"]
        label = _STATUS_LABEL.get(status, status)
        customer = order.get("customer_name") or "Anonymní"
        table = order.get("table_number") or "—"
        tab_label = order.get("tab_label") or "—"
        created = order["created_at"]
        created_str = fmt_dt(created, "%H:%M")

        with st.container(border=True):
            hc1, hc2 = st.columns([4, 1])
            with hc1:
                st.markdown(
                    f"**{customer}** · Stůl {table} · {created_str} · Účet: {tab_label}"
                )
                st.caption(f"Stav: {label} · {order['total_amount']} {org_ctx.currency}")
            with hc2:
                st.markdown(f"### {label}")

            # Items
            items = order.get("items") or _load_items(conn, order["order_id"])
            for item in items:
                st.write(f"• {item.get('product_name', item.get('name', '?'))} × {item['quantity']}")

            if order.get("note"):
                st.info(f"📝 {order['note']}")

            # Action buttons
            actions = _NEXT_ACTIONS.get(status, [])
            if actions:
                btn_cols = st.columns(len(actions))
                for col, (btn_label, new_status) in zip(btn_cols, actions):
                    btn_key = f"action_{order['order_id']}_{new_status}"
                    if col.button(btn_label, key=btn_key, use_container_width=True):
                        reason = None
                        if new_status == "rejected":
                            reason = st.text_input(
                                "Důvod zamítnutí:", key=f"reason_{order['order_id']}"
                            )
                        transition_order(
                            conn, order["order_id"], new_status,
                            actor_type="admin", actor_id=actor_id, reason=reason,
                        )
                        st.session_state["sync"].mark_dirty()
                        st.rerun()


def _load_items(conn: duckdb.DuckDBPyConnection, order_id: str) -> list[dict]:
    rows = conn.execute(
        """
        SELECT oi.quantity, p.name AS product_name
        FROM order_items oi JOIN products p ON oi.product_id = p.product_id
        WHERE oi.order_id = ?
        """,
        [order_id],
    ).fetchall()
    return [{"quantity": r[0], "product_name": r[1]} for r in rows]
