"""Admin view – správa zákazníků."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import fmt as fmt_dt
from services.customer_service import (
    create_customer,
    deactivate_customer,
    get_customer_by_id,
    list_customers,
    update_customer,
)
from services.loyalty_service import adjust_points, get_transaction_history
from services.order_service import list_orders
from services.permission_service import (
    can_customer_order,
    can_customer_tab,
    get_credit_limit,
    grant_order_permission,
    grant_tab_permission,
    revoke_order_permission,
    revoke_tab_permission,
)
from services.qr_service import generate_customer_qr, qr_to_base64
from services.tab_service import get_open_tab_for_customer, list_tabs


def render_customers(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the customers management page."""
    st.title("👥 Zákazníci")
    admin = st.session_state.get("admin", {})

    list_tab, add_tab = st.tabs(["Seznam", "Přidat zákazníka"])

    with list_tab:
        _render_customer_list(conn, org_ctx, admin)

    with add_tab:
        _render_add_customer(conn, org_ctx, admin)


# ---------------------------------------------------------------------------
# Customer list + detail
# ---------------------------------------------------------------------------

def _render_customer_list(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    admin: dict,
) -> None:
    search = st.text_input("🔍 Hledat zákazníka:", placeholder="Jméno, telefon, email…")
    customers = list_customers(conn, org_ctx.org_id, search=search or None)

    if not customers:
        st.info("Žádní zákazníci.")
        return

    for c in customers:
        with st.expander(
            f"**{c['display_name']}** · {c['points_balance']} bodů · "
            f"{c['membership_type']} · {'✅' if c['is_active'] else '❌'}"
        ):
            _render_customer_detail(conn, org_ctx, admin, c)


def _render_customer_detail(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    admin: dict,
    customer: dict,
) -> None:
    cid = customer["customer_id"]

    # ── Basic info ────────────────────────────────────────────────────
    ic1, ic2 = st.columns(2)
    ic1.write(f"📧 {customer.get('email') or '—'}")
    ic1.write(f"📞 {customer.get('phone') or '—'}")
    ic2.metric("Body", customer["points_balance"])
    ic2.metric("Utraceno", f"{customer['total_spent']} {org_ctx.currency}")

    # ── Permissions ───────────────────────────────────────────────────
    st.markdown("**Oprávnění**")
    perm_c1, perm_c2 = st.columns(2)

    cur_order = can_customer_order(conn, cid, org_ctx.org_id)
    new_order = perm_c1.toggle(
        "Smí objednávat přes app", value=cur_order,
        key=f"can_order_{cid}"
    )
    if new_order != cur_order:
        if new_order:
            grant_order_permission(conn, cid, org_ctx.org_id, admin.get("admin_id", ""))
        else:
            revoke_order_permission(conn, cid, org_ctx.org_id)
        st.session_state["sync"].mark_dirty()
        st.rerun()

    cur_tab = can_customer_tab(conn, cid, org_ctx.org_id)
    new_tab = perm_c2.toggle(
        "Smí mít otevřený účet", value=cur_tab,
        key=f"can_tab_{cid}"
    )
    if new_tab != cur_tab:
        if new_tab:
            limit = Decimal(str(perm_c2.number_input(
                "Kreditní limit (Kč, 0=bez limitu):",
                min_value=0.0, value=0.0, step=100.0,
                key=f"credit_limit_{cid}"
            )))
            grant_tab_permission(conn, cid, org_ctx.org_id, limit, admin.get("admin_id", ""))
        else:
            revoke_tab_permission(conn, cid, org_ctx.org_id)
        st.session_state["sync"].mark_dirty()
        st.rerun()

    if cur_tab:
        cur_limit = get_credit_limit(conn, cid, org_ctx.org_id)
        perm_c2.caption(f"Kreditní limit: {cur_limit} {org_ctx.currency} (0=bez limitu)")

    # ── Active tab ────────────────────────────────────────────────────
    open_tab = get_open_tab_for_customer(conn, cid, org_ctx.org_id)
    if open_tab:
        st.info(
            f"📋 Otevřený účet: **{open_tab['label']}** · "
            f"{open_tab['total_amount']} {org_ctx.currency}"
        )

    # ── QR code ───────────────────────────────────────────────────────
    st.markdown("**QR kódy**")
    qr_col1, qr_col2 = st.columns(2)
    with qr_col1:
        qr_bytes = generate_customer_qr(customer["uid_token"], org_ctx.base_url)
        st.image(qr_bytes, caption="Osobní QR kód", width=150)
        st.download_button(
            "⬇ Stáhnout QR",
            data=qr_bytes,
            file_name=f"qr_{customer['uid_token'][:8]}.png",
            mime="image/png",
            key=f"dl_qr_{cid}",
        )
    with qr_col2:
        url = f"{org_ctx.base_url}/?uid={customer['uid_token']}"
        st.code(url, language=None)

    # ── History tabs ──────────────────────────────────────────────────
    hist_ord, hist_tabs, hist_pts = st.tabs(["Objednávky", "Účty", "Body"])

    with hist_ord:
        orders = list_orders(conn, org_ctx.org_id, customer_id=cid, limit=20)
        if orders:
            import pandas as pd
            df = pd.DataFrame([{
                "Čas": fmt_dt(o["created_at"], "%d.%m %H:%M"),
                f"Částka": float(o["total_amount"]),
                "Platba": o["payment_status"],
                "Stav": o["fulfillment_status"],
                "Zdroj": o["source"],
            } for o in orders])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádné objednávky.")

    with hist_tabs:
        tabs_list = list_tabs(conn, org_ctx.org_id, customer_id=cid)
        if tabs_list:
            import pandas as pd
            df = pd.DataFrame([{
                "Účet": t.label,
                "Otevřen": fmt_dt(t.opened_at, "%d.%m %H:%M"),
                "Uzavřen": fmt_dt(t.closed_at, "%d.%m %H:%M"),
                f"Celkem": float(t.total_amount),
                "Status": t.status,
            } for t in tabs_list])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádné účty.")

    with hist_pts:
        txns = get_transaction_history(conn, cid, limit=30)
        if txns:
            import pandas as pd
            df = pd.DataFrame([{
                "Čas": fmt_dt(t["created_at"], "%d.%m %H:%M"),
                "Typ": t["txn_type"],
                "Body": t["points_delta"],
                "Zůstatek": t["balance_after"],
                "Poznámka": t["note"] or "",
            } for t in txns])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádné transakce.")

        # Manual adjustment
        with st.expander("Manuální úprava bodů"):
            adj = st.number_input("Delta bodů (+/-)", step=1, key=f"adj_{cid}")
            adj_note = st.text_input("Poznámka", key=f"adj_note_{cid}")
            if st.button("Použít", key=f"adj_btn_{cid}"):
                adjust_points(conn, cid, int(adj), adj_note, admin.get("admin_id", ""))
                st.session_state["sync"].mark_dirty()
                st.rerun()


# ---------------------------------------------------------------------------
# Add customer
# ---------------------------------------------------------------------------

def _render_add_customer(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    admin: dict,
) -> None:
    st.subheader("Přidat nového zákazníka")
    with st.form("add_customer_form"):
        name = st.text_input("Jméno *", placeholder="Jan Novák")
        phone = st.text_input("Telefon", placeholder="+420 xxx xxx xxx")
        email = st.text_input("Email", placeholder="jan@example.com")
        membership = st.selectbox("Typ členství", ["host", "člen", "čestný člen"])
        notes = st.text_area("Poznámky")
        submitted = st.form_submit_button("Přidat zákazníka", type="primary")

    if submitted:
        if not name:
            st.error("Jméno je povinné.")
        else:
            c = create_customer(
                conn, org_ctx.org_id, name, phone, email, membership, notes
            )
            st.success(f"✅ Zákazník **{c['display_name']}** přidán.")
            st.code(f"{org_ctx.base_url}/?uid={c['uid_token']}")
            st.session_state["sync"].mark_dirty()
