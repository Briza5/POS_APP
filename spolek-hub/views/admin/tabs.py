"""Admin view – správa účtů (tabs)."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import fmt as fmt_dt
from services import tab_service
from services.customer_service import list_customers
from services.permission_service import can_customer_tab


def render_tabs(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the tabs management page."""
    st.title("📋 Správa účtů")

    tab_open, tab_new, tab_history = st.tabs([
        "Otevřené účty", "Nový účet", "Historie"
    ])

    # ──────────────────────────────────────────────────────────────────
    with tab_open:
        _render_open_tabs(conn, org_ctx)

    # ──────────────────────────────────────────────────────────────────
    with tab_new:
        _render_new_tab(conn, org_ctx)

    # ──────────────────────────────────────────────────────────────────
    with tab_history:
        _render_history(conn, org_ctx)


# ---------------------------------------------------------------------------
# Open tabs
# ---------------------------------------------------------------------------

def _render_open_tabs(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
) -> None:
    open_tabs = tab_service.list_open_tabs(conn, org_ctx.org_id)
    admin = st.session_state.get("admin", {})
    admin_role = admin.get("role", "bartender")

    if not open_tabs:
        st.info("Žádné otevřené účty.")
        return

    # Search / filter
    scol1, scol2, scol3 = st.columns([3, 1, 1])
    query = scol1.text_input("🔍 Hledat", placeholder="Zákazník, popis účtu nebo stůl…", label_visibility="collapsed", key="tab_search")
    mode_filter = scol2.selectbox("Režim", ["vše", "immediate", "tab"], key="tab_mode_filter")
    st.caption(f"Celkem otevřených: **{len(open_tabs)}**")

    if query:
        q = query.lower()
        open_tabs = [
            s for s in open_tabs
            if q in (s.label or "").lower()
            or q in (s.customer_name or "").lower()
            or q in (s.table_number or "").lower()
        ]
    if mode_filter != "vše":
        open_tabs = [s for s in open_tabs if s.payment_mode == mode_filter]

    if not open_tabs:
        st.info("Žádné účty neodpovídají filtru.")
        return

    for summary in open_tabs:
        mode_badge = "💳 Okamžitá" if summary.payment_mode == "immediate" else "📋 Na účet"
        customer_label = summary.customer_name or "Anonymní"
        table_label = summary.table_number or "—"

        with st.container(border=True):
            # Header
            hcol1, hcol2 = st.columns([3, 1])
            with hcol1:
                st.markdown(f"**{summary.label}** | {customer_label} | Stůl: {table_label}")
                st.caption(f"{mode_badge} · Otevřeno: {fmt_dt(summary.opened_at, '%H:%M')}")
            with hcol2:
                st.metric("Celkem", f"{summary.total_amount} {org_ctx.currency}")

            # Body
            bcol1, bcol2, bcol3 = st.columns(3)
            bcol1.metric("Nezaplaceno", f"{summary.unpaid_amount} {org_ctx.currency}")
            bcol2.metric("Objednávky", summary.orders_count)
            bcol3.metric("Nezaplacených", summary.unpaid_orders)

            # Orders expander
            with st.expander("Zobrazit položky"):
                orders = tab_service.get_tab_orders(conn, summary.tab_id)
                for i, order in enumerate(orders, 1):
                    pay_badge = (
                        "✅ Zaplaceno" if order["payment_status"] == "paid"
                        else ("🚫 Storno" if order["payment_status"] == "voided" else "⏳ Čeká")
                    )
                    ocol1, ocol2, ocol3, ocol4 = st.columns([1, 2, 2, 2])
                    ocol1.write(f"#{i}")
                    ocol2.write(fmt_dt(order["created_at"], "%H:%M"))
                    ocol3.write(order["source"])
                    ocol4.write(f"{order['total_amount']} {org_ctx.currency} · {pay_badge}")

                    for item in order.get("items", []):
                        st.caption(f"  └ {item['product_name']} × {item['quantity']} @ {item['unit_price']} {org_ctx.currency}")

                    # Immediate mode: pay single order button
                    if (
                        order["payment_status"] == "unpaid"
                        and summary.payment_mode == "immediate"
                    ):
                        btn_key = f"pay_order_{order['order_id']}"
                        if st.button(f"💰 Zaplatit tuto objednávku", key=btn_key):
                            st.session_state[f"paying_order_{order['order_id']}"] = True

                        if st.session_state.get(f"paying_order_{order['order_id']}"):
                            _payment_panel_single(conn, org_ctx, order)

            # Tab-level actions
            acol1, acol2, acol3 = st.columns(3)

            # Close tab (mode='tab' only)
            if summary.payment_mode == "tab":
                with acol1:
                    if st.button("💰 Uzavřít a zaplatit", key=f"close_{summary.tab_id}", use_container_width=True):
                        st.session_state[f"closing_tab_{summary.tab_id}"] = True

                if st.session_state.get(f"closing_tab_{summary.tab_id}"):
                    _close_tab_panel(conn, org_ctx, summary)

            # Void tab
            if admin_role in ("owner", "manager"):
                with acol2:
                    if st.button("🚫 Stornovat", key=f"void_{summary.tab_id}", use_container_width=True):
                        st.session_state[f"voiding_tab_{summary.tab_id}"] = True

                if st.session_state.get(f"voiding_tab_{summary.tab_id}"):
                    reason = st.text_input("Důvod storna:", key=f"void_reason_{summary.tab_id}")
                    if st.button("Potvrdit storno", key=f"void_confirm_{summary.tab_id}", type="primary"):
                        tab_service.void_tab(conn, summary.tab_id, reason, admin_role)
                        st.session_state[f"voiding_tab_{summary.tab_id}"] = False
                        st.session_state["sync"].mark_dirty()
                        st.rerun()


def _payment_panel_single(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    order: dict,
) -> None:
    """Inline payment panel for a single order."""
    amount_due = Decimal(str(order["total_amount"]))
    st.markdown(f"**Celkem k zaplacení: {amount_due} {org_ctx.currency}**")
    tendered = st.number_input(
        "Přijatá hotovost:", min_value=0.0, value=float(amount_due),
        step=10.0, key=f"tendered_order_{order['order_id']}"
    )
    tendered_dec = Decimal(str(tendered))
    info = tab_service.calculate_change(amount_due, tendered_dec)
    change_color = "green" if info["is_sufficient"] else "red"
    change_text = f"Vrátit: {info['amount_change']} {org_ctx.currency}"
    st.markdown(f":{change_color}[**{change_text}**]")

    if st.button("✅ Potvrdit platbu", key=f"confirm_order_{order['order_id']}",
                 disabled=not info["is_sufficient"], use_container_width=True):
        result = tab_service.pay_single_order(conn, order["order_id"], tendered_dec)
        st.success(f"Zaplaceno! Vráceno: {result['change']} {org_ctx.currency}")
        st.session_state[f"paying_order_{order['order_id']}"] = False
        st.session_state["sync"].mark_dirty()
        st.rerun()


def _close_tab_panel(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    summary: tab_service.TabSummary,
) -> None:
    """Panel for closing a tab-mode tab."""
    admin = st.session_state.get("admin", {})
    st.markdown(f"**Celkem k zaplacení: {summary.unpaid_amount} {org_ctx.currency}**")
    tendered = st.number_input(
        "Přijatá hotovost:", min_value=0.0, value=float(summary.unpaid_amount),
        step=10.0, key=f"tendered_tab_{summary.tab_id}"
    )
    tendered_dec = Decimal(str(tendered))
    info = tab_service.calculate_change(summary.unpaid_amount, tendered_dec)
    change_color = "green" if info["is_sufficient"] else "red"
    st.markdown(f":{change_color}[**Vrátit: {info['amount_change']} {org_ctx.currency}**]")

    if st.button(
        "✅ Potvrdit uzavření", key=f"close_confirm_{summary.tab_id}",
        disabled=not info["is_sufficient"], use_container_width=True, type="primary"
    ):
        result = tab_service.close_tab(
            conn, summary.tab_id, tendered_dec,
            closed_by=admin.get("username"),
        )
        st.success(
            f"Účet uzavřen! Vráceno: {result['amount_change']} {org_ctx.currency} · "
            f"Body: {result['points_earned']}"
        )
        st.session_state[f"closing_tab_{summary.tab_id}"] = False
        st.session_state["sync"].mark_dirty()
        st.rerun()


# ---------------------------------------------------------------------------
# New tab form
# ---------------------------------------------------------------------------

def _render_new_tab(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
) -> None:
    st.subheader("Otevřít nový účet")
    admin = st.session_state.get("admin", {})

    customers = list_customers(conn, org_ctx.org_id)
    customer_options = {"— Anonymní —": None}
    for c in customers:
        customer_options[c["display_name"]] = c["customer_id"]

    tables_rows = conn.execute(
        "SELECT table_number FROM tables WHERE org_id = ? AND is_active = true ORDER BY table_number",
        [org_ctx.org_id],
    ).fetchall()
    table_options = ["— Bez stolu —"] + [r[0] for r in tables_rows]

    with st.form("new_tab_form"):
        sel_customer = st.selectbox("Zákazník", list(customer_options.keys()))
        sel_table = st.selectbox("Stůl", table_options)
        label_input = st.text_input("Popis účtu (volitelné, auto-generuj pokud prázdné)")

        chosen_customer_id = customer_options[sel_customer]
        chosen_table = None if sel_table.startswith("—") else sel_table

        # Payment mode
        default_mode_idx = 0 if org_ctx.default_tab_mode == "immediate" else 1
        has_tab_perm = (
            can_customer_tab(conn, chosen_customer_id, org_ctx.org_id)
            if chosen_customer_id else False
        )
        mode_options = ["💳 Okamžitá platba per objednávka", "📋 Platba při uzavření účtu"]
        mode_disabled = (not has_tab_perm and chosen_customer_id is not None)
        payment_mode_label = st.radio(
            "Způsob platby",
            mode_options,
            index=default_mode_idx,
            disabled=mode_disabled,
        )
        if mode_disabled:
            st.caption("⚠️ Zákazník nemá oprávnění can_tab – nelze použít režim 'Na účet'.")

        submitted = st.form_submit_button("Otevřít účet", use_container_width=True, type="primary")

    if submitted:
        payment_mode = "immediate" if "Okamžitá" in payment_mode_label else "tab"
        try:
            new_tab = tab_service.open_tab(
                conn, org_ctx.org_id,
                payment_mode=payment_mode,
                customer_id=chosen_customer_id,
                table_number=chosen_table,
                label=label_input or None,
                opened_by=admin.get("username"),
            )
            st.success(f"✅ Účet **{new_tab['label']}** otevřen.")
            st.session_state["sync"].mark_dirty()
            st.rerun()
        except ValueError as e:
            st.error(str(e))


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

def _render_history(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
) -> None:
    st.subheader("Historie účtů")

    fcol1, fcol2, fcol3 = st.columns(3)
    with fcol1:
        date_from = st.date_input("Od", value=None, key="hist_from")
    with fcol2:
        date_to = st.date_input("Do", value=None, key="hist_to")
    with fcol3:
        status_filter = st.selectbox("Status", ["vše", "closed", "void"], key="hist_status")

    status_val = None if status_filter == "vše" else status_filter
    summaries = tab_service.list_tabs(
        conn, org_ctx.org_id,
        date_from=date_from.isoformat() if date_from else None,
        date_to=date_to.isoformat() if date_to else None,
        status=status_val,
    )
    closed = [s for s in summaries if s.status != "open"]

    if not closed:
        st.info("Žádné uzavřené účty v daném období.")
        return

    import pandas as pd
    rows = []
    for s in closed:
        duration = None
        if s.closed_at and s.opened_at:
            duration = round((s.closed_at - s.opened_at).total_seconds() / 60)
        rows.append({
            "Účet": s.label,
            "Zákazník": s.customer_name or "Anonymní",
            "Otevřen": fmt_dt(s.opened_at, "%d.%m %H:%M"),
            "Uzavřen": fmt_dt(s.closed_at, "%d.%m %H:%M"),
            f"Celkem ({org_ctx.currency})": float(s.total_amount),
            "Objednávek": s.orders_count,
            "Status": s.status,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
