"""Admin view – nová objednávka."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import duckdb
import streamlit as st

from core.config import OrgContext
from services import tab_service
from services.customer_service import list_customers
from services.order_service import OrderItem, create_pos_order
from services.tab_service import calculate_change, open_tab, pay_single_order


def render_new_order(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the new order wizard."""
    st.title("🛒 Nová objednávka")
    admin = st.session_state.get("admin", {})

    # ── Step 1: Tab selection / creation ──────────────────────────────
    st.subheader("1️⃣ Výběr účtu")
    tab_mode = st.radio(
        "Kam zařadit objednávku?",
        [
            "Přidat k existujícímu účtu",
            "Otevřít nový účet",
            "Jednorázová objednávka",
        ],
        horizontal=True,
        key="new_order_tab_mode",
    )

    selected_tab_id: str | None = None
    selected_customer_id: str | None = None

    if tab_mode == "Přidat k existujícímu účtu":
        open_tabs = tab_service.list_open_tabs(conn, org_ctx.org_id)
        if not open_tabs:
            st.warning("Žádné otevřené účty. Vytvořte nový.")
        else:
            tab_options = {
                f"{s.label} | {s.customer_name or 'Anonymní'} | {s.total_amount} {org_ctx.currency}": s.tab_id
                for s in open_tabs
            }
            sel = st.selectbox("Vyberte účet:", list(tab_options.keys()))
            selected_tab_id = tab_options[sel]
            # Pre-fill customer from selected tab
            tab_row = tab_service.get_tab(conn, selected_tab_id)
            if tab_row:
                selected_customer_id = tab_row.get("customer_id")

    elif tab_mode == "Otevřít nový účet":
        customers = list_customers(conn, org_ctx.org_id)
        c_opts = {"— Anonymní —": None}
        for c in customers:
            c_opts[c["display_name"]] = c["customer_id"]
        sel_c = st.selectbox("Zákazník:", list(c_opts.keys()), key="no_customer")
        selected_customer_id = c_opts[sel_c]

        tables_rows = conn.execute(
            "SELECT table_number FROM tables WHERE org_id = ? AND is_active = true ORDER BY table_number",
            [org_ctx.org_id],
        ).fetchall()
        tbl_opts = ["— Bez stolu —"] + [r[0] for r in tables_rows]
        sel_t = st.selectbox("Stůl:", tbl_opts, key="no_table")
        sel_table = None if sel_t.startswith("—") else sel_t

        pm_label = st.radio(
            "Způsob platby:",
            ["💳 Okamžitá", "📋 Na účet"],
            index=0 if org_ctx.default_tab_mode == "immediate" else 1,
            key="no_pm",
        )
        pm = "immediate" if "Okamžitá" in pm_label else "tab"

        if st.button("Otevřít účet", key="no_open_tab"):
            try:
                new_tab = open_tab(
                    conn, org_ctx.org_id, pm,
                    customer_id=selected_customer_id,
                    table_number=sel_table,
                    opened_by=admin.get("username"),
                )
                st.session_state["active_tab_id"] = new_tab["tab_id"]
                st.session_state["sync"].mark_dirty()
                st.success(f"Účet **{new_tab['label']}** otevřen.")
                selected_tab_id = new_tab["tab_id"]
            except ValueError as e:
                st.error(str(e))
        else:
            selected_tab_id = st.session_state.get("active_tab_id")

    else:  # Jednorázová
        now_str = datetime.now(timezone.utc).strftime("%H:%M")
        try:
            tmp_tab = open_tab(
                conn, org_ctx.org_id, "immediate",
                label=f"Jednorázová {now_str}",
                opened_by=admin.get("username"),
            )
            selected_tab_id = tmp_tab["tab_id"]
            st.info(f"Dočasný účet: **{tmp_tab['label']}**")
        except Exception:
            selected_tab_id = st.session_state.get("_last_single_tab")
        st.session_state["_last_single_tab"] = selected_tab_id

    if not selected_tab_id:
        st.stop()

    # ── Step 2: Products ───────────────────────────────────────────────
    st.divider()
    st.subheader("2️⃣ Výběr produktů")

    products_rows = conn.execute(
        """
        SELECT product_id, name, category, price, points_value, unit
        FROM products
        WHERE org_id = ? AND is_active = true
        ORDER BY category, sort_order, name
        """,
        [org_ctx.org_id],
    ).fetchall()
    cols = ["product_id", "name", "category", "price", "points_value", "unit"]
    products = [dict(zip(cols, r)) for r in products_rows]

    # Group by category
    categories: dict[str, list[dict]] = {}
    for p in products:
        categories.setdefault(p["category"] or "Ostatní", []).append(p)

    cart: dict[str, int] = st.session_state.get("cart", {})

    if categories:
        cat_tabs = st.tabs(list(categories.keys()))
        for cat_tab, (cat_name, cat_products) in zip(cat_tabs, categories.items()):
            with cat_tab:
                for p in cat_products:
                    pc1, pc2, pc3 = st.columns([3, 1, 1])
                    pc1.write(f"**{p['name']}** – {p['price']} {org_ctx.currency}")
                    qty = cart.get(p["product_id"], 0)
                    minus_key = f"minus_{p['product_id']}"
                    plus_key = f"plus_{p['product_id']}"
                    if pc2.button("➕", key=plus_key):
                        cart[p["product_id"]] = qty + 1
                        st.session_state["cart"] = cart
                        st.rerun()
                    if pc3.button("➖", key=minus_key, disabled=qty == 0):
                        cart[p["product_id"]] = max(0, qty - 1)
                        st.session_state["cart"] = cart
                        st.rerun()
                    if qty:
                        pc1.caption(f"V košíku: {qty}")

    # ── Step 3: Summary & checkout ─────────────────────────────────────
    st.divider()
    st.subheader("3️⃣ Shrnutí")

    cart = {pid: qty for pid, qty in cart.items() if qty > 0}
    if not cart:
        st.info("Košík je prázdný.")
        st.stop()

    # Build items list
    pid_map = {p["product_id"]: p for p in products}
    items: list[OrderItem] = []
    total = Decimal("0")
    for pid, qty in cart.items():
        if pid not in pid_map:
            continue
        p = pid_map[pid]
        unit_price = Decimal(str(p["price"]))
        items.append(OrderItem(
            product_id=pid,
            quantity=qty,
            unit_price=unit_price,
        ))
        total += unit_price * qty
        st.write(f"• {p['name']} × {qty} = {unit_price * qty} {org_ctx.currency}")

    st.markdown(f"**Celkem: {total} {org_ctx.currency}**")

    note = st.text_input("Poznámka k objednávce:", key="new_order_note")
    send_to_queue = st.checkbox(
        "📋 Odeslat do fronty přípravy",
        value=False,
        help="Objednávka se zobrazí ve frontě – obsluha ji odškrtá po vydání.",
        key="new_order_queue",
    )

    if st.button("✅ Zaúčtovat", use_container_width=True, type="primary"):
        try:
            order = create_pos_order(
                conn, org_ctx.org_id, items, selected_tab_id,
                customer_id=selected_customer_id,
                created_by=admin.get("username"),
                note=note or None,
                queue=send_to_queue,
            )
            st.session_state["cart"] = {}
            st.session_state["sync"].mark_dirty()

            # Get tab to check payment_mode
            tab_row = tab_service.get_tab(conn, selected_tab_id)
            pm = tab_row["payment_mode"] if tab_row else "immediate"

            if pm == "immediate":
                # Show payment panel immediately
                st.session_state[f"paying_order_{order['order_id']}"] = True
                st.rerun()
            else:
                tab_label = tab_row["label"] if tab_row else "—"
                current_total = tab_row["total_amount"] if tab_row else total
                st.success(f"✅ Přidáno na účet **{tab_label}**")
                st.info(f"Aktuální stav účtu: {current_total} {org_ctx.currency}")

        except Exception as e:
            st.error(str(e))

    # Immediate payment panel (after booking)
    for pid, _ in list(cart.items()):
        # Check if we're in paying state for last created order
        for key in list(st.session_state.keys()):
            if key.startswith("paying_order_") and st.session_state[key]:
                order_id = key.replace("paying_order_", "")
                _immediate_payment_panel(conn, org_ctx, order_id, total)
                break


def _immediate_payment_panel(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    order_id: str,
    total: Decimal,
) -> None:
    """Inline payment panel for an immediate-mode order."""
    st.divider()
    st.subheader("💰 Platba")
    st.markdown(f"**Celkem: {total} {org_ctx.currency}**")
    tendered = st.number_input(
        "Přijatá hotovost:", min_value=0.0, value=float(total),
        step=10.0, key=f"tendered_imm_{order_id}"
    )
    tendered_dec = Decimal(str(tendered))
    info = calculate_change(total, tendered_dec)
    color = "green" if info["is_sufficient"] else "red"
    st.markdown(f":{color}[**Vrátit: {info['amount_change']} {org_ctx.currency}**]")

    if st.button(
        "✅ Potvrdit platbu", key=f"confirm_imm_{order_id}",
        disabled=not info["is_sufficient"], use_container_width=True, type="primary"
    ):
        result = pay_single_order(conn, order_id, tendered_dec)
        st.success(f"Zaplaceno! Vráceno: {result['change']} {org_ctx.currency}")
        st.session_state[f"paying_order_{order_id}"] = False
        st.session_state["sync"].mark_dirty()
        st.rerun()
