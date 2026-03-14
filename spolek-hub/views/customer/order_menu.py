"""Customer view – objednávkové menu."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import streamlit as st

from core.config import OrgContext
from services.loyalty_service import calculate_points_earned
from services.order_service import OrderItem, create_customer_order
from services.tab_service import get_tab


def render_order_menu(
    conn: duckdb.DuckDBPyConnection,
    customer: dict,
    org_ctx: OrgContext,
    tab_id: str,
    table_number: str | None = None,
    source: str = "customer",
) -> None:
    """Render the mobile-friendly order menu for a customer.

    Args:
        tab_id: The tab this order will be added to.
        table_number: Optional table context.
        source: 'customer' or 'table_qr'.
    """
    tab = get_tab(conn, tab_id)
    tab_label = tab["label"] if tab else "—"

    st.markdown(f"### 🛒 Nová objednávka")
    st.caption(f"Přidáváš k účtu: **{tab_label}**")
    if table_number:
        st.caption(f"📍 Stůl {table_number}")

    # ── Products ──────────────────────────────────────────────────────
    products_rows = conn.execute(
        """
        SELECT product_id, name, category, price, points_value, unit
        FROM products
        WHERE org_id = ? AND is_active = true AND visible_to_customer = true
        ORDER BY category, sort_order, name
        """,
        [org_ctx.org_id],
    ).fetchall()
    p_cols = ["product_id", "name", "category", "price", "points_value", "unit"]
    products = [dict(zip(p_cols, r)) for r in products_rows]

    if not products:
        st.info("Žádné produkty k dispozici.")
        return

    categories: dict[str, list[dict]] = {}
    for p in products:
        categories.setdefault(p["category"] or "Ostatní", []).append(p)

    cart: dict[str, int] = st.session_state.get("cart", {})

    cat_tabs = st.tabs(list(categories.keys()))
    for cat_tab, (cat_name, cat_products) in zip(cat_tabs, categories.items()):
        with cat_tab:
            for p in cat_products:
                pid = p["product_id"]
                qty = cart.get(pid, 0)
                c1, c2, c3 = st.columns([4, 1, 1])
                c1.markdown(f"**{p['name']}**  \n{p['price']} {org_ctx.currency}")
                if c3.button("➕", key=f"plus_{pid}", use_container_width=True):
                    cart[pid] = qty + 1
                    st.session_state["cart"] = cart
                    st.rerun()
                if c2.button(
                    f"**{qty}**" if qty else "0",
                    key=f"qty_{pid}",
                    use_container_width=True,
                    disabled=qty == 0,
                ):
                    cart[pid] = max(0, qty - 1)
                    st.session_state["cart"] = cart
                    st.rerun()

    # ── Cart summary ──────────────────────────────────────────────────
    cart = {pid: qty for pid, qty in cart.items() if qty > 0}
    if not cart:
        st.info("Košík je prázdný.")
        return

    st.divider()
    st.subheader("Košík")

    pid_map = {p["product_id"]: p for p in products}
    items: list[OrderItem] = []
    total = Decimal("0")
    total_pts = 0

    for pid, qty in cart.items():
        if pid not in pid_map:
            continue
        p = pid_map[pid]
        unit_price = Decimal(str(p["price"]))
        line_total = unit_price * qty
        total += line_total
        pts = calculate_points_earned(line_total, org_ctx.points_per_czk)
        total_pts += pts
        items.append(OrderItem(product_id=pid, quantity=qty, unit_price=unit_price))
        st.write(f"• {p['name']} × {qty} = **{line_total} {org_ctx.currency}**")

    st.markdown(f"**Celkem: {total} {org_ctx.currency}**")
    st.caption(f"🏆 Získáte přibližně {total_pts} bodů")

    # Points redemption
    balance = customer["points_balance"]
    points_redeemed = 0
    if balance > 0:
        use_points = st.checkbox(f"Využít body ({balance} k dispozici)")
        if use_points:
            points_redeemed = st.slider(
                "Kolik bodů uplatnit?", 0, balance, 0, step=10
            )
            st.caption(f"Sleva: {points_redeemed} bodů")

    note = st.text_area("Poznámka k objednávce:", max_chars=200)

    st.divider()
    if st.button("🛒 Odeslat objednávku", use_container_width=True, type="primary"):
        st.session_state["confirm_order"] = True

    if st.session_state.get("confirm_order"):
        st.warning("Potvrdit objednávku?")
        cc1, cc2 = st.columns(2)
        if cc1.button("✅ Ano, odeslat", use_container_width=True, type="primary"):
            try:
                create_customer_order(
                    conn, org_ctx.org_id, items,
                    customer_id=customer["customer_id"],
                    tab_id=tab_id,
                    source=source,
                    table_number=table_number,
                    points_redeemed=points_redeemed,
                    note=note or None,
                )
                st.session_state["cart"] = {}
                st.session_state["confirm_order"] = False
                if "sync" in st.session_state:
                    st.session_state["sync"].mark_dirty()
                st.success("✅ Objednávka odeslána! Obsluha ji brzy potvrdí.")
                st.balloons()
                st.rerun()
            except (PermissionError, ValueError) as e:
                st.error(str(e))
                st.session_state["confirm_order"] = False
        if cc2.button("❌ Zpět", use_container_width=True):
            st.session_state["confirm_order"] = False
            st.rerun()
