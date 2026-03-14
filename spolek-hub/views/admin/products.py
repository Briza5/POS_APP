"""Admin view – správa produktů."""

from __future__ import annotations

import uuid

import duckdb
import streamlit as st

from core.config import OrgContext


def render_products(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the product management page."""
    st.title("📦 Produkty")

    list_tab, add_tab = st.tabs(["Seznam produktů", "Přidat produkt"])

    with list_tab:
        _render_product_list(conn, org_ctx)

    with add_tab:
        _render_add_product(conn, org_ctx)


# ---------------------------------------------------------------------------
# Product list
# ---------------------------------------------------------------------------

def _render_product_list(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
) -> None:
    rows = conn.execute(
        """
        SELECT product_id, name, category, price, points_value,
               unit, is_active, visible_to_customer, track_inventory, sort_order
        FROM products
        WHERE org_id = ?
        ORDER BY category, sort_order, name
        """,
        [org_ctx.org_id],
    ).fetchall()
    cols = [
        "product_id", "name", "category", "price", "points_value",
        "unit", "is_active", "visible_to_customer", "track_inventory", "sort_order",
    ]
    products = [dict(zip(cols, r)) for r in rows]

    if not products:
        st.info("Žádné produkty. Přidejte první produkt.")
        return

    # Group by category
    categories: dict[str, list[dict]] = {}
    for p in products:
        categories.setdefault(p["category"] or "Ostatní", []).append(p)

    for cat, items in categories.items():
        st.subheader(cat)
        for p in items:
            with st.expander(
                f"{'✅' if p['is_active'] else '❌'} **{p['name']}** "
                f"– {p['price']} {org_ctx.currency} / {p['unit']}"
            ):
                _render_product_edit(conn, org_ctx, p)


def _render_product_edit(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
    product: dict,
) -> None:
    pid = product["product_id"]
    with st.form(f"edit_product_{pid}"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Název", value=product["name"])
        category = c2.text_input("Kategorie", value=product["category"] or "")
        price = c1.number_input(
            f"Cena ({org_ctx.currency})", value=float(product["price"]), min_value=0.0, step=1.0
        )
        points = c2.number_input("Body za nákup", value=int(product["points_value"]), min_value=0)
        unit = c1.text_input("Jednotka", value=product["unit"] or "ks")
        sort_order = c2.number_input("Pořadí", value=int(product["sort_order"] or 0), min_value=0)

        bc1, bc2, bc3 = st.columns(3)
        active = bc1.checkbox("Aktivní", value=bool(product["is_active"]))
        visible = bc2.checkbox("Viditelný zákazníkům", value=bool(product["visible_to_customer"]))
        track_inv = bc3.checkbox("Sledovat sklad", value=bool(product["track_inventory"]))

        save = st.form_submit_button("💾 Uložit", use_container_width=True)

    if save:
        conn.execute(
            """
            UPDATE products
            SET name = ?, category = ?, price = ?, points_value = ?,
                unit = ?, sort_order = ?, is_active = ?,
                visible_to_customer = ?, track_inventory = ?
            WHERE product_id = ?
            """,
            [name, category or None, price, points, unit, sort_order,
             active, visible, track_inv, pid],
        )
        st.session_state["sync"].mark_dirty()
        st.success("Uloženo.")
        st.rerun()

    # Inventory info
    if org_ctx.modules.inventory:
        inv_row = conn.execute(
            """
            SELECT i.quantity_on_hand, i.unit, w.name
            FROM inventory i JOIN warehouses w ON i.warehouse_id = w.warehouse_id
            WHERE i.product_id = ?
            """,
            [pid],
        ).fetchone()
        if inv_row:
            qty, unit_inv, wh_name = inv_row
            st.caption(f"📦 Sklad '{wh_name}': {qty} {unit_inv}")
            if product.get("low_stock_alert") and qty <= product["low_stock_alert"]:
                st.warning("⚠️ Nízký stav skladu!")


# ---------------------------------------------------------------------------
# Add product
# ---------------------------------------------------------------------------

def _render_add_product(
    conn: duckdb.DuckDBPyConnection,
    org_ctx: OrgContext,
) -> None:
    st.subheader("Přidat nový produkt")
    with st.form("add_product_form"):
        c1, c2 = st.columns(2)
        name = c1.text_input("Název *", placeholder="Pivo 10°")
        category = c2.text_input("Kategorie", placeholder="Pivo")
        price = c1.number_input(f"Cena ({org_ctx.currency}) *", min_value=0.0, step=1.0)
        points = c2.number_input("Body za nákup", min_value=0, value=0)
        unit = c1.text_input("Jednotka", value="ks")
        sku = c2.text_input("SKU (volitelné)")

        bc1, bc2 = st.columns(2)
        visible = bc1.checkbox("Viditelný zákazníkům", value=True)
        track = bc2.checkbox("Sledovat sklad", value=False)

        submitted = st.form_submit_button("➕ Přidat", type="primary")

    if submitted:
        if not name or price <= 0:
            st.error("Vyplňte alespoň název a cenu.")
        else:
            pid = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO products
                    (product_id, org_id, name, category, price, points_value,
                     unit, sku, visible_to_customer, track_inventory, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, true)
                """,
                [pid, org_ctx.org_id, name, category or None, price, points,
                 unit, sku or None, visible, track],
            )
            st.success(f"✅ Produkt **{name}** přidán.")
            st.session_state["sync"].mark_dirty()
            st.rerun()
