"""Admin dashboard view."""

from __future__ import annotations

from datetime import date

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import prague_offset_hours
from services.order_status_service import get_queue_summary
from services.tab_service import list_open_tabs


def render_dashboard(conn: duckdb.DuckDBPyConnection, org_ctx: OrgContext) -> None:
    """Render the admin dashboard with KPIs, alerts and charts."""
    st.title("🏠 Dashboard")

    today = date.today().isoformat()

    # ------------------------------------------------------------------
    # KPIs
    # ------------------------------------------------------------------
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(total_amount), 0)          AS revenue,
            COUNT(*)                                  AS orders,
            COALESCE(AVG(total_amount), 0)           AS avg_order,
            COUNT(DISTINCT customer_id)              AS customers
        FROM orders
        WHERE org_id = ?
          AND DATE(created_at) = ?
          AND payment_status = 'paid'
        """,
        [org_ctx.org_id, today],
    ).fetchone()

    revenue, orders, avg_order, customers = (row or (0, 0, 0, 0))

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("💰 Dnešní tržby", f"{revenue:,.0f} {org_ctx.currency}")
    col2.metric("🛒 Objednávky", int(orders))
    col3.metric("📊 Průměr / obj.", f"{avg_order:,.0f} {org_ctx.currency}")
    col4.metric("👥 Zákazníci", int(customers))

    st.divider()

    # ------------------------------------------------------------------
    # Alerts
    # ------------------------------------------------------------------
    queue = get_queue_summary(conn, org_ctx.org_id)
    open_tabs = list_open_tabs(conn, org_ctx.org_id)

    if queue["pending"] > 0:
        st.warning(
            f"🔔 **{queue['pending']} čekající objednávk{'a' if queue['pending'] == 1 else 'y'}** "
            "– přejděte na Objednávky"
        )
    if open_tabs:
        st.info(f"📋 Otevřených účtů: **{len(open_tabs)}**")

    # ------------------------------------------------------------------
    # Charts
    # ------------------------------------------------------------------
    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📈 Tržby – posledních 7 dní")
        rows_7d = conn.execute(
            """
            SELECT DATE(created_at) AS day,
                   SUM(total_amount) AS revenue
            FROM orders
            WHERE org_id = ?
              AND payment_status = 'paid'
              AND DATE(created_at) >= current_date - INTERVAL '6 days'
            GROUP BY day
            ORDER BY day
            """,
            [org_ctx.org_id],
        ).fetchall()
        if rows_7d:
            import pandas as pd
            df = pd.DataFrame(rows_7d, columns=["Den", "Tržby"])
            df["Den"] = pd.to_datetime(df["Den"])
            st.bar_chart(df.set_index("Den")["Tržby"])
        else:
            st.caption("Žádná data za posledních 7 dní.")

    with col_right:
        st.subheader("🏆 Top 5 produktů")
        top_rows = conn.execute(
            """
            SELECT p.name, SUM(oi.quantity) AS qty, SUM(oi.quantity * oi.unit_price) AS rev
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN orders o   ON oi.order_id = o.order_id
            WHERE o.org_id = ?
              AND o.payment_status = 'paid'
              AND DATE(o.created_at) >= current_date - INTERVAL '30 days'
            GROUP BY p.name
            ORDER BY rev DESC
            LIMIT 5
            """,
            [org_ctx.org_id],
        ).fetchall()
        if top_rows:
            import pandas as pd
            df_top = pd.DataFrame(top_rows, columns=["Produkt", "Ks", f"Tržby ({org_ctx.currency})"])
            st.dataframe(df_top, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádná data.")

    # ------------------------------------------------------------------
    # Last 10 orders
    # ------------------------------------------------------------------
    st.subheader("🕐 Posledních 10 objednávek")
    last_rows = conn.execute(
        """
        SELECT o.created_at, c.display_name, o.source,
               o.total_amount, o.payment_status, o.fulfillment_status,
               t.label AS tab_label
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN tabs t      ON o.tab_id = t.tab_id
        WHERE o.org_id = ?
        ORDER BY o.created_at DESC
        LIMIT 10
        """,
        [org_ctx.org_id],
    ).fetchall()
    if last_rows:
        import pandas as pd
        df_last = pd.DataFrame(last_rows, columns=[
            "Čas", "Zákazník", "Zdroj",
            f"Částka ({org_ctx.currency})", "Platba", "Příprava", "Účet",
        ])
        df_last["Zákazník"] = df_last["Zákazník"].fillna("Anonymní")
        df_last["Čas"] = (
            pd.to_datetime(df_last["Čas"])
            .dt.tz_localize("UTC")
            .dt.tz_convert("Europe/Prague")
            .dt.strftime("%H:%M")
        )
        st.dataframe(df_last, use_container_width=True, hide_index=True)
    else:
        st.caption("Žádné objednávky.")
