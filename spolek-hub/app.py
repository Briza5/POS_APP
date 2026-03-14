"""
Spolkový Hospodský Systém – hlavní vstupní bod aplikace.

URL routing:
  ?uid=xyz            → zákaznický profil
  ?uid=xyz&table=abc  → zákaznický profil + kontext stolu
  ?table=abc          → anonymní objednávka ke stolu
  (nic)               → admin rozhraní
"""

from __future__ import annotations

import time
import traceback

import streamlit as st

# ---------------------------------------------------------------------------
# Page config – musí být první Streamlit příkaz
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Spolek Hub",
    page_icon="🍺",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Imports (po set_page_config)
# ---------------------------------------------------------------------------
from core.timezone import fmt as fmt_dt, to_prague
from core.auth import (
    get_current_org_id,
    is_admin_logged_in,
    validate_table_token,
    validate_uid_token,
    verify_admin,
)
from core.config import load_first_org_context
from core.database import DatabaseManager, is_empty, seed_demo_data
from core.gdrive_sync import get_sync_manager
from services.notification_service import get_unread_count
from services.order_status_service import get_queue_summary
from services.tab_service import list_open_tabs


# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------

def _init_session() -> None:
    """Initialise DB, sync and org context into session state (once)."""
    if "conn" in st.session_state:
        return  # already initialised

    # Sync manager (GDrive or Mock)
    sync = get_sync_manager()
    try:
        db_path, device_id = sync.initialize()
    except RuntimeError as exc:
        st.error(f"⛔ {exc}")
        st.stop()

    st.session_state["sync"] = sync
    st.session_state["db_path"] = db_path

    # Database
    db = DatabaseManager(str(db_path))
    db.initialize()
    conn = db.get_connection()
    st.session_state["conn"] = conn

    # Seed demo data on first run
    if is_empty(conn):
        seed_demo_data(conn)
        sync.mark_dirty()

    # Org context (single-tenant)
    org_ctx = load_first_org_context(conn)
    st.session_state["org_ctx"] = org_ctx
    st.session_state["org_id"] = org_ctx.org_id

    # UI state defaults
    st.session_state.setdefault("admin", None)
    st.session_state.setdefault("cart", {})
    st.session_state.setdefault("active_tab_id", None)
    st.session_state.setdefault("prev_pending_count", 0)
    st.session_state.setdefault("last_queue_poll", 0.0)
    st.session_state.setdefault("customer_last_poll", 0.0)


# ---------------------------------------------------------------------------
# Admin login widget (sidebar)
# ---------------------------------------------------------------------------

def _render_login() -> None:
    """Show login form in sidebar if no admin is logged in."""
    with st.sidebar:
        st.header("🔐 Přihlášení")
        with st.form("login_form"):
            username = st.text_input("Uživatelské jméno")
            password = st.text_input("Heslo", type="password")
            submitted = st.form_submit_button("Přihlásit se", use_container_width=True)

        if submitted:
            conn = st.session_state["conn"]
            admin = verify_admin(username, password, conn)
            if admin:
                st.session_state["admin"] = admin
                st.session_state["sync"].mark_dirty()
                st.rerun()
            else:
                st.error("Nesprávné přihlašovací údaje.")


def _render_sidebar(conn, org_ctx) -> str:
    """Render admin sidebar navigation. Returns selected page key."""
    admin = st.session_state.get("admin", {})
    sync = st.session_state.get("sync")

    with st.sidebar:
        # Sync status
        if sync:
            status = sync.status
            if status["online"]:
                indicator = "🟢" if not status["dirty"] else "🟡"
                last = status["last_sync"]
                last_str = fmt_dt(last, "%H:%M") if last else "—"
                st.caption(f"{indicator} GDrive · sync {last_str}")
            else:
                st.caption("🔴 Offline režim")

        st.divider()
        st.markdown(f"👤 **{admin.get('username', '?')}** · {admin.get('role', '')}")

        if st.button("Odhlásit se", use_container_width=True):
            st.session_state["admin"] = None
            st.rerun()

        st.divider()

        # Queue badges
        queue = get_queue_summary(conn, org_ctx.org_id)
        open_tabs = list_open_tabs(conn, org_ctx.org_id)
        pending = queue.get("pending", 0)
        open_count = len(open_tabs)

        pages = {
            "dashboard":   "🏠 Dashboard",
            "tabs":        f"📋 Účty  [{open_count}]" if open_count else "📋 Účty",
            "order_queue": f"🔔 Objednávky  [{pending}]" if pending else "🔔 Objednávky",
            "new_order":   "🛒 Nová objednávka",
            "customers":   "👥 Zákazníci",
            "products":    "📦 Produkty",
        }

        if org_ctx.modules.inventory:
            pages["inventory"] = "🗄️ Sklad"
        if org_ctx.modules.events:
            pages["events"] = "🎉 Události"

        pages["analytics"] = "📊 Analytika"

        selected = st.radio(
            "Navigace",
            list(pages.keys()),
            format_func=lambda k: pages[k],
            label_visibility="collapsed",
        )

    return selected  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Analytics page (inline)
# ---------------------------------------------------------------------------

def _render_analytics(conn, org_ctx) -> None:
    """Render basic analytics page using reports module."""
    from datetime import date, timedelta
    import pandas as pd
    from analytics.reports import (
        customer_stats,
        daily_summary,
        order_source_breakdown,
        payment_mode_breakdown,
        revenue_by_category,
        top_products,
        weekly_revenue,
    )

    st.title("📊 Analytika")

    date_to = date.today()
    date_from = date_to - timedelta(days=30)

    fc1, fc2 = st.columns(2)
    date_from = fc1.date_input("Od", value=date_from)
    date_to = fc2.date_input("Do", value=date_to)

    df_str = date_from.isoformat()
    dt_str = date_to.isoformat()

    tabs = st.tabs(["Tržby", "Produkty", "Zákazníci", "Účty", "Zdroje"])

    with tabs[0]:
        st.subheader("Týdenní tržby")
        df = weekly_revenue(conn, org_ctx.org_id)
        if not df.empty:
            st.bar_chart(df.set_index("week_start")["revenue"])
            st.dataframe(df, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádná data.")

        st.subheader("Tržby dle kategorií")
        df_cat = revenue_by_category(conn, org_ctx.org_id, df_str, dt_str)
        if not df_cat.empty:
            st.dataframe(df_cat, use_container_width=True, hide_index=True)

    with tabs[1]:
        st.subheader("Top produkty")
        df_top = top_products(conn, org_ctx.org_id, df_str, dt_str)
        if not df_top.empty:
            st.bar_chart(df_top.set_index("name")["revenue"])
            st.dataframe(df_top, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádná data.")

    with tabs[2]:
        st.subheader("Statistiky zákazníků")
        df_cust = customer_stats(conn, org_ctx.org_id)
        if not df_cust.empty:
            st.dataframe(df_cust, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádní zákazníci.")

    with tabs[3]:
        st.subheader("Způsob platby")
        df_pm = payment_mode_breakdown(conn, org_ctx.org_id, df_str, dt_str)
        if not df_pm.empty:
            st.dataframe(df_pm, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádná data.")

    with tabs[4]:
        st.subheader("Zdroje objednávek")
        df_src = order_source_breakdown(conn, org_ctx.org_id, df_str, dt_str)
        if not df_src.empty:
            st.dataframe(df_src, use_container_width=True, hide_index=True)
        else:
            st.caption("Žádná data.")


# ---------------------------------------------------------------------------
# Anonymous table QR view
# ---------------------------------------------------------------------------

def _render_anonymous_table(conn, org_ctx, table_info: dict) -> None:
    """Minimal order UI for anonymous table QR scan (no loyalty)."""
    from services.tab_service import get_tab, open_tab
    from views.customer.order_menu import render_order_menu

    table_number = table_info["table_number"]
    st.title(f"📍 Stůl {table_number}")
    st.caption(f"{org_ctx.name} · Objednávkový systém")

    # Get or create anonymous tab for this table
    tab_key = f"anon_tab_{table_info['table_id']}"
    tab_id = st.session_state.get(tab_key)

    if tab_id:
        tab = get_tab(conn, tab_id)
        if tab and tab["status"] != "open":
            tab_id = None

    if not tab_id:
        anon_tab = open_tab(
            conn, org_ctx.org_id, org_ctx.default_tab_mode,
            table_number=table_number,
            label=f"Stůl {table_number} – {__import__('datetime').datetime.now().strftime('%H:%M')}",
        )
        tab_id = anon_tab["tab_id"]
        st.session_state[tab_key] = tab_id
        st.session_state["sync"].mark_dirty()

    # Fake customer dict with no permissions (table_qr source)
    fake_customer = {
        "customer_id": None,
        "display_name": f"Stůl {table_number}",
        "points_balance": 0,
        "uid_token": None,
    }

    render_order_menu(
        conn, fake_customer, org_ctx, tab_id,
        table_number=table_number, source="table_qr",
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        _init_session()

        conn = st.session_state["conn"]
        org_ctx = st.session_state["org_ctx"]

        # ── URL routing ────────────────────────────────────────────────
        uid_token = st.query_params.get("uid")
        table_token = st.query_params.get("table")

        # PŘÍPAD A: zákaznický profil (s nebo bez stolu)
        if uid_token:
            from views.customer.profile import render_customer_profile
            render_customer_profile(
                conn, uid_token,
                table_token=table_token,
                org_ctx=org_ctx,
            )
            st.stop()

        # PŘÍPAD B: anonymní stolový QR
        if table_token:
            table_info = validate_table_token(table_token, conn)
            if table_info is None:
                st.error("Neplatný QR kód stolu.")
                st.stop()
            _render_anonymous_table(conn, org_ctx, table_info)
            st.stop()

        # PŘÍPAD C: admin UI
        if not is_admin_logged_in():
            _render_login()
            st.title("🍺 Spolek Hub")
            st.info("Přihlaste se jako administrátor v postranním panelu.")
            st.stop()

        # Admin navigation
        page = _render_sidebar(conn, org_ctx)

        # Mark dirty after every page that might write (via sync.mark_dirty
        # calls inside views) – the sync thread handles actual upload.

        if page == "dashboard":
            from views.admin.dashboard import render_dashboard
            render_dashboard(conn, org_ctx)

        elif page == "tabs":
            from views.admin.tabs import render_tabs
            render_tabs(conn, org_ctx)

        elif page == "order_queue":
            from views.admin.order_queue import render_order_queue
            render_order_queue(conn, org_ctx)

        elif page == "new_order":
            from views.admin.new_order import render_new_order
            render_new_order(conn, org_ctx)

        elif page == "customers":
            from views.admin.customers import render_customers
            render_customers(conn, org_ctx)

        elif page == "products":
            from views.admin.products import render_products
            render_products(conn, org_ctx)

        elif page == "analytics":
            _render_analytics(conn, org_ctx)

        elif page == "inventory":
            _render_inventory(conn, org_ctx)

        elif page == "events":
            st.title("🎉 Události")
            st.info("Modul událostí – připravujeme.")

    except Exception:
        st.error("Nastala neočekávaná chyba.")
        with st.expander("Technický detail"):
            st.code(traceback.format_exc())


def _render_inventory(conn, org_ctx) -> None:
    """Simple inventory overview page."""
    from analytics.reports import inventory_status

    st.title("🗄️ Sklad")
    df = inventory_status(conn, org_ctx.org_id)
    if df.empty:
        st.info("Žádné produkty se sledováním skladu.")
        return

    # Colour-code status
    def _row_style(row):
        if row["status"] == "CRITICAL":
            return ["background-color: #ffcccc"] * len(row)
        if row["status"] == "LOW":
            return ["background-color: #fff3cd"] * len(row)
        return [""] * len(row)

    st.dataframe(
        df.style.apply(_row_style, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    # Manual stock adjustment
    st.subheader("Ruční úprava stavu")
    products_rows = conn.execute(
        """
        SELECT p.product_id, p.name, i.quantity_on_hand
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        JOIN warehouses w ON i.warehouse_id = w.warehouse_id
        WHERE w.org_id = ? AND p.track_inventory = true
        ORDER BY p.name
        """,
        [org_ctx.org_id],
    ).fetchall()
    prod_opts = {r[1]: r[0] for r in products_rows}

    with st.form("inv_adjust"):
        sel = st.selectbox("Produkt", list(prod_opts.keys()))
        new_qty = st.number_input("Nové množství na skladě", min_value=0.0, step=1.0)
        submitted = st.form_submit_button("Uložit")

    if submitted:
        pid = prod_opts[sel]
        wh_row = conn.execute(
            "SELECT warehouse_id FROM warehouses WHERE org_id = ? AND is_default = true LIMIT 1",
            [org_ctx.org_id],
        ).fetchone()
        if wh_row:
            conn.execute(
                "UPDATE inventory SET quantity_on_hand = ?, last_counted_at = current_timestamp WHERE warehouse_id = ? AND product_id = ?",
                [new_qty, wh_row[0], pid],
            )
            st.session_state["sync"].mark_dirty()
            st.success("Stav skladu aktualizován.")
            st.rerun()


if __name__ == "__main__":
    main()
