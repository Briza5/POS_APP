"""Customer view – odměny a věrnostní program."""

from __future__ import annotations

import duckdb
import streamlit as st

from core.config import OrgContext
from core.timezone import fmt as fmt_dt
from services.loyalty_service import get_available_rewards, get_transaction_history


def render_rewards(
    conn: duckdb.DuckDBPyConnection,
    customer: dict,
    org_ctx: OrgContext,
) -> None:
    """Render the rewards catalogue and loyalty transaction history."""
    st.subheader("🎁 Odměny")

    balance = customer["points_balance"]
    st.metric("Vaše body", balance)

    rewards = get_available_rewards(conn, org_ctx.org_id, balance)

    if not rewards:
        st.info("Žádné dostupné odměny pro váš aktuální počet bodů.")
    else:
        for r in rewards:
            with st.container(border=True):
                rc1, rc2 = st.columns([3, 1])
                rc1.markdown(f"**{r['name']}**")
                if r.get("description"):
                    rc1.caption(r["description"])
                if r.get("valid_until"):
                    rc1.caption(f"Platí do: {r['valid_until']}")
                rc2.metric("Cena", f"{r['cost_points']} b")

    st.divider()
    st.subheader("📋 Historie transakcí")
    txns = get_transaction_history(conn, customer["customer_id"], limit=30)
    if not txns:
        st.caption("Žádné transakce.")
        return

    for t in txns:
        delta = t["points_delta"]
        color = "green" if delta > 0 else "red"
        sign = "+" if delta > 0 else ""
        created = t["created_at"]
        time_str = fmt_dt(created, "%d.%m %H:%M")
        st.markdown(
            f":{color}[**{sign}{delta} b**] &nbsp; {time_str} &nbsp; "
            f"_{t.get('note') or t['txn_type']}_ &nbsp; → {t['balance_after']} b"
        )
