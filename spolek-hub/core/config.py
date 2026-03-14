"""
Organisation context and configuration dataclasses.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import duckdb


@dataclass
class ModuleConfig:
    """Flags for optional modules per organisation."""

    loyalty: bool = True
    inventory: bool = False
    events: bool = False

    @classmethod
    def from_json(cls, raw: str | dict | None) -> "ModuleConfig":
        """Parse from a JSON string or dict (as stored in DuckDB JSON column)."""
        if raw is None:
            return cls()
        if isinstance(raw, str):
            data: dict[str, Any] = json.loads(raw)
        else:
            data = dict(raw)
        return cls(
            loyalty=bool(data.get("loyalty", True)),
            inventory=bool(data.get("inventory", False)),
            events=bool(data.get("events", False)),
        )


@dataclass
class OrgContext:
    """Runtime context for a single organisation.

    Loaded once at startup and stored in ``st.session_state["org_ctx"]``.
    """

    org_id: str
    name: str
    slug: str
    currency: str
    points_per_czk: float
    default_tab_mode: str          # 'immediate' or 'tab'
    modules: ModuleConfig = field(default_factory=ModuleConfig)
    base_url: str = "http://localhost:8501"

    @classmethod
    def from_db_row(cls, row: tuple) -> "OrgContext":
        """Build OrgContext from a raw DB row.

        Expected column order (from ``load_org_context`` query)::

            org_id, name, slug, currency, points_per_czk,
            default_tab_mode, modules_enabled
        """
        org_id, name, slug, currency, points_per_czk, default_tab_mode, modules_json = row
        base_url = (
            os.getenv("BASE_URL")
            or _try_streamlit_secret("BASE_URL")
            or "http://localhost:8501"
        )
        return cls(
            org_id=str(org_id),
            name=str(name),
            slug=str(slug),
            currency=str(currency),
            points_per_czk=float(points_per_czk),
            default_tab_mode=str(default_tab_mode),
            modules=ModuleConfig.from_json(modules_json),
            base_url=base_url,
        )


def load_org_context(
    org_id: str,
    conn: duckdb.DuckDBPyConnection,
) -> OrgContext:
    """Fetch organisation from DB and return an OrgContext.

    Raises:
        ValueError: if *org_id* is not found or organisation is inactive.
    """
    row = conn.execute(
        """
        SELECT org_id, name, slug, currency, points_per_czk,
               default_tab_mode, modules_enabled
        FROM organizations
        WHERE org_id = ? AND is_active = true
        """,
        [org_id],
    ).fetchone()

    if row is None:
        raise ValueError(f"Organizace {org_id!r} nebyla nalezena.")

    return OrgContext.from_db_row(row)


def load_first_org_context(conn: duckdb.DuckDBPyConnection) -> OrgContext:
    """Return OrgContext for the first active organisation (demo / single-tenant use)."""
    row = conn.execute(
        """
        SELECT org_id, name, slug, currency, points_per_czk,
               default_tab_mode, modules_enabled
        FROM organizations
        WHERE is_active = true
        ORDER BY created_at
        LIMIT 1
        """,
    ).fetchone()

    if row is None:
        raise ValueError("Žádná aktivní organizace nenalezena. Spusťte seed_demo_data().")

    return OrgContext.from_db_row(row)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _try_streamlit_secret(key: str) -> str | None:
    """Safely read a Streamlit secret without crashing if not in Streamlit."""
    try:
        import streamlit as st
        return st.secrets.get(key)
    except Exception:
        return None
