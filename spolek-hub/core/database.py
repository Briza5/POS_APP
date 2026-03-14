"""
Database manager for Spolkový Hospodský Systém.
Uses DuckDB as the storage engine.
"""

from __future__ import annotations

import uuid
import hashlib
from datetime import date, datetime, timezone
from decimal import Decimal

import duckdb


class DatabaseManager:
    """Manages DuckDB connection and schema initialization."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    def initialize(self) -> None:
        """Open connection and create all tables if they don't exist."""
        self._conn = duckdb.connect(self.db_path)
        self._create_schema()

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return the active connection, initializing if needed."""
        if self._conn is None:
            self.initialize()
        return self._conn  # type: ignore[return-value]

    def close(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _create_schema(self) -> None:
        conn = self._conn
        assert conn is not None

        conn.execute("""
            CREATE TABLE IF NOT EXISTS organizations (
                org_id          VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                name            VARCHAR(150) NOT NULL,
                type            VARCHAR(50),
                slug            VARCHAR(50) UNIQUE NOT NULL,
                contact_email   VARCHAR(150),
                points_per_czk  DECIMAL(5,2) NOT NULL DEFAULT 1.0,
                currency        VARCHAR(3) NOT NULL DEFAULT 'CZK',
                modules_enabled JSON NOT NULL DEFAULT
                    '{"loyalty":true,"inventory":false,"events":false}',
                default_tab_mode VARCHAR(20) NOT NULL DEFAULT 'immediate',
                created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
                is_active       BOOLEAN NOT NULL DEFAULT true
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                customer_id     VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                uid_token       VARCHAR(32) UNIQUE NOT NULL,
                display_name    VARCHAR(100) NOT NULL,
                phone           VARCHAR(20),
                email           VARCHAR(150),
                points_balance  INTEGER NOT NULL DEFAULT 0,
                total_spent     DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                membership_type VARCHAR(50) DEFAULT 'host',
                member_since    DATE,
                created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
                last_seen_at    TIMESTAMP,
                is_active       BOOLEAN NOT NULL DEFAULT true,
                notes           TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS customer_permissions (
                permission_id   VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                customer_id     VARCHAR NOT NULL REFERENCES customers(customer_id),
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                can_order       BOOLEAN NOT NULL DEFAULT false,
                can_tab         BOOLEAN NOT NULL DEFAULT false,
                credit_limit    DECIMAL(10,2) DEFAULT 0,
                granted_by      VARCHAR,
                granted_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
                UNIQUE(customer_id, org_id)
            )
        """)

        # events must exist before tabs (FK reference)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id        VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                name            VARCHAR(200) NOT NULL,
                event_type      VARCHAR(50),
                event_date      DATE NOT NULL,
                start_time      TIME,
                location        VARCHAR(200),
                ticket_price    DECIMAL(8,2),
                expected_guests INTEGER,
                actual_guests   INTEGER,
                warehouse_id    VARCHAR,
                status          VARCHAR(20) DEFAULT 'planned',
                notes           TEXT,
                created_by      VARCHAR(50),
                created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tabs (
                tab_id          VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                customer_id     VARCHAR REFERENCES customers(customer_id),
                event_id        VARCHAR REFERENCES events(event_id),
                table_number    VARCHAR(20),
                label           VARCHAR(100),
                payment_mode    VARCHAR(20) NOT NULL DEFAULT 'immediate',
                status          VARCHAR(20) NOT NULL DEFAULT 'open',
                total_amount    DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                total_paid      DECIMAL(10,2) NOT NULL DEFAULT 0.00,
                total_points_earned INTEGER NOT NULL DEFAULT 0,
                opened_at       TIMESTAMP NOT NULL DEFAULT current_timestamp,
                closed_at       TIMESTAMP,
                opened_by       VARCHAR(50),
                closed_by       VARCHAR(50)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tab_payments (
                payment_id      VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                tab_id          VARCHAR NOT NULL REFERENCES tabs(tab_id),
                amount_due      DECIMAL(10,2) NOT NULL,
                amount_tendered DECIMAL(10,2) NOT NULL,
                amount_change   DECIMAL(10,2) NOT NULL,
                payment_method  VARCHAR(20) DEFAULT 'cash',
                note            TEXT,
                created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
                created_by      VARCHAR(50)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS tables (
                table_id        VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                table_number    VARCHAR(20) NOT NULL,
                description     VARCHAR(100),
                qr_token        VARCHAR(32) UNIQUE NOT NULL,
                is_active       BOOLEAN NOT NULL DEFAULT true,
                UNIQUE(org_id, table_number)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id      VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id          VARCHAR NOT NULL REFERENCES organizations(org_id),
                sku             VARCHAR(50),
                name            VARCHAR(100) NOT NULL,
                category        VARCHAR(50),
                unit            VARCHAR(20) DEFAULT 'ks',
                price           DECIMAL(8,2) NOT NULL,
                cost_price      DECIMAL(8,2),
                points_value    INTEGER NOT NULL DEFAULT 0,
                is_reward       BOOLEAN NOT NULL DEFAULT false,
                reward_cost_pts INTEGER,
                track_inventory BOOLEAN NOT NULL DEFAULT false,
                low_stock_alert INTEGER,
                visible_to_customer BOOLEAN NOT NULL DEFAULT true,
                is_active       BOOLEAN NOT NULL DEFAULT true,
                sort_order      INTEGER DEFAULT 0,
                created_at      TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id            VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id              VARCHAR NOT NULL REFERENCES organizations(org_id),
                tab_id              VARCHAR REFERENCES tabs(tab_id),
                customer_id         VARCHAR REFERENCES customers(customer_id),
                event_id            VARCHAR,
                table_number        VARCHAR(20),
                source              VARCHAR(20) DEFAULT 'pos',
                created_at          TIMESTAMP NOT NULL DEFAULT current_timestamp,
                total_amount        DECIMAL(10,2) NOT NULL,
                points_earned       INTEGER NOT NULL DEFAULT 0,
                points_redeemed     INTEGER NOT NULL DEFAULT 0,
                payment_method      VARCHAR(20) DEFAULT 'cash',
                payment_status      VARCHAR(20) NOT NULL DEFAULT 'unpaid',
                fulfillment_status  VARCHAR(20) NOT NULL DEFAULT 'completed',
                status_history      JSON DEFAULT '[]',
                rejection_reason    TEXT,
                note                TEXT,
                created_by          VARCHAR(50)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                item_id       VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                order_id      VARCHAR NOT NULL REFERENCES orders(order_id),
                product_id    VARCHAR NOT NULL REFERENCES products(product_id),
                quantity      INTEGER NOT NULL DEFAULT 1,
                unit_price    DECIMAL(8,2) NOT NULL,
                points_earned INTEGER NOT NULL DEFAULT 0
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS loyalty_transactions (
                txn_id        VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                customer_id   VARCHAR NOT NULL REFERENCES customers(customer_id),
                order_id      VARCHAR REFERENCES orders(order_id),
                tab_id        VARCHAR REFERENCES tabs(tab_id),
                txn_type      VARCHAR(20) NOT NULL,
                points_delta  INTEGER NOT NULL,
                balance_after INTEGER NOT NULL,
                note          TEXT,
                created_at    TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS rewards (
                reward_id   VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id      VARCHAR NOT NULL REFERENCES organizations(org_id),
                name        VARCHAR(100) NOT NULL,
                description TEXT,
                cost_points INTEGER NOT NULL,
                product_id  VARCHAR REFERENCES products(product_id),
                is_active   BOOLEAN NOT NULL DEFAULT true,
                valid_until DATE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS admin_users (
                admin_id      VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id        VARCHAR NOT NULL REFERENCES organizations(org_id),
                username      VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(64) NOT NULL,
                role          VARCHAR(20) DEFAULT 'bartender',
                created_at    TIMESTAMP NOT NULL DEFAULT current_timestamp,
                last_login_at TIMESTAMP
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS notifications (
                notification_id   VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id            VARCHAR NOT NULL REFERENCES organizations(org_id),
                target_type       VARCHAR(20) NOT NULL,
                target_id         VARCHAR,
                target_role       VARCHAR(20),
                notification_type VARCHAR(30) NOT NULL,
                title             VARCHAR(200) NOT NULL,
                body              TEXT,
                reference_id      VARCHAR,
                reference_type    VARCHAR(30),
                is_read           BOOLEAN NOT NULL DEFAULT false,
                read_at           TIMESTAMP,
                created_at        TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS warehouses (
                warehouse_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                org_id       VARCHAR NOT NULL REFERENCES organizations(org_id),
                name         VARCHAR(100) NOT NULL,
                location     VARCHAR(200),
                is_default   BOOLEAN NOT NULL DEFAULT false,
                created_at   TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                inventory_id      VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                warehouse_id      VARCHAR NOT NULL REFERENCES warehouses(warehouse_id),
                product_id        VARCHAR NOT NULL REFERENCES products(product_id),
                quantity_on_hand  DECIMAL(10,3) NOT NULL DEFAULT 0,
                quantity_reserved DECIMAL(10,3) NOT NULL DEFAULT 0,
                unit              VARCHAR(20) DEFAULT 'ks',
                last_counted_at   TIMESTAMP,
                UNIQUE(warehouse_id, product_id)
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory_movements (
                movement_id    VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                warehouse_id   VARCHAR NOT NULL REFERENCES warehouses(warehouse_id),
                product_id     VARCHAR NOT NULL REFERENCES products(product_id),
                movement_type  VARCHAR(30) NOT NULL,
                quantity_delta DECIMAL(10,3) NOT NULL,
                quantity_after DECIMAL(10,3) NOT NULL,
                unit_cost      DECIMAL(8,2),
                reference_id   VARCHAR,
                reference_type VARCHAR(30),
                note           TEXT,
                created_by     VARCHAR(50),
                created_at     TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS event_tickets (
                ticket_id      VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::VARCHAR,
                event_id       VARCHAR NOT NULL REFERENCES events(event_id),
                customer_id    VARCHAR REFERENCES customers(customer_id),
                ticket_code    VARCHAR(32) UNIQUE NOT NULL,
                guest_name     VARCHAR(100),
                quantity       INTEGER NOT NULL DEFAULT 1,
                price_paid     DECIMAL(8,2),
                payment_method VARCHAR(20) DEFAULT 'cash',
                is_used        BOOLEAN NOT NULL DEFAULT false,
                used_at        TIMESTAMP,
                created_at     TIMESTAMP NOT NULL DEFAULT current_timestamp
            )
        """)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _hash_password(password: str) -> str:
    """Return SHA-256 hex digest of *password*."""
    return hashlib.sha256(password.encode()).hexdigest()


def _gen_uid() -> str:
    """Return a 32-char hex token."""
    return uuid.uuid4().hex


def is_empty(conn: duckdb.DuckDBPyConnection) -> bool:
    """Return True if the organizations table has no rows."""
    row = conn.execute("SELECT COUNT(*) FROM organizations").fetchone()
    return row is None or row[0] == 0


# ----------------------------------------------------------------------
# Demo data
# ----------------------------------------------------------------------

def seed_demo_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Insert demo data for *TJ Sokol Demo* organisation."""

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    today = date.today()

    # ── Organization ──────────────────────────────────────────────────
    org_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO organizations
            (org_id, name, type, slug, points_per_czk, currency,
             modules_enabled, default_tab_mode, is_active)
        VALUES (?, 'TJ Sokol Demo', 'TJ Sokol', 'demo', 1.0, 'CZK',
                '{"loyalty":true,"inventory":true,"events":true}',
                'tab', true)
        """,
        [org_id],
    )

    # ── Admin user ─────────────────────────────────────────────────────
    admin_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO admin_users (admin_id, org_id, username, password_hash, role)
        VALUES (?, ?, 'admin', ?, 'owner')
        """,
        [admin_id, org_id, _hash_password("admin123")],
    )

    # ── Products ───────────────────────────────────────────────────────
    product_specs = [
        ("Pivo 10°", "Pivo",   45.0,  45),
        ("Pivo 12°", "Pivo",   55.0,  55),
        ("Víno",     "Nápoje", 60.0,  60),
        ("Kofola",   "Nápoje", 35.0,  35),
        ("Svíčková", "Jídlo", 150.0, 150),
    ]
    product_ids: list[str] = []
    for name, cat, price, pts in product_specs:
        pid = str(uuid.uuid4())
        product_ids.append(pid)
        conn.execute(
            """
            INSERT INTO products
                (product_id, org_id, name, category, price, points_value,
                 visible_to_customer, is_active, track_inventory)
            VALUES (?, ?, ?, ?, ?, ?, true, true, true)
            """,
            [pid, org_id, name, cat, price, pts],
        )

    # ── Customers ──────────────────────────────────────────────────────
    customer_specs = [
        ("Jan Novák",   250, True,  True),
        ("Marie Nová",  80,  True,  False),
        ("Petr Volný",  0,   False, False),
    ]
    customer_ids: list[str] = []
    for name, pts, can_order, can_tab in customer_specs:
        cid = str(uuid.uuid4())
        customer_ids.append(cid)
        conn.execute(
            """
            INSERT INTO customers
                (customer_id, org_id, uid_token, display_name,
                 points_balance, membership_type, member_since, is_active)
            VALUES (?, ?, ?, ?, ?, 'člen', ?, true)
            """,
            [cid, org_id, _gen_uid(), name, pts, today],
        )
        conn.execute(
            """
            INSERT INTO customer_permissions
                (customer_id, org_id, can_order, can_tab, granted_by)
            VALUES (?, ?, ?, ?, ?)
            """,
            [cid, org_id, can_order, can_tab, admin_id],
        )

    # ── Tables ─────────────────────────────────────────────────────────
    for tnum in ["1", "2", "3", "Terasa"]:
        conn.execute(
            """
            INSERT INTO tables (org_id, table_number, qr_token, is_active)
            VALUES (?, ?, ?, true)
            """,
            [org_id, tnum, _gen_uid()],
        )

    # ── Warehouse + inventory ──────────────────────────────────────────
    wh_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO warehouses (warehouse_id, org_id, name, is_default)
        VALUES (?, ?, 'Hlavní sklad', true)
        """,
        [wh_id, org_id],
    )
    for pid, qty in zip(product_ids, [100, 80, 50, 60, 20]):
        conn.execute(
            """
            INSERT INTO inventory (warehouse_id, product_id, quantity_on_hand, unit)
            VALUES (?, ?, ?, 'ks')
            """,
            [wh_id, pid, qty],
        )

    # ── Tab A: Jan Novák – mode='tab', open, 2 unpaid orders (145 Kč) ─
    tab_a_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO tabs
            (tab_id, org_id, customer_id, payment_mode, status,
             total_amount, total_paid, label, opened_at, opened_by)
        VALUES (?, ?, ?, 'tab', 'open', 145.00, 0.00,
                'Účet #1 – demo', ?, 'admin')
        """,
        [tab_a_id, org_id, customer_ids[0], now],
    )

    # Order A1: 2× Pivo 10° (90 Kč) + 1× Kofola (35 Kč) = 125 Kč
    # Adjusted to reach 145 total: A1=100, A2=45
    order_a1_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO orders
            (order_id, org_id, tab_id, customer_id, source,
             total_amount, payment_status, fulfillment_status,
             created_at, created_by)
        VALUES (?, ?, ?, ?, 'pos', 100.00, 'unpaid', 'completed', ?, 'admin')
        """,
        [order_a1_id, org_id, tab_a_id, customer_ids[0], now],
    )
    conn.execute(
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (?, ?, 2, 45.00)
        """,
        [order_a1_id, product_ids[0]],
    )
    conn.execute(
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (?, ?, 1, 10.00)
        """,
        [order_a1_id, product_ids[3]],
    )

    order_a2_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO orders
            (order_id, org_id, tab_id, customer_id, source,
             total_amount, payment_status, fulfillment_status,
             created_at, created_by)
        VALUES (?, ?, ?, ?, 'pos', 45.00, 'unpaid', 'completed', ?, 'admin')
        """,
        [order_a2_id, org_id, tab_a_id, customer_ids[0], now],
    )
    conn.execute(
        """
        INSERT INTO order_items (order_id, product_id, quantity, unit_price)
        VALUES (?, ?, 1, 45.00)
        """,
        [order_a2_id, product_ids[0]],
    )

    # ── Tab B: Marie Nová – mode='immediate', closed, 1 paid order ─────
    tab_b_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO tabs
            (tab_id, org_id, customer_id, payment_mode, status,
             total_amount, total_paid, total_points_earned,
             label, opened_at, closed_at, opened_by, closed_by)
        VALUES (?, ?, ?, 'immediate', 'closed', 55.00, 55.00, 55,
                'Účet #2 – demo', ?, ?, 'admin', 'admin')
        """,
        [tab_b_id, org_id, customer_ids[1], now, now],
    )

    order_b1_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO orders
            (order_id, org_id, tab_id, customer_id, source,
             total_amount, points_earned, payment_status,
             fulfillment_status, created_at, created_by)
        VALUES (?, ?, ?, ?, 'pos', 55.00, 55, 'paid', 'completed', ?, 'admin')
        """,
        [order_b1_id, org_id, tab_b_id, customer_ids[1], now],
    )
    conn.execute(
        """
        INSERT INTO order_items
            (order_id, product_id, quantity, unit_price, points_earned)
        VALUES (?, ?, 1, 55.00, 55)
        """,
        [order_b1_id, product_ids[1]],
    )

    # Payment record for Tab B
    conn.execute(
        """
        INSERT INTO tab_payments
            (tab_id, amount_due, amount_tendered, amount_change,
             payment_method, created_by)
        VALUES (?, 55.00, 60.00, 5.00, 'cash', 'admin')
        """,
        [tab_b_id],
    )

    # Loyalty transaction for Marie (80 existing + 55 earned = 135)
    new_balance = 80 + 55
    conn.execute(
        """
        INSERT INTO loyalty_transactions
            (customer_id, order_id, txn_type, points_delta, balance_after, note)
        VALUES (?, ?, 'earn', 55, ?, 'Pivo 12° – demo platba')
        """,
        [customer_ids[1], order_b1_id, new_balance],
    )
    conn.execute(
        """
        UPDATE customers
        SET points_balance = ?, total_spent = 55.00
        WHERE customer_id = ?
        """,
        [new_balance, customer_ids[1]],
    )
