# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

**Spolek Hub** — POS systém pro spolkové hospody. Streamlit + DuckDB, single-tenant, offline-first s volitelnou GDrive synchronizací.

- Python 3.13.2
- Virtual environment at `.venv/` (parent level — `POS_APP/.venv/`, NOT inside `spolek-hub/`)
- Application code in `spolek-hub/`

## Environment Setup

```bash
# Activate virtual environment (Windows)
.venv\Scripts\activate

# Install dependencies
pip install -r spolek-hub/requirements.txt
```

## Running

```bash
# From POS_APP root:
.venv/Scripts/streamlit run spolek-hub/app.py

# Or from spolek-hub/:
cd spolek-hub
../.venv/Scripts/streamlit run app.py
```

## Smoke Tests

```bash
# From POS_APP root:
.venv/Scripts/python spolek-hub/test_smoke.py
```

8 scenarios: DB init+seed, open tab, POS orders, calculate change, close tab + loyalty points, pay single order, customer order with full fulfillment transition, queue summary.

## Architecture

```text
spolek-hub/
├── app.py                  # Hlavní vstupní bod, URL routing, session init
├── core/
│   ├── auth.py             # Admin login, UID/table token validace
│   ├── config.py           # OrgContext (org settings, modules flags)
│   ├── database.py         # DatabaseManager, schema migrations, seed data
│   ├── gdrive_sync.py      # GDriveSync / MockGDriveSync (offline fallback)
│   └── timezone.py         # Prague timezone helpers (DST-aware via zoneinfo)
├── services/
│   ├── order_service.py    # create_pos_order(), create_customer_order()
│   ├── order_status_service.py  # transition_order(), get_queue_summary()
│   ├── tab_service.py      # open_tab(), close_tab(), pay_single_order(), TabSummary
│   ├── loyalty_service.py  # Body: get_available_rewards(), get_transaction_history()
│   ├── customer_service.py # list_customers(), get_customer_by_id()
│   ├── notification_service.py  # get_unread_notifications(), mark_as_read()
│   ├── permission_service.py    # can_customer_order(), can_customer_tab()
│   └── qr_service.py       # Generování QR kódů pro zákazníky a stoly
├── views/
│   ├── admin/
│   │   ├── dashboard.py    # KPIs, alerts, grafy (7 dní tržby, Top 5 produktů)
│   │   ├── tabs.py         # Správa účtů: otevřené (search+filter), nový, historie
│   │   ├── order_queue.py  # Fronta objednávek s fulfillment přechody
│   │   ├── new_order.py    # Nová POS objednávka (volitelně do fronty přípravy)
│   │   ├── customers.py    # Správa zákazníků, QR, body
│   │   └── products.py     # Správa produktů a kategorií
│   └── customer/
│       ├── profile.py      # Mobilní profil: body, tracker, objednávky, účty, notifikace
│       ├── order_menu.py   # Katalog produktů + košík
│       └── rewards.py      # Katalog odměn + historie transakcí
└── analytics/
    └── reports.py          # Pandas reporty: weekly_revenue, top_products, atd.
```

## URL Routing (`app.py`)

| URL parametry | Zobrazení |
| --- | --- |
| _(nic)_ | Admin login → admin dashboard |
| `?uid=<token>` | Zákaznický profil (mobile) |
| `?uid=<token>&table=<token>` | Zákaznický profil + kontext stolu |
| `?table=<token>` | Anonymní objednávka ke stolu (QR na stole) |

## Klíčové designové rozhodnutí

### Objednávky a fronta

- **Admin POS objednávky** (`create_pos_order`): výchozí `fulfillment_status='completed'` — přeskakují frontu. Checkbox "Odeslat do fronty přípravy" (`queue=True`) nastaví `pending`.
- **Zákaznické objednávky** (`create_customer_order`): vždy `fulfillment_status='pending'` → jdou do fronty.

### Účty (Tabs)

- `payment_mode='immediate'`: platba per objednávka při vydání
- `payment_mode='tab'`: platba celého účtu při uzavření
- `can_tab` permission: zákazník musí mít explicitně povolenou možnost "na účet"

### Timezone

- DB ukládá UTC (naive datetime)
- `core/timezone.py`: `to_prague(dt)`, `fmt(dt, fmt)`, `prague_offset_hours()`
- DST automaticky přes `zoneinfo.ZoneInfo("Europe/Prague")` (Python 3.9+)
- SQL DATE grouping: `DATE(created_at + INTERVAL '{offset} hours')` — offset se dynamicky injektuje

### DuckDB DECIMAL → float

- Schema používá `DECIMAL(10,2)` → Python vrací `Decimal` objekty
- `st.bar_chart` a pandas je špatně interpretují → vždy castovat `::DOUBLE` v agregačních dotazech

### Offline / sync

- `MockGDriveSync`: DB uložena v `tempfile.gettempdir()/spolek_hub.db` — pro produkci změnit na konfigurovatelnou cestu
- `GDriveSync`: sync přes Google Drive API
- `sync.mark_dirty()` — volat po každém zápisu; sync thread uploaduje na pozadí

## Permissions

```text
customer_permissions tabulka:
  can_order  — může objednávat přes zákaznickou app
  can_tab    — může mít otevřený účet (platba při uzavření)
  credit_limit — max dluh na účtu
```

## Budoucí TODO (zaznamenáno, neimplementováno)

- **Produkční DB cesta**: `MockGDriveSync` by měl brát `LOCAL_DB_PATH` z `.env` / Streamlit secrets místo temp složky
- **Events modul**: stránka existuje ale říká "připravujeme"
