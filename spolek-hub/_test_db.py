import sys
sys.path.insert(0, r"d:/OneDrive/Data engineer/Projekty/POS_APP/spolek-hub")

from core.database import DatabaseManager, seed_demo_data, is_empty

db = DatabaseManager(":memory:")
db.initialize()
conn = db.get_connection()

assert is_empty(conn), "should be empty before seed"
seed_demo_data(conn)
assert not is_empty(conn), "should not be empty after seed"

orgs = conn.execute("SELECT name, slug, default_tab_mode FROM organizations").fetchall()
assert len(orgs) == 1
assert orgs[0] == ("TJ Sokol Demo", "demo", "tab")

customers = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
assert customers == 3, f"expected 3 customers, got {customers}"

tabs = conn.execute("SELECT status, payment_mode FROM tabs ORDER BY label").fetchall()
assert len(tabs) == 2
assert ("closed", "immediate") in tabs
assert ("open", "tab") in tabs

orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
assert orders == 3, f"expected 3 orders, got {orders}"

products = conn.execute("SELECT COUNT(*) FROM products").fetchone()[0]
assert products == 5

tables = conn.execute("SELECT COUNT(*) FROM tables").fetchone()[0]
assert tables == 4

print("OK – schema a seed_demo_data ověřeny")
