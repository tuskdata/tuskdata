"""Create the `tusk_demo` PostgreSQL database used by the docs screenshots.

Synthetic data only — customers, products, orders, order_items, events —
so nothing private ever lands in a public screenshot. Idempotent: drops
and recreates the database each run.

    .venv/bin/python scripts/demo_db.py [--dsn postgresql://postgres@localhost:5432/postgres]
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta

import psycopg

DB = "tusk_demo"
SCHEMA = """
CREATE TABLE customers (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL UNIQUE,
    country text NOT NULL,
    segment text NOT NULL CHECK (segment IN ('smb', 'mid', 'enterprise')),
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE products (
    id serial PRIMARY KEY,
    sku text NOT NULL UNIQUE,
    name text NOT NULL,
    category text NOT NULL,
    unit_price numeric(10,2) NOT NULL,
    active boolean NOT NULL DEFAULT true
);
CREATE TABLE orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES customers(id),
    status text NOT NULL CHECK (status IN ('pending', 'paid', 'shipped', 'cancelled')),
    total numeric(12,2) NOT NULL,
    created_at timestamptz NOT NULL,
    shipped_at timestamptz
);
CREATE INDEX orders_customer_id_idx ON orders (customer_id);
CREATE INDEX orders_created_at_idx ON orders (created_at);
CREATE TABLE order_items (
    id serial PRIMARY KEY,
    order_id integer NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id integer NOT NULL REFERENCES products(id),
    quantity integer NOT NULL CHECK (quantity > 0),
    unit_price numeric(10,2) NOT NULL
);
CREATE INDEX order_items_order_id_idx ON order_items (order_id);
CREATE TABLE events (
    id bigserial PRIMARY KEY,
    customer_id integer REFERENCES customers(id),
    kind text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}',
    occurred_at timestamptz NOT NULL
);
CREATE INDEX events_occurred_at_idx ON events (occurred_at);
"""

FIRST = ["Ana", "Luis", "María", "Carlos", "Sofía", "Diego", "Lucía", "Javier", "Elena", "Pedro",
         "Grace", "Wei", "Aisha", "Tomás", "Nadia", "Omar", "Yuki", "Ivan", "Zoe", "Mateo"]
LAST = ["García", "Pérez", "Rodríguez", "Lee", "Müller", "Silva", "Kim", "Rossi", "Okafor", "Novak"]
COUNTRIES = ["DO", "ES", "MX", "US", "CO", "AR", "BR", "DE", "PT", "CL"]
CATEGORIES = ["Coffee", "Tea", "Equipment", "Merch", "Subscriptions"]
EVENT_KINDS = ["signup", "login", "cart_add", "checkout", "support_ticket", "churn_risk"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://postgres@localhost:5432/postgres")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    with psycopg.connect(args.dsn, autocommit=True) as admin:
        admin.execute(f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
        admin.execute(f"CREATE DATABASE {DB}")

    demo_dsn = args.dsn.rsplit("/", 1)[0] + f"/{DB}"
    with psycopg.connect(demo_dsn) as conn:
        conn.execute(SCHEMA)
        cur = conn.cursor()

        customers = []
        start = datetime(2025, 1, 1)
        for i in range(400):
            name = f"{random.choice(FIRST)} {random.choice(LAST)}"
            customers.append((name, f"user{i}@example.com", random.choice(COUNTRIES),
                              random.choices(["smb", "mid", "enterprise"], [6, 3, 1])[0],
                              start + timedelta(days=random.randint(0, 600))))
        cur.executemany("INSERT INTO customers (name, email, country, segment, created_at) VALUES (%s, %s, %s, %s, %s)", customers)

        products = []
        for i in range(60):
            cat = random.choice(CATEGORIES)
            products.append((f"SKU-{1000 + i}", f"{cat} item {i}", cat, round(random.uniform(4, 240), 2), random.random() > 0.08))
        cur.executemany("INSERT INTO products (sku, name, category, unit_price, active) VALUES (%s, %s, %s, %s, %s)", products)

        orders = []
        for i in range(6000):
            created = start + timedelta(days=random.randint(0, 610), hours=random.randint(0, 23))
            status = random.choices(["pending", "paid", "shipped", "cancelled"], [1, 3, 6, 1])[0]
            shipped = created + timedelta(days=random.randint(1, 6)) if status == "shipped" else None
            orders.append((random.randint(1, 400), status, 0, created, shipped))
        cur.executemany("INSERT INTO orders (customer_id, status, total, created_at, shipped_at) VALUES (%s, %s, %s, %s, %s)", orders)

        items = []
        for order_id in range(1, 6001):
            for _ in range(random.randint(1, 4)):
                pid = random.randint(1, 60)
                items.append((order_id, pid, random.randint(1, 5), products[pid - 1][3]))
        cur.executemany("INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)", items)
        cur.execute("UPDATE orders o SET total = s.t FROM (SELECT order_id, SUM(quantity * unit_price) AS t FROM order_items GROUP BY order_id) s WHERE s.order_id = o.id")

        events = []
        for _ in range(40000):
            events.append((random.randint(1, 400), random.choice(EVENT_KINDS),
                           '{"source": "web"}', start + timedelta(minutes=random.randint(0, 610 * 24 * 60))))
        cur.executemany("INSERT INTO events (customer_id, kind, payload, occurred_at) VALUES (%s, %s, %s, %s)", events)
        conn.execute("ANALYZE")
        conn.commit()
    print(f"{DB} ready: 400 customers, 60 products, 6000 orders, {len(items)} items, 40000 events")


if __name__ == "__main__":
    main()
