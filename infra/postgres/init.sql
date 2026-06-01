-- Airflow DB is created by the POSTGRES_DB env var.
-- This creates the secondary OLTP source database used by simulators.

CREATE DATABASE oltp_source;
CREATE USER oltp_user WITH PASSWORD 'oltp_pass_2024';
GRANT ALL PRIVILEGES ON DATABASE oltp_source TO oltp_user;

\c oltp_source oltp_user

CREATE SCHEMA IF NOT EXISTS store;

CREATE TABLE store.products (
    product_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(200) NOT NULL,
    category        VARCHAR(100),
    price           DECIMAL(10, 2),
    stock_quantity  INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE store.customers (
    customer_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email        VARCHAR(255) UNIQUE NOT NULL,
    name         VARCHAR(200),
    region       VARCHAR(100),
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE store.orders (
    order_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id  UUID REFERENCES store.customers(customer_id),
    product_id   UUID REFERENCES store.products(product_id),
    quantity     INTEGER,
    total_amount DECIMAL(12, 2),
    status       VARCHAR(50) DEFAULT 'pending',
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_orders_customer ON store.orders(customer_id);
CREATE INDEX idx_orders_created ON store.orders(created_at);
