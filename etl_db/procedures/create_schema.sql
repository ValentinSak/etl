BEGIN;

CREATE SCHEMA IF NOT EXISTS etl;

DROP TABLE IF EXISTS etl.sales;
DROP TABLE IF EXISTS etl.orders;
DROP TABLE IF EXISTS etl.products;
DROP TABLE IF EXISTS etl.stores;
DROP TABLE IF EXISTS etl.raw_events;

CREATE TABLE etl.stores (
    id INT PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT,
    tax_id BIGINT,
    status TEXT CHECK (status IN ('active', 'closed', 'test'))
);

CREATE TABLE etl.products (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT
);

CREATE TABLE etl.orders (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    user_id INT
);

CREATE TABLE etl.sales (
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES etl.orders(id),
    store_id INT REFERENCES etl.stores(id),
    product_id INT REFERENCES etl.products(id),
    quantity INT,
    sale_date TIMESTAMP
);

CREATE TABLE etl.raw_events (
    id SERIAL PRIMARY KEY,
    batch_id TEXT;
    event_type TEXT,
    payload JSONB,
    created_at TIMESTAMP DEFAULT now()
);

CREATE INDEX idx_sales_order_id ON etl.sales(order_id);
CREATE INDEX idx_sales_store_number ON etl.sales(store_id);
CREATE INDEX idx_sales_product_id ON etl.sales(product_id);
CREATE INDEX idx_sales_sale_date ON etl.sales(sale_date);
CREATE INDEX idx_raw_events_event_type ON raw_events(event_type);
CREATE INDEX idx_raw_events_created_at ON raw_events(created_at);

COMMIT;