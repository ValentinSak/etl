BEGIN;

CREATE SCHEMA IF NOT EXISTS etl;
CREATE SCHEMA IF NOT EXISTS utilities;

DROP TABLE IF EXISTS etl.sales;
DROP TABLE IF EXISTS etl.orders;
DROP TABLE IF EXISTS etl.products;
DROP TABLE IF EXISTS etl.stores;
DROP TABLE IF EXISTS etl.raw_events;

CREATE TABLE etl.raw_events (
    raw_id SERIAL PRIMARY KEY,
    batch_id TEXT,
    event_type TEXT,
    payload JSONB,
    batch_created_at TIMESTAMP
);

CREATE TABLE etl.stores (
    raw_id INT REFERENCES etl.raw_events(raw_id),
    batch_id TEXT,
    batch_created_at TIMESTAMP,
    id INT PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT,
    tax_id BIGINT,
    status TEXT CHECK (status IN ('active', 'closed', 'test'))
);

CREATE TABLE etl.products (
    raw_id INT REFERENCES etl.raw_events(raw_id),
    batch_id TEXT,
    batch_created_at TIMESTAMP,
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT
);

CREATE TABLE etl.orders (
    raw_id INT REFERENCES etl.raw_events(raw_id),
    batch_id TEXT,
    batch_created_at TIMESTAMP,
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    user_id INT
);

CREATE TABLE etl.sales (
    raw_id INT REFERENCES etl.raw_events(raw_id),
    batch_id TEXT,
    batch_created_at TIMESTAMP,
    id SERIAL PRIMARY KEY,
    order_id INT REFERENCES etl.orders(id),
    store_id INT REFERENCES etl.stores(id),
    product_id INT REFERENCES etl.products(id),
    quantity INT,
    sale_date TIMESTAMP
);

CREATE TABLE utilities.processed_files (
    id SERIAL PRIMARY KEY,
    filename TEXT UNIQUE,
    loaded_at TIMESTAMP DEFAULT now()
);

CREATE TABLE etl.rejected_sales_events (
    raw_id INT PRIMARY KEY,
    batch_id TEXT,
    batch_created_at TIMESTAMP,
    rejection_reason TEXT,
    payload JSONB,
    rejected_at TIMESTAMP DEFAULT now()
);

COMMIT;

BEGIN;

CREATE INDEX idx_sales_order_id ON etl.sales(order_id);
CREATE INDEX idx_sales_store_number ON etl.sales(store_id);
CREATE INDEX idx_sales_product_id ON etl.sales(product_id);
CREATE INDEX idx_sales_sale_date ON etl.sales(sale_date);   
CREATE INDEX idx_raw_events_event_type ON etl.raw_events(event_type);
CREATE INDEX idx_raw_events_created_at ON etl.raw_events(batch_created_at);
CREATE INDEX idx_orders_raw_id ON etl.orders(raw_id);


COMMIT;