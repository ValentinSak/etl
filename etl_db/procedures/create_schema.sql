BEGIN;

CREATE SCHEMA IF NOT EXISTS etl;

CREATE OR REPLACE PROCEDURE etl.initialize_schema()
LANGUAGE plpgsql
AS $$
DROP TABLE IF EXISTS etl.sales;
DROP TABLE IF EXISTS etl.orders;
DROP TABLE IF EXISTS etl.products;
DROP TABLE IF EXISTS etl.stores;

CREATE TABLE etl.stores (
    store_number INT PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT,
    store_tax_id BIGINT,
    status TEXT CHECK (status IN ('active', 'closed', 'test'))
);

CREATE TABLE etl.products (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    name TEXT
);

CREATE TABLE etl.orders (
    order_id SERIAL PRIMARY KEY,
    created_at TIMESTAMP,
    user_id INT
);

CREATE TABLE etl.sales (
    sale_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES etl.orders(order_id),
    store_number INT REFERENCES etl.stores(store_number),
    product_id INT REFERENCES etl.products(id),
    quantity INT,
    sale_date TIMESTAMP
);

CREATE INDEX idx_sales_order_id ON etl.sales(order_id);
CREATE INDEX idx_sales_store_number ON etl.sales(store_number);
CREATE INDEX idx_sales_product_id ON etl.sales(product_id);
CREATE INDEX idx_sales_sale_date ON etl.sales(sale_date);
END $$;

COMMIT;