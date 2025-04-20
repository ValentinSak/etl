CREATE PROCEDURE etl.fill_sales()
LANGUAGE plpgsql
AS $$
BEGIN
    COPY etl.sales (order_id, store_id, product_id, quantity, sale_date)
    FROM '/data/sales.csv'
    WITH CSV HEADER;
END;
$$;