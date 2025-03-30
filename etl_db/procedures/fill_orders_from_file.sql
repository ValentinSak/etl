CREATE PROCEDURE etl.fill_orders()
LANGUAGE plpgsql
AS $$
BEGIN
COPY etl.orders
FROM '/data/orders.csv'
WITH CSV HEADER;
END;
$$;