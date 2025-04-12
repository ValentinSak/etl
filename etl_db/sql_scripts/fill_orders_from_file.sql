CREATE PROCEDURE etl.fill_orders()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        COPY etl.orders
        FROM '/data/orders.csv'
        WITH CSV HEADER;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'COPY failed: %', SQLERRM;
    END;
END;
$$;