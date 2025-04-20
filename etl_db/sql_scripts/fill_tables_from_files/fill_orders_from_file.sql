CREATE PROCEDURE etl.fill_orders()
LANGUAGE plpgsql
AS $$
BEGIN
    BEGIN
        COPY etl.orders (created_at, user_id)
        FROM '/data/orders.csv'
        WITH CSV HEADER;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'COPY failed: %', SQLERRM;
    END;
END;
$$;