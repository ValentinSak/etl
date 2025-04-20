CREATE OR REPLACE PROCEDURE etl.fill_products_from_events()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH product_events_cte AS (
        SELECT 
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            (events.payload->>'created_at')::timestamp AS created_at,
            (events.payload->>'name')::TEXT AS name,
            (events.payload->>'price'):: DECIMAL(10, 2) AS price
        FROM etl.raw_events events
        WHERE events.event_type = 'product_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(products.batch_created_at) FROM etl.products AS products), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.products AS products WHERE products.raw_id = events.raw_id
          )
    )
    INSERT INTO etl.products (raw_id, batch_id, batch_created_at, created_at, name, price)
    SELECT raw_id, batch_id, batch_created_at, created_at, name, price FROM product_events_cte;
END;
$$;
