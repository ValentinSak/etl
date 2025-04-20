CREATE OR REPLACE PROCEDURE etl.fill_orders_from_events()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH order_events_cte AS (
        SELECT 
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            (events.payload->>'created_at')::timestamp AS created_at,
            (events.payload->>'user_id')::int AS user_id
        FROM etl.raw_events events
        WHERE events.event_type = 'order_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(orders.batch_created_at) FROM etl.orders AS orders), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.orders AS orders WHERE orders.raw_id = events.raw_id
          )
    )
    INSERT INTO etl.orders (raw_id, batch_id, batch_created_at, created_at, user_id)
    SELECT raw_id, batch_id, batch_created_at, created_at, user_id FROM order_events_cte;
END;
$$;
