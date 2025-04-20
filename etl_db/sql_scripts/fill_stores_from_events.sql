CREATE OR REPLACE PROCEDURE etl.fill_stores_from_events()
LANGUAGE plpgsql
AS $$
BEGIN
WITH 
    store_events_cte AS (
        SELECT 
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            (events.payload->>'created_at')::timestamp AS created_at,
            (events.payload->>'name'):: TEXT AS name,
            (events.payload->>'tax_id'):: BIGINT AS tax_id,
            (events.payload->>'status'):: TEXT AS status
        FROM etl.raw_events events
        WHERE events.event_type = 'store_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(stores.batch_created_at) FROM etl.stores AS stores), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.stores AS stores WHERE stores.raw_id = events.raw_id
          )
          AND (events.payload->>'created_at') IS NOT NULL
          AND (events.payload->>'name') IS NOT NULL
          AND (events.payload->>'tax_id') IS NOT NULL
          AND (events.payload->>'status') IS NOT NULL
    )

    INSERT INTO etl.stores 
        (raw_id, batch_id, batch_created_at, created_at, name, tax_id, status)
    SELECT 
        raw_id, batch_id, batch_created_at, created_at, name, tax_id, status
    FROM store_events_cte;

WITH 
rejected_store_events_cte AS (
        SELECT
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            events.payload,
            CASE
                WHEN (events.payload->>'created_at') IS NULL THEN 'missing_created_at'
                WHEN (events.payload->>'name') IS NULL THEN 'missing_name'
                WHEN (events.payload->>'tax_id') IS NULL THEN 'missing_tax_id'
                WHEN (events.payload->>'status') IS NULL THEN 'missing_status'
                WHEN (events.payload->>'status') NOT IN ('active', 'closed', 'test') THEN 'wrong_status'
            END as rejection_reason
        FROM etl.raw_events events
        WHERE events.event_type = 'store_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(stores.batch_created_at) FROM etl.stores AS stores), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.stores AS stores WHERE stores.raw_id = events.raw_id
          )
          AND (
            (events.payload->>'created_at') IS NULL OR
            (events.payload->>'name') IS NOT NULL OR
            (events.payload->>'tax_id') IS NOT NULL OR 
            (events.payload->>'status') IS NOT NULL OR
            (events.payload->>'status') NOT IN ('active', 'closed', 'test')
          )
    )



    INSERT INTO etl.rejected_events
        (raw_id, batch_id, batch_created_at, payload, rejection_reason)
    SELECT 
        raw_id, batch_id, batch_created_at, payload, rejection_reason
    FROM rejected_store_events_cte;


END;
$$;