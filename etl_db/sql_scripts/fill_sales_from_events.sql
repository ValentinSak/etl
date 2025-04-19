CREATE OR REPLACE PROCEDURE etl.fill_sales_from_events()
LANGUAGE plpgsql
AS $$
BEGIN
    WITH 
    sales_events_cte AS (
        SELECT 
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            (events.payload->>'order_id')::INT AS order_id,
            (events.payload->>'store_id')::INT AS store_id,
            (events.payload->>'product_id')::INT AS product_id,
            (events.payload->>'quantity')::INT AS quantity,
            (events.payload->>'sale_date')::TIMESTAMP AS sale_date
        FROM etl.raw_events events
        WHERE events.event_type = 'sales_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(sales.batch_created_at) FROM etl.sales AS sales), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.sales AS sales WHERE sales.raw_id = events.raw_id
          )
          AND (events.payload->>'order_id') IS NOT NULL
          AND (events.payload->>'store_id') IS NOT NULL
          AND (events.payload->>'product_id') IS NOT NULL
          AND (events.payload->>'quantity') IS NOT NULL
          AND (events.payload->>'sale_date') IS NOT NULL
          AND (events.payload->>'quantity')::INT > 0 
    ),
    rejected_sales_events_cte (
        SELECT 
            events.raw_id,
            events.batch_id,
            events.batch_created_at,
            events.payload,
            CASE 
                WHEN (events.payload->>'order_id') IS NULL THEN 'missing order_id'
                WHEN (events.payload->>'store_id') IS NULL THEN 'missing store_id'
                WHEN (events.payload->>'product_id') IS NULL THEN 'missing product_id'
                WHEN (events.payload->>'quantity') IS NULL THEN 'missing quantity'
                WHEN (events.payload->>'sale_date') IS NULL THEN 'missing sale_date'
                WHEN (events.payload->>'quantity')::INT <= 0 THEN 'invalid quantity'
                ELSE 'Unknown reason'
            END AS rejection_reason

        FROM etl.raw_events events
        WHERE events.event_type = 'sales_event'
          AND events.batch_created_at > COALESCE((SELECT MAX(sales.batch_created_at) FROM etl.sales AS sales), '1900-01-01')
          AND NOT EXISTS (
              SELECT 1 FROM etl.sales AS sales WHERE sales.raw_id = events.raw_id
          )
          AND (
            (events.payload->>'order_id') IS NOT NULL OR
            (events.payload->>'store_id') IS NOT NULL OR
            (events.payload->>'product_id') IS NOT NULL OR
            (events.payload->>'quantity') IS NOT NULL OR 
            (events.payload->>'sale_date') IS NOT NULL OR 
            (events.payload->>'quantity')::INT <= 0 
          )
    )


    INSERT INTO etl.sales 
        (raw_id, batch_id, batch_created_at, order_id, store_id, product_id, quantity, sale_date)
    SELECT 
        (raw_id, batch_id, batch_created_at, order_id, store_id, product_id, quantity, sale_date) 
    FROM sales_events_cte;

    INSERT INTO etl.rejected_sales_events
        (raw_id, batch_id, batch_created_at, payload, rejection_reason)
    SELECT 
        raw_id, batch_id, batch_created_at, payload, rejection_reason
    FROM rejected_events_cte;
END;
$$;
