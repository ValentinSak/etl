CREATE PROCEDURE etl.fill_stores()
LANGUAGE plpgsql
AS $$
BEGIN
COPY etl.stores (id, created_at, name, tax_id, status)
FROM '/data/stores.csv'
WITH CSV HEADER;
END;
$$;