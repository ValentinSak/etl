CREATE PROCEDURE etl.fill_stores()
LANGUAGE plpgsql
AS $$
BEGIN
COPY etl.stores
FROM '/data/stores.csv'
WITH CSV HEADER;
END;
$$;