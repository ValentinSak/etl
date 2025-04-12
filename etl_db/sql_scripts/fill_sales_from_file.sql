CREATE PROCEDURE etl.fill_sales()
LANGUAGE plpgsql
AS $$
BEGIN
COPY etl.sales
FROM '/data/sales.csv'
WITH CSV HEADER;
END;
$$;