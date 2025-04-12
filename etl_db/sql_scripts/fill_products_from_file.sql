CREATE PROCEDURE etl.fill_products()
LANGUAGE plpgsql
AS $$
BEGIN
COPY etl.products
FROM '/data/products.csv'
WITH CSV HEADER;
END;
$$;