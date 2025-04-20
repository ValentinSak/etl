CREATE PROCEDURE etl.fill_products()
LANGUAGE plpgsql
AS $$
BEGIN
    COPY etl.products (created_at, name, price)
    FROM '/data/products.csv'
    WITH CSV HEADER;
END;
$$;