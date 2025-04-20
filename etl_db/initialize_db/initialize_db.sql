-- create schema
\i /sql_scripts/create_schema/create_schema.sql;
-- fill tables from files
\i /sql_scripts/fill_tables_from_files/fill_orders_from_file.sql;
\i /sql_scripts/fill_tables_from_files/fill_products_from_file.sql;
\i /sql_scripts/fill_tables_from_files/fill_stores_from_file.sql;
\i /sql_scripts/fill_tables_from_files/fill_sales_from_file.sql;
\i /sql_scripts/fill_tables_from_files/fill_events_from_files.sql;
\i /sql_scripts/fill_tables_from_files/fill_tables.sql;

-- fill tables from events
\i /sql_scripts/fill_tables_from_events/fill_orders_from_events.sql;
\i /sql_scripts/fill_tables_from_events/fill_stores_from_events.sql;
\i /sql_scripts/fill_tables_from_events/fill_products_from_events.sql;
\i /sql_scripts/fill_tables_from_events/fill_sales_from_events.sql;