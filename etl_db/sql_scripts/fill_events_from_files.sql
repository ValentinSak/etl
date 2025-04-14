CREATE OR REPLACE PROCEDURE load_csv_file(file_path TEXT, file_name TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO utilities.processed_files(filename)
    VALUES (file_name)
    ON CONFLICT (filename) DO NOTHING;

    IF NOT FOUND THEN
        RAISE NOTICE 'File % already logged. Skipping.', file_name;
        RETURN;
    END IF;

    EXECUTE format(
        'COPY etl.raw_events(batch_id, event_type, payload, batch_created_at)
         FROM %L
         WITH (FORMAT csv, HEADER true)',
        file_path
    );

    RAISE NOTICE 'File % loaded and logged.', file_name;
END;
$$;
