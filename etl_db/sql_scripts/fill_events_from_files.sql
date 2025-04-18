CREATE OR REPLACE PROCEDURE etl.load_csv_file(IN file_path text, IN file_name text)
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
	     FROM ''%s''
	     WITH (FORMAT csv, HEADER true, DELIMITER '','', QUOTE ''"'')',
        file_path
    );

    RAISE NOTICE 'File % loaded and logged.', file_name;
END;
$$
;