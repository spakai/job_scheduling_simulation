DO $block$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'edr_reader') THEN
    CREATE ROLE edr_reader LOGIN PASSWORD 'edr-reader-local';
  END IF;
END
$block$;
GRANT CONNECT ON DATABASE edr TO edr_reader;
GRANT USAGE ON SCHEMA public TO edr_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO edr_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE edr_owner IN SCHEMA public GRANT SELECT ON TABLES TO edr_reader;
