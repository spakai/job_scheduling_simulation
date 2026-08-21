CREATE ROLE scheduler_owner LOGIN PASSWORD 'scheduler-local';
CREATE ROLE edr_owner LOGIN PASSWORD 'edr-local';
CREATE ROLE edr_sink LOGIN PASSWORD 'edr-sink-local';
CREATE ROLE edr_reader LOGIN PASSWORD 'edr-reader-local';
CREATE DATABASE scheduler OWNER scheduler_owner;
CREATE DATABASE edr OWNER edr_owner;
REVOKE CONNECT ON DATABASE scheduler FROM PUBLIC;
REVOKE CONNECT ON DATABASE edr FROM PUBLIC;
GRANT CONNECT ON DATABASE scheduler TO scheduler_owner;
GRANT CONNECT ON DATABASE edr TO edr_owner;
GRANT CONNECT ON DATABASE edr TO edr_reader;
\connect edr
GRANT CONNECT ON DATABASE edr TO edr_sink;
GRANT USAGE ON SCHEMA public TO edr_sink;
GRANT USAGE ON SCHEMA public TO edr_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE edr_owner IN SCHEMA public GRANT SELECT ON TABLES TO edr_reader;
