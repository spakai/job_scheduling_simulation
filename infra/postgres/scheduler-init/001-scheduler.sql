CREATE ROLE scheduler_owner LOGIN PASSWORD 'scheduler-local';
CREATE DATABASE scheduler OWNER scheduler_owner;
REVOKE CONNECT ON DATABASE scheduler FROM PUBLIC;
GRANT CONNECT ON DATABASE scheduler TO scheduler_owner;
