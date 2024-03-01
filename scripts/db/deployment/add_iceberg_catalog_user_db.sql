-- CREATE DB
CREATE DATABASE iceberg_catalog;

--CONNECT to database
\connect iceberg_catalog;

-- CREATE user
CREATE USER de_user PASSWORD 'de_pwd';

-- CREATE SCHEMA
CREATE SCHEMA IF NOT EXISTS iceberg_dev;
SET SCHEMA 'iceberg_dev';

-- GRANT privileges
GRANT USAGE, CREATE ON SCHEMA iceberg_dev TO de_user;
GRANT ALL ON ALL TABLES IN SCHEMA iceberg_dev TO de_user;
