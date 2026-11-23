-- One-time Unity Catalog setup for the bronze_silver bundle.
-- Run in the Databricks SQL editor as a user who can create schemas in the catalog.
--
-- Replace before running:
--   <catalog>             the catalog set in databricks.yml
--   <sp-application-id>   the service principal's application (client) ID

-- 1. Schemas and landing volumes, one set per target --------------------------
CREATE SCHEMA IF NOT EXISTS <catalog>.dev_bronze;
CREATE SCHEMA IF NOT EXISTS <catalog>.dev_silver;
CREATE VOLUME IF NOT EXISTS <catalog>.dev_bronze.landing;

CREATE SCHEMA IF NOT EXISTS <catalog>.test_bronze;
CREATE SCHEMA IF NOT EXISTS <catalog>.test_silver;
CREATE VOLUME IF NOT EXISTS <catalog>.test_bronze.landing;

CREATE SCHEMA IF NOT EXISTS <catalog>.bronze;
CREATE SCHEMA IF NOT EXISTS <catalog>.silver;
CREATE VOLUME IF NOT EXISTS <catalog>.bronze.landing;

-- 2. Let the service principal (used by GitHub Actions) run test and prod -----
GRANT USE CATALOG ON CATALOG <catalog> TO `<sp-application-id>`;

GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.test_bronze TO `<sp-application-id>`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.test_silver TO `<sp-application-id>`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.bronze      TO `<sp-application-id>`;
GRANT USE SCHEMA, CREATE TABLE ON SCHEMA <catalog>.silver      TO `<sp-application-id>`;

-- READ to load files; WRITE only matters if clean_source = MOVE
GRANT READ VOLUME, WRITE VOLUME ON VOLUME <catalog>.test_bronze.landing TO `<sp-application-id>`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME <catalog>.bronze.landing      TO `<sp-application-id>`;

-- 3. Check ---------------------------------------------------------------------
SHOW GRANTS `<sp-application-id>` ON SCHEMA <catalog>.bronze;
