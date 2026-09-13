-- Unity Catalog bootstrap: catalogs per environment.
-- Run once per workspace (dev/stage/prod) by a metastore admin, via the
-- bootstrap_unity_catalog job (resources/jobs/bootstrap_unity_catalog.job.yml).

CREATE CATALOG IF NOT EXISTS ${catalog_name}
  COMMENT 'BookMyShow data platform - ${target} environment';

USE CATALOG ${catalog_name};

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw, as-landed data. Schema-on-read.';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleansed, deduplicated, conformed data.';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Business-level facts, dimensions, aggregates.';
