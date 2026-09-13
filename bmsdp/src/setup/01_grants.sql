-- Role-based grants via Azure AD groups mapped into Unity Catalog.
-- Group names below are placeholders - replace with your Entra ID group names.

USE CATALOG ${catalog_name};

-- Data engineers: full read/write across all layers for pipeline development.
GRANT USE CATALOG, USE SCHEMA, CREATE TABLE, MODIFY, SELECT
  ON CATALOG ${catalog_name} TO `bmsdp-data-engineers`;

-- Analysts / business users: read-only on gold (curated) layer only.
GRANT USE CATALOG ON CATALOG ${catalog_name} TO `bmsdp-analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog_name}.gold TO `bmsdp-analysts`;

-- ML engineers: read on silver/gold plus write on gold for feature tables.
GRANT USE CATALOG ON CATALOG ${catalog_name} TO `bmsdp-ml-engineers`;
GRANT USE SCHEMA, SELECT ON SCHEMA ${catalog_name}.silver TO `bmsdp-ml-engineers`;
GRANT USE SCHEMA, SELECT, MODIFY, CREATE TABLE ON SCHEMA ${catalog_name}.gold TO `bmsdp-ml-engineers`;

-- Nobody outside data engineers touches bronze directly (raw/unmasked PII risk).
REVOKE SELECT ON SCHEMA ${catalog_name}.bronze FROM `bmsdp-analysts`;
REVOKE SELECT ON SCHEMA ${catalog_name}.bronze FROM `bmsdp-ml-engineers`;
