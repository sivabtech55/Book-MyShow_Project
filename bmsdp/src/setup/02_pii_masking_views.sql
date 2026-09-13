-- Dynamic views over silver.users that mask PII for non-privileged groups.
-- Analysts/ML engineers query gold/masked views, never raw silver.users directly
-- for anything containing phone/email.

USE CATALOG ${catalog_name};

CREATE OR REPLACE VIEW gold.dim_user_masked AS
SELECT
  user_id,
  CASE
    WHEN is_account_group_member('bmsdp-data-engineers') THEN email
    ELSE regexp_replace(email, '(^.).*(@.*$)', '$1***$2')
  END AS email,
  CASE
    WHEN is_account_group_member('bmsdp-data-engineers') THEN phone
    ELSE concat('******', right(phone, 4))
  END AS phone,
  city,
  signup_date,
  effective_start,
  effective_end,
  is_current
FROM gold.dim_user;

GRANT SELECT ON VIEW gold.dim_user_masked TO `bmsdp-analysts`;
GRANT SELECT ON VIEW gold.dim_user_masked TO `bmsdp-ml-engineers`;
