-- An HLR answer keeps the operator's name and the country calling code
-- (Saleem, 6 Nov 2026: "also accept operator name, country calling code ccc,
-- these are beneficial for our records"). Filled by a live lookup (an HLR
-- provider's response values operator_name and ccc), by an import, or left
-- NULL. The shards made before get the columns from the template at start
-- (bootstrap schema_clones, family hlr_template). One column per ALTER.
-- The twin of the mssql file of the same name.
ALTER TABLE hlr_template ADD COLUMN operator_name VARCHAR(64) DEFAULT NULL;
ALTER TABLE hlr_template ADD COLUMN ccc VARCHAR(6) DEFAULT NULL;
