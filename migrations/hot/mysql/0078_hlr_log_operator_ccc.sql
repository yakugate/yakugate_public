-- A number lookup's log row keeps the operator's name and the country calling
-- code it found (6 Nov 2026, Saleem: "beneficial for our records"): from our
-- HLR data, a live lookup, or the number range (the operators table). NULL
-- when not known. The day tables made before get the columns from the
-- template at start (schema_clones). The twin of the mssql file of the same
-- name. One column per ALTER.
ALTER TABLE tpl_day_hlr ADD COLUMN operator_name VARCHAR(64) DEFAULT NULL;
ALTER TABLE tpl_day_hlr ADD COLUMN ccc VARCHAR(6) DEFAULT NULL;
