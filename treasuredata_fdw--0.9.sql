\echo Use "CREATE EXTENSION treasuredata_fdw" to load this file. \quit

CREATE FUNCTION treasuredata_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION treasuredata_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER treasuredata_fdw
  HANDLER treasuredata_fdw_handler
  VALIDATOR treasuredata_fdw_validator;
