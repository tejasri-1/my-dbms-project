/* contrib/auto_index_advisor/auto_index_advisor--1.0.sql */

CREATE FUNCTION auto_index_advisor_check()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_refresh_recommendations()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_refresh_costing()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_apply_recommendations()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_drop_indexes()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_run_test_cycle()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION auto_index_advisor_run_full_cycle()
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
