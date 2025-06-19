CREATE TABLE metadata_schema.debug_log (
	id bigserial NOT NULL,
	sql_query text NULL, 
	run_tm timestamp NULL,
	param text NULL,
	proc text NULL
);