CREATE OR REPLACE FUNCTION metadata_schema.sp_debug (sql_query text, param text, proc text) 
	RETURNS void
	LANGUAGE plpgsql
	VOLATILE
AS $$

<<LMAIN>> 
BEGIN

INSERT INTO metadata_schema.debug_log(sql_query, run_tm, param, proc) VALUES (sql_query, clock_timestamp(), param, proc);
End LMAIN;
$$