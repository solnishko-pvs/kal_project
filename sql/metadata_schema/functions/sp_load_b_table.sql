CREATE OR REPLACE FUNCTION metadata_schema.sp_load_b_table(in in_schema_name text, in in_atable_name text, in in_btable_name text, in in_workflow_run_id int8, out return_long_count_value int8, out return_int_return_code int4)
	RETURNS record
	LANGUAGE plpgsql
AS $$

<<LMAIN>>
DECLARE
	v_workflow_run_id 		text = cast(in_workflow_run_id as text) ;
	v_sql 					text default '';
	v_b_table_column_list 	text;
	v_query_column_list 	text;
	v_insert_query_templ 	text := E'insert into {in_schema_name}.{in_btable_name} ({b_table_column_list})' ||
								 	E'\nselect\n\t{query_column_list}\nfrom {in_schema_name}.{a_table_name}';
	tmp_result_row_count 	bigint default 0;
	tmp_result_code 		int default 0;
	v_proc 					text default 'sp_load_b_table_' || in_btable_name;
	v_error_text 			text;
	v_uuid_column 			text;
	_ACTIVITY_COUNT 		bigint default 0;
begin

	tmp_result_row_count = 0;
	raise notice 'Start:  %', trim(cast(current_timestamp AS text));
	tmp_result_code=1;
	perform metadata_schema.sp_debug('Start loading ' || in_btable_name ||' from a_stage ', v_workflow_run_id, v_proc);
	
	v_sql = 'truncate table '||in_schema_name||'.'||in_btable_name||' RESTART IDENTITY';
	raise notice 'v_sql: %', v_sql;
	perform metadata_schema.sp_debug(E'Query for TRUNCATE: \n\n'''||v_sql||'''', v_workflow_run_id, v_proc);
	
	execute v_sql;

	/*
	prepare sql query
	1. get B-table column list, exclude deleted flag and row nbr if columns have default values
	*/

	select string_agg(column_name, ',') into v_b_table_column_list
	from information_schema.columns
	where table_schema = in_schema_name
		and table_name = lower(in_btable_name)
		and not (column_name = 'deleted flag' and column_default is not null)
		and not (column_name = 'row_nbr' and column_default is not null);
	  
	raise notice 'v_b_table_column_list: %',v_b_table_column_list;

	tmp_result_code=tmp_result_code + 1;
	get diagnostics _ACTIVITY_COUNT=row_count;
	_ACTIVITY_COUNT=coalesce(_ACTIVITY_COUNT, 0);
	

	tmp_result_row_count=tmp_result_row_count + _ACTIVITY_COUNT;

	/*
	2. prepare query_column_list text
	(define workflow run id if it exists)
	*/
	v_query_column_list = ',' ||replace(v_b_table_column_list, 'workflow_run_id', in_workflow_run_id || ' as workflow_run_id') ||',';
	raise notice 'v_query_column_list: %',v_query_column_list;

	foreach v_uuid_column in array (string_to_array(substr(coalesce(metadata_schema.k_sp_src_uuid_columns(in_btable_name ), ''), 4), ', '||chr(13)||chr(10) ||'md5')) 
	loop

		v_query_column_list = replace( v_query_column_list, ','|| substring(v_uuid_column,' as (.+)' ) || ',', ','||'md5' || v_uuid_column||',');
		
	end loop;

	v_query_column_list = substr(v_query_column_list, 2, length(v_query_column_list)-2);
	raise notice 'v_query_column_list: %',v_query_column_list;

	v_sql = replace(
			replace(
					replace(
							replace(
									replace(v_insert_query_templ, '{in_schema_name}', in_schema_name), -- schema_name 
																 '{in_btable_name}', in_btable_name), -- table_name
							'{b_table_column_list}', '"'||replace(v_b_table_column_list,',','","') ||'"'
							),
					'{query_column_list}',v_query_column_list),
			'{a_table_name}', in_atable_name);

	perform metadata_schema.sp_debug (E'Query for loading: \n\n'''||v_sql||'''',v_workflow_run_id, v_proc);
	raise notice 'v_sql: %',v_sql;
	execute v_sql;
	
	tmp_result_code=tmp_result_code + 1;
	get diagnostics _ACTIVITY_COUNT=row_count;
	_ACTIVITY_COUNT=coalesce(_ACTIVITY_COUNT, 0);
	tmp_result_row_count=tmp_result_row_count + _ACTIVITY_COUNT;	
	perform metadata_schema.sp_debug ('Finish loading '||in_btable_name ||E' from a_stage.\nLoaded ' || _ACTIVITY_COUNT||' rows', v_workflow_run_id, v_proc);
	
	return_int_return_code=0;
	return_long_count_value=tmp_result_row_count;
	
exception
	when others then
		GET STACKED DIAGNOSTICS v_error_text = MESSAGE_TEXT;
		return_int_return_code =- 1 * tmp_result_code;
		return_long_count_value =-1;
		raise notice '%',v_error_text;
		perform metadata_schema.sp_debug('Loading error ' || v_error_text||' '||in_btable_name||' v_sql is '||v_sql, v_workflow_run_id, v_proc);
		exit LMAIN;

end;

$$
;