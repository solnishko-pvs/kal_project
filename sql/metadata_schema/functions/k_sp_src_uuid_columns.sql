CREATE OR REPLACE FUNCTION metadata_schema.k_sp_src_uuid_columns (in in_b_table_name text, out out_string text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$

<<LMAIN>>

BEGIN

	--формируем список uuid-колонок для последующей их встройки в заполнение В-таблиц, и возвращаем результат в выходной строке out_string через запятую 
	with
	wh_tmplt as
	(
		select   m.src_key_templt_id
				,trim(lower(t.srgt_key_map_tbl_name)) as k_table_name
				,trim(lower(t.srgt_key_map_col_name)) as nk_column_name
				,trim(lower(t.src_col_name))          as src_col_name
				,trim(coalesce(t.src_col_convert_func, lower (t.src_col_name))) as src_col_convert_func
				,trim(lower (m.b_col_name)) as src_col_name_alias
				,m.srgt_key_map_col_type as srgt_key_map_col_type
		from metadata_schema.k_src_key_elem t
		join (select src_key_templt_id, max (b_col_name) as b_col_name, lower(trim(srgt_key_map_col_name)) as srgt_key_map_col_name, lower(trim(srgt_key_map_col_type)) as srgt_key_map_col_type from metadata_schema.k_src_key_templt m where srgt_key_elem_role_cd= 10 group by src_key_templt_id, lower(trim(srgt_key_map_col_name)), lower(trim(srgt_key_map_col_type))) m 
		  on m.src_key_templt_id = t.src_key_templt_id
         and m.srgt_key_map_col_name = trim (lower(t.srgt_key_map_col_name))
		where trim(lower(t.src_tbl_name)) = trim(lower(in_b_table_name))
		  and trim (lower (t.srgt_key_map_col_name)) !='info_system_type_cd' 
		  and t.srgt_key_elem_role_cd = 10  -- признак шаблона для uuid
	)
	select
		string_agg('md5( ' || metadata_schema.md5_concat_string(column_value_list, column_name_list, column_type_list) || ' )::uuid as ' || src_col_name_alias, ', '||chr(13)||chr(10) order by src_col_name_alias) 
		into out_string
	from (
		select
			 t.k_table_name as table_name
			,coalesce(max(t.src_col_name_alias), '_uuid_' || t.k_table_name || '_' || t.src_key_templt_id ) as src_col_name_alias
			,array_agg(t.src_col_convert_func) as column_value_list
			,array_agg(t.nk_column_name) as column_name_list
			,array_agg(trim(lower(t.srgt_key_map_col_type))) as column_type_list
		from wh_tmplt t
		group by
			t.k_table_name,
			t.src_key_templt_id
		) t;

End LMAIN;
$$
;