CREATE OR REPLACE FUNCTION metadata_schema.md5_concat_string(in in_col_value_list _text, in in_col_name_list _text, in in_col_datatype_list _text, out out_md5_string text)
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
AS $$

<<LMAIN>>

begin

	-- формируем строку для md5 колонок/значений

select '''^''|| ' || string_agg(column_value || ' ||''^''||','' order by column_name ) || ''''''
	into out_md5_string
		from (
			select
				column_name
			   ,'rtrim(lower( ' ||
				case when lower (column_type) in ('date', 'timestamp', 'numeric')
					 then 'to_char ('
					 else ''
				end ||
					 coalesce (null||'.','') || trim (column_value) ||
				case when lower (column_type) in ('numeric')
					 then ', ''FM999999999999999999999999999999999999999D9999999999999999999'''
					 else ''
				end ||
				case when lower (column_type) in ('date')
					 then ', ''YYYYMMDD'''
					 else ''
				end ||
				case when lower (column_type) in ('timestamp')
					 then ', ''YYYYMMDD HH24:MI:SS.US'''
					 else ''
				end ||
				case when lower (column_type) in ('numeric')
					 then ')::numeric'
					 else ''
				end ||
				case when lower (column_type) in ('date', 'timestamp')
					 then ')::text'
					 else '::text'
				end || ' ) )' as column_value
			from (select unnest (in_col_name_list) as column_name, unnest (in_col_value_list) as column_value, unnest (in_col_datatype_list) as column_type) k
			 ) s;

end lmain;

$$
;