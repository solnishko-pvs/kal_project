CREATE TABLE metadata_schema.k_src_key_elem ( 
	src_col_name text NOT NULL,
	src_tbl_name text NOT NULL, 
	src_key_id int4 NOT NULL, 
	src_db_name text NOT NULL,
	srgt_key_map_col_name text NOT NULL, srgt_key_map_tbl_name text NOT NULL,
	srgt_key_map_db_name text NOT NULL,
	src_col_convert_func text NULL,
	srgt_key_elem_role_cd int2 NULL,
	src_key_templt_id int4 NULL,
	natural_key_cd text DEFAULT 'NOKY':: text NOT NULL
)
;