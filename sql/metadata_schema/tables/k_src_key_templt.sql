CREATE TABLE metadata_schema.k_src_key_templt (
	src_key_templt_id int4 NOT NULL,
	srgt_key_elem_role_cd int2 NOT NULL,
	src_col_name text NOT NULL,
	src_col_convert_func text NOT NULL,
	src_tbl_name text NOT NULL,
	src_db_name text NOT NULL,
	srgt_key_map_col_name text NOT NULL,
	srgt_key_map_tbl_name text NOT NULL,
	srgt_key_map_db_name text NOT NULL,
	stg_col_name text NOT NULL,
	stg_tbl_name text NOT NULL,
	tgt_col_name text NOT NULL,
	tgt_tbl_name text NOT NULL,
	tgt_db_name text NOT NULL,
	"nullable" text DEFAULT 'Y'::text NOT NULL,
	natural_key_cd text DEFAULT 'NOKY':: text NOT NULL,
	src_key_mult int4 DEFAULT 1 NOT NULL,
	src_key_add int4 DEFAULT 0 NOT NULL,
	b_col_name text NULL,
	srgt_key_map_col_type text NULL
)
;