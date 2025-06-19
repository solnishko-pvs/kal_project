create schema metadata_schema;
create schema a_b_layer;

INSERT INTO metadata_schema.k_src_key_elem (src_col_name, src_tbl_name, src_key_id, src_db_name, srgt_key_map_col_name, srgt_key_map_tbl_name, srgt_key_map_db_name, src_col_convert_func, srgt_key_elem_role_cd, src_key_templt_id, natural_key_cd) VALUES
('INFO_SYSTEM_INST_CD', 'B5060000490001_AGR',1,'S_GRNPLM_AS_T_DIDSD_506_DB_STG', 'INFO_SYSTEM_INST_CD', 'K_AGRMNT_NK134','S_GRNPLM_AS_T_DIDSD_506_DB_TMD','INFO_SYSTEM_INST_CD',10,1506001, 'NOKY'),
('agr_sk', 'B5060000490001_AGR',1,'S_GRNPLM_AS_T_DIDSD_506_DB_STG', 'NK134_AGR_CARD_ID', 'K_AGRMNT_NK134','S_GRNPLM_AS_T_DIDSD_506_DB_TMD', 'agr_sk',10,1506001, 'NOKY');

INSERT INTO metadata_schema.k_src_key_templt (src_key_templt_id, srgt_key_elem_role_cd, src_col_name, src_col_convert_func, src_tbl_name, src_db_name, srgt_key_map_col_name, srgt_key_map_tbl_name, srgt_key_map_db_name, stg_col_name, stg_tbl_name,tgt_col_name, tgt_tbl_name, tgt_db_name, "nullable", natural_key_cd, src_key_mult, src_key_add,b_col_name, srgt_key_map_col_type) VALUES
(1506001,10, 'INFO_SYSTEM_INST_CD', 'INFO_SYSTEM_INST_CD', 'B5060000490001_AGR','S_GRNPLM_AS_T_DIDSD_506_DB_STG', 'INFO_SYSTEM_INST_CD','K_AGRMNT_NK134','S_GRNPLM_AS_T_DIDSD_506_DB_TMD','','B5060000490001_AGR', 'AGR_CARD_ID', 'T_AGR_CARD','S_GRNPLM_AS_T_DIDSD_506_DB_DWH', 'Y', 'NK134',100,134,'agr_sk_uid', 'TEXT'),
(1506001,10,'agr_sk', 'agr_sk', 'B5060000490001_AGR','S_GRNPLM_AS_T_DIDSD_506_DB_STG', 'NK134_AGR_CARD_ID', 'K_AGRMNT_NK134','S_GRNPLM_AS_T_DIDSD_506_DB_TMD', '', 'B5060000490001_AGR', 'AGR_CARD_ID', 'T_AGR_CARD', 'S_GRNPLM_AS_T_DIDSD_506_DB_DWH', 'N', 'NK134',100,134, 'agr_sk_uid', 'BIGINT');

