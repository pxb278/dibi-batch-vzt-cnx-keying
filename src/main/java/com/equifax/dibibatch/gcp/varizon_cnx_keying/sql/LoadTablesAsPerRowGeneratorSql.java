package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class LoadTablesAsPerRowGeneratorSql {
	public static final String INSERT_INTO_VZT_BATCH = "INSERT INTO {{1}}.VZT_BATCH (BATCH_ID, ULTIMATE_BATCH_ID, START_DT, BATCH_NAME, FAIL_CNT, FILE_NAME, END_DT, REC_CNT, COMMENTS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
	public static final String UPDATE_VZT_SUMMARY = "UPDATE {{1}}.VZT_BATCH_SUMMARY SET VZT_OUTPUT_FILE_BATCH_ID = ?, VZT_OUTPUT_FILE_NAME = ?, VZT_OUTPUT_FILE_EXTRACT_CNT = ?, VZT_OUTPUT_FILE_EXTRACT_DT = ? WHERE (ULTIMATE_BATCH_ID = ?)";
	public static final String INSERT_VZT_SUMMARY = "INSERT INTO {{1}}.VZT_BATCH_SUMMARY (ULTIMATE_BATCH_ID, VZT_OUTPUT_FILE_BATCH_ID, VZT_OUTPUT_FILE_NAME, VZT_OUTPUT_FILE_EXTRACT_CNT, VZT_OUTPUT_FILE_EXTRACT_DT) VALUES (?, ?, ?, ?, ?)";
	public static final String UPDATE_VZT_STAT = "UPDATE {{1}}.VZT_BATCH_STATS SET HIT_CNT = ?, NOHIT_CNT = ? WHERE ULTIMATE_BATCH_ID = ?";
	public static final String UPSERT_VZT_SUMMARY = "INSERT INTO {{1}}.VZT_BATCH_SUMMARY (ULTIMATE_BATCH_ID, VZT_OUTPUT_FILE_BATCH_ID, VZT_OUTPUT_FILE_NAME, VZT_OUTPUT_FILE_EXTRACT_CNT, VZT_OUTPUT_FILE_EXTRACT_DT) VALUES (?, ?, ?, ?, ?) ON CONFLICT (ULTIMATE_BATCH_ID) DO UPDATE SET VZT_OUTPUT_FILE_BATCH_ID = EXCLUDED.VZT_OUTPUT_FILE_BATCH_ID, VZT_OUTPUT_FILE_NAME =EXCLUDED.VZT_OUTPUT_FILE_NAME, VZT_OUTPUT_FILE_EXTRACT_CNT =EXCLUDED.VZT_OUTPUT_FILE_EXTRACT_CNT, VZT_OUTPUT_FILE_EXTRACT_DT = EXCLUDED.VZT_OUTPUT_FILE_EXTRACT_DT";
	public static final String SELECT_VZT_SUMMARY = "SELECT ULTIMATE_BATCH_ID, VZT_OUTPUT_FILE_BATCH_ID, VZT_OUTPUT_FILE_NAME,VZT_OUTPUT_FILE_EXTRACT_CNT,VZT_OUTPUT_FILE_EXTRACT_DT FROM {{1}}.VZT_BATCH_SUMMARY";
	public static final String SELECT_FROM_VZT_BATCH = "SELECT  BATCH_ID, ULTIMATE_BATCH_ID, START_DT, BATCH_NAME, FAIL_CNT, FILE_NAME, END_DT, REC_CNT, COMMENTS FROM {{1}}.VZT_BATCH";
	public static final String SELECT_DUMMY_ROWS = "SELECT ULTIMATE_BATCH_ID, VZT_OUTPUT_FILE_BATCH_ID, VZT_OUTPUT_FILE_NAME,VZT_OUTPUT_FILE_EXTRACT_CNT,VZT_OUTPUT_FILE_EXTRACT_DT FROM {{1}}.VZT_BATCH_SUMMARY LIMIT 1";
	
}