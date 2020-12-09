package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class RequestResponseFileMonitorSql {

	public static final String SELECT_REQUEST_FILE_DATE_DIFF = "select DATE_PART('day', LOCALTIMESTAMP - VZT_OUTPUT_FILE_EXTRACT_DT) DATEDIFF "
			+ "from {{1}}.vzt_batch_summary "
			+ "where ultimate_batch_id ="
			+ "(SELECT t.ultimate_batch_id "
			+ "FROM {{1}}.vzt_batch_summary t,"
			+ "(SELECT MAX(vzt_input_file_load_dt) as max_date "
			+ "FROM {{1}}.vzt_batch_summary "
			+ "WHERE VZT_INPUT_FILE_NAME LIKE 'VZT_DS_PROD.KREQ%') a "
			+ "WHERE vzt_input_file_load_dt = a.max_date)";
	
	public static final String SELECT_RESPONSE_FILE_BATCH_ID_FILE_NAME = "SELECT "
			+ "ULTIMATE_BATCH_ID,"
			+ "CNX_INPUT_FILE_NAME "
			+ "FROM {{1}}.VZT_BATCH_SUMMARY "
			+ "WHERE TO_CHAR(VZT_INPUT_FILE_LOAD_DT,'YYYYMMDD')< TO_CHAR(LOCALTIMESTAMP,'YYYYMMDD') "
			+ "and VZT_OUTPUT_FILE_NAME IS NULL and CNX_INPUT_FILE_NAME like 'DICNX0000002-efx_vzw%'";

}
