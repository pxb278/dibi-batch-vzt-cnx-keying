package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

public class MonthlyCnxBatchSalesSql {
	
	public static final String SELECT_VW_MNTHLY_CNXBTCH_SALES_TRACK = "SELECT CURR_YEAR as YEAR, CURR_MONTH,CAST(VZ_CNX_Offline_MONTHLY_COUNT AS TEXT),CAST(RUNNING_SUM AS TEXT) FROM {{1}}.VW_MNTHLY_CNXBTCH_SALES_TRACK order by YEAR asc";

}
