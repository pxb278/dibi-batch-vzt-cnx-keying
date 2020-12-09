package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.beam.sdk.values.Row;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.MonthlyCnxBatchSalesSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;

public class MonthlyCnxBatchSalesTrackHelper {
	public static Row rowBuilderMonthlyCnxBatchSales(ResultSet resultSet) throws Exception {
		Row.Builder rowBuilder = null;
		rowBuilder = Row.withSchema(MonthlyCnxBatchSalesSchema.selectMonthlyCnxBatchSalesTrack());
		rowBuilder.addValue(resultSet.getString("YEAR"));
		rowBuilder.addValue(resultSet.getString("CURR_MONTH"));
		rowBuilder.addValue(resultSet.getString("VZ_CNX_Offline_MONTHLY_COUNT"));
		rowBuilder.addValue(resultSet.getString("RUNNING_SUM"));

		return rowBuilder.build();
	}
	
	public static Map<String, PGPEncryptFileIO.FieldFn<Row>> getMonthyCnxBatchSalesConfig() {
		Map<String, PGPEncryptFileIO.FieldFn<Row>> config = new LinkedHashMap<>();
		config.put("YEAR", (c) -> c.element().getString("YEAR"));
		config.put("CURR_MONTH", (c) -> c.element().getString("CURR_MONTH"));
		config.put("VZ_CNX_Offline_MONTHLY_COUNT", (c) -> c.element().getString("VZ_CNX_Offline_MONTHLY_COUNT"));
		config.put("RUNNING_SUM", (c) -> c.element().getString("RUNNING_SUM"));
		return config;
	}
	
	public static Map<String, PGPEncryptFileIO.FieldFn<String>> getMonthyCnxBatchSalesSummaryUpdate() {
		Map<String, PGPEncryptFileIO.FieldFn<String>> config = new LinkedHashMap<>();
		config.put("", (c) -> c.element());
		return config;
	}
}

