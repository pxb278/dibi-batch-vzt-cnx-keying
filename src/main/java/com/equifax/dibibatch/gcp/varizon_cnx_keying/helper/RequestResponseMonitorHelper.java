package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.ResultSet;

import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.RequestResponseMonitorSchema;

public class RequestResponseMonitorHelper {

	private static final Logger log = LoggerFactory.getLogger(RequestResponseMonitorHelper.class);
	
	public static Double rowBuilderVztBatchSummaryMonitor(ResultSet resultSet) throws Exception {

		Double dateDiff = null;

		try {

			dateDiff = resultSet.getDouble("DATEDIFF");

		} catch (Exception ex) {

			log.error("Execpetion occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return dateDiff;
	}

	public static Row rowBuilderVztBatchSummaryResponseMonitor(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(RequestResponseMonitorSchema.schemaBatchResponseFileMonitor());
			
			rowBuilder.addValue(resultSet.getInt("ULTIMATE_BATCH_ID"));
			rowBuilder.addValue(resultSet.getString("CNX_INPUT_FILE_NAME"));

		} catch (Exception ex) {

			log.error("Execpetion occurred: " + ex.getMessage(), ex);

			throw ex;
		}
		
		return rowBuilder.build();
	}

}
