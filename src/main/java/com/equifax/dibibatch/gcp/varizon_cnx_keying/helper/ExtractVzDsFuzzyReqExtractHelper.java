package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractVzDsFuzzyReqExtractSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;

public class ExtractVzDsFuzzyReqExtractHelper {
	private static final Logger log = LoggerFactory.getLogger(ExtractVzDsFuzzyReqExtractHelper.class);

	public static Row readExtractVzDsFuzzyReqExtractRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtractToDecrypt());

			rowBuilder.addValue(resultSet.getString("SURROGATE_KEY"));
			rowBuilder.addValue(resultSet.getString("FIRST_NAME"));
			rowBuilder.addValue(resultSet.getString("MIDDLE_NAME"));
			rowBuilder.addValue(resultSet.getString("LAST_NAME"));
			rowBuilder.addValue(resultSet.getString("BILL_STREET_NO"));
			rowBuilder.addValue(resultSet.getString("BILL_STREET_NAME"));
			rowBuilder.addValue(resultSet.getString("BILL_CITY"));
			rowBuilder.addValue(resultSet.getString("BILL_STATE"));
			rowBuilder.addValue(resultSet.getString("BILL_ZIP"));
			rowBuilder.addValue(resultSet.getString("SVC_STREET_NO"));
			rowBuilder.addValue(resultSet.getString("SVC_STREET_NAME"));
			rowBuilder.addValue(resultSet.getString("SVC_CITY"));
			rowBuilder.addValue(resultSet.getString("SVC_STATE"));
			rowBuilder.addValue(resultSet.getString("SVC_ZIP"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void setExtractVzDsFuzzyReqExtractRow(PreparedStatement preparedStatement,
			ValueProvider<Integer> batchId) throws SQLException {

		try {

			preparedStatement.setLong(1, batchId.get());

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

}
