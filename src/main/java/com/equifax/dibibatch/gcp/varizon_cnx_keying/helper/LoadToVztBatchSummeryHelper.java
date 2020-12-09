package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchAndVztSummarySchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class LoadToVztBatchSummeryHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadToVztBatchSummeryHelper.class);

	public static Row readForLoadVztBatchSummeryRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(LoadVztBatchAndVztSummarySchema.readForRowsToLoadVztBatchSummerytables());

			rowBuilder.addValue(resultSet.getBigDecimal("LOAD_CNT"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void insertIntoVztBatch(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId,
			ValueProvider<String> vztCnxRespFile, ValueProvider<Integer> vztCnxRespFileCnt,
			ValueProvider<Integer> ultimateBatchId) throws SQLException {
		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setTimestamp(3,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setString(4, "VZT DATASHARE CNX RESP LOAD");

			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 5);
			preparedStatement.setBigDecimal(6, null);

			preparedStatement.setString(7, vztCnxRespFile.get());
			preparedStatement.setTimestamp(8,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setBigDecimal(9, BigDecimal.valueOf(vztCnxRespFileCnt.get()));
			preparedStatement.setString(10, "Processed DataShare Cnx Key Response File");

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void insertIntoVztDataSummary(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId,
			ValueProvider<String> vztCnxRespFile, ValueProvider<Integer> vztCnxRespFileCnt,
			ValueProvider<Integer> ultimateBatchId) throws SQLException {
		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setString(3, vztCnxRespFile.get());
			preparedStatement.setBigDecimal(4, BigDecimal.valueOf(vztCnxRespFileCnt.get()));
			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 5);
			preparedStatement.setTimestamp(6,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void updateVztDataSummary(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId,
			ValueProvider<String> vztCnxRespFile, ValueProvider<Integer> vztCnxRespFileCnt,
			ValueProvider<Integer> ultimateBatchId) throws SQLException {
		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setString(2, vztCnxRespFile.get());
			preparedStatement.setBigDecimal(3, BigDecimal.valueOf(vztCnxRespFileCnt.get()));
			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 4);
			preparedStatement.setTimestamp(5,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setBigDecimal(6, BigDecimal.valueOf(ultimateBatchId.get()));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void setParameterLoadVztBatchSummeryStatsRow(PreparedStatement preparedStatement,
			ValueProvider<Integer> batchId) throws SQLException {

		try {

			preparedStatement.setLong(1, batchId.get());

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

}
