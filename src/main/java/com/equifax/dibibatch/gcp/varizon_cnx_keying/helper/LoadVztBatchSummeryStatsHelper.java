package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class LoadVztBatchSummeryStatsHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadVztBatchSummeryStatsHelper.class);

	public static void setParameterLoadVztBatchSummeryStatsRow(PreparedStatement preparedStatement,
			ValueProvider<String> inputFileDate, ValueProvider<Integer> batchId) throws SQLException {

		try {

			preparedStatement.setString(1, inputFileDate.get());
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(batchId.get()));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static Row readLoadVztBatchSummeryStatsRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(LoadVztBatchSummeryStatsSchema.readRowsToLoadVztBatchSummeryStatstables());

			rowBuilder.addValue(resultSet.getBigDecimal("LOAD_CNT"));
			rowBuilder.addValue(resultSet.getString("INPUT_FILE_DT"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void insertVztBatch(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> srcFileName,
			ValueProvider<Integer> ultimateBatchId) throws SQLException {

		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setTimestamp(3,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setString(4, "VZT DATASHARE REQUEST FILE LOAD");

			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 5);
			preparedStatement.setBigDecimal(6, null);

			preparedStatement.setString(7, srcFileName.get());
			preparedStatement.setTimestamp(8,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 9);
			preparedStatement.setString(10, "Processed DataShare Request file and Sent Cnx Key Request File");
			preparedStatement.setBigDecimal(11, null);
		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void insertVztBatchStats(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId,
			ValueProvider<Integer> ultimateBatchId, ValueProvider<String> srcFileName,
			ValueProvider<Integer> inputFileRecCount, ValueProvider<Integer> vztInsertCnt,
			ValueProvider<Integer> sourceVbbRecordCount, ValueProvider<Integer> sourceVzbRecordCount,
			ValueProvider<Integer> sourceVztRecordCount, ValueProvider<Integer> inputfileLiveCount,
			ValueProvider<Integer> inputfileFinalCount, ValueProvider<Integer> sourceVbwRecordCount)
			throws SQLException {
		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(ultimateBatchId.get()));

			preparedStatement.setString(2, srcFileName.get());
			JdbcCommons.setFromString(row, "INPUT_FILE_DT", preparedStatement, 3);
			preparedStatement.setBigDecimal(4, BigDecimal.valueOf(inputFileRecCount.get()));
			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 5);
			preparedStatement.setBigDecimal(6, BigDecimal.valueOf(inputFileRecCount.get()));
			preparedStatement.setBigDecimal(7, BigDecimal.valueOf(vztInsertCnt.get()));
			preparedStatement.setBigDecimal(8, BigDecimal.valueOf(vztInsertCnt.get()));
			preparedStatement.setBigDecimal(9, BigDecimal.valueOf(sourceVztRecordCount.get()));
			preparedStatement.setBigDecimal(10, BigDecimal.valueOf(sourceVbwRecordCount.get()));
			preparedStatement.setBigDecimal(11, BigDecimal.valueOf(sourceVzbRecordCount.get()));
			preparedStatement.setBigDecimal(12, BigDecimal.valueOf(sourceVbbRecordCount.get()));
			preparedStatement.setBigDecimal(13, BigDecimal.valueOf(inputfileLiveCount.get()));
			preparedStatement.setBigDecimal(14, BigDecimal.valueOf(inputfileFinalCount.get()));
			preparedStatement.setBigDecimal(15, null);
			preparedStatement.setBigDecimal(16, null);

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void insertVztBatchSummary(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId,
			ValueProvider<Integer> ultimateBatchId, ValueProvider<String> srcFileName,
			ValueProvider<Integer> inputFileRecCount, ValueProvider<Integer> keyingFlagValueF,
			ValueProvider<Integer> keyingFlagValueT, ValueProvider<Integer> CNXExtractCount,
			ValueProvider<String> vztCnxReqFile) throws SQLException {

		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setString(3, srcFileName.get());
			JdbcCommons.setFromString(row, "INPUT_FILE_DT", preparedStatement, 4);
			preparedStatement.setBigDecimal(5, BigDecimal.valueOf(inputFileRecCount.get()));
			JdbcCommons.setFromDecimal(row, "LOAD_CNT", preparedStatement, 6);
			preparedStatement.setBigDecimal(7, BigDecimal.valueOf(inputFileRecCount.get()));
			preparedStatement.setBigDecimal(8, BigDecimal.valueOf(keyingFlagValueT.get()));
			preparedStatement.setBigDecimal(9, BigDecimal.valueOf(keyingFlagValueF.get()));
			preparedStatement.setTimestamp(10,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setBigDecimal(11, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setString(12, vztCnxReqFile.get());
			preparedStatement.setBigDecimal(13, BigDecimal.valueOf(CNXExtractCount.get()));
			preparedStatement.setTimestamp(14,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

}
