package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline.LoadTablesAsPerRowGeneratorPipeLine;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadTablesAsPerRowGeneratorSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.utils.ObjectUtils;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class LoadTablesAsPerRowGeneratorHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadTablesAsPerRowGeneratorHelper.class);

	public static void insertVztBatch(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> ultimateBatchId,
			ValueProvider<Integer> batchId, ValueProvider<Integer> vztDSRespFileCount,
			ValueProvider<String> vztDSRespFileName) throws SQLException {

		try {

			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setTimestamp(3,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
			preparedStatement.setString(4, "VZT DATASHARE RESPONSE FILE PROCESS");
			preparedStatement.setLong(5, ObjectUtils.nullToZero(ObjectUtil.convertToLong(null)));
			preparedStatement.setString(6, vztDSRespFileName.get());
			preparedStatement.setTimestamp(7,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
			preparedStatement.setBigDecimal(8, BigDecimal.valueOf(vztDSRespFileCount.get()));
			preparedStatement.setString(9, "Sent VZ Response File");
		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void insertVztBatchSummary(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> ultimateBatchId,
			ValueProvider<Integer> batchId, ValueProvider<String> vztDSRespFileName,
			ValueProvider<Integer> vztDSRespFileCount) throws SQLException {

		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(ultimateBatchId.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setString(3, vztDSRespFileName.get());
			preparedStatement.setBigDecimal(4, BigDecimal.valueOf(vztDSRespFileCount.get()));
			preparedStatement.setTimestamp(5,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void updateVztBatchSummary(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> ultimateBatchId,
			ValueProvider<Integer> batchId, ValueProvider<String> vztDSRespFileName,
			ValueProvider<Integer> vztDSRespFileCount) throws SQLException {

		LoadTablesAsPerRowGeneratorPipeLine.isUpdated = true;
		try {

			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(batchId.get()));
			preparedStatement.setString(2, vztDSRespFileName.get());
			preparedStatement.setBigDecimal(3, BigDecimal.valueOf(vztDSRespFileCount.get()));
			preparedStatement.setTimestamp(4,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
			preparedStatement.setBigDecimal(5, BigDecimal.valueOf(ultimateBatchId.get()));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static void updateVztStat(Row row, PreparedStatement preparedStatement,
			ValueProvider<Integer> ultimatebatchId, ValueProvider<Integer> vztHitCount,
			ValueProvider<Integer> vztNoHitCount) throws SQLException {

		try {

			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(vztHitCount.get()));
			preparedStatement.setBigDecimal(2, BigDecimal.valueOf(vztNoHitCount.get()));
			preparedStatement.setBigDecimal(3, BigDecimal.valueOf(ultimatebatchId.get()));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static Row readForLoadVztBatchSummeryRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {
			rowBuilder = Row.withSchema(LoadTablesAsPerRowGeneratorSchema.vztBatchSummary());

			rowBuilder.addValue(resultSet.getBigDecimal("ULTIMATE_BATCH_ID"));
			rowBuilder.addValue(resultSet.getBigDecimal("VZT_OUTPUT_FILE_BATCH_ID"));
			rowBuilder.addValue(resultSet.getString("VZT_OUTPUT_FILE_NAME"));
			rowBuilder.addValue(resultSet.getBigDecimal("VZT_OUTPUT_FILE_EXTRACT_CNT"));
			rowBuilder.addValue(ObjectUtil.convertToRowDateTime(resultSet.getTimestamp("VZT_OUTPUT_FILE_EXTRACT_DT")));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void setParameterLoadVztBatchSummeryRow(PreparedStatement preparedStatement,
			ValueProvider<Integer> ultimateBatchId) throws SQLException {

		try {
			preparedStatement.setBigDecimal(1, BigDecimal.valueOf(ultimateBatchId.get()));

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static Row readVztBatchRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(LoadTablesAsPerRowGeneratorSchema.vztBatchSchema());

			rowBuilder.addValue(resultSet.getBigDecimal("BATCH_ID"));
			rowBuilder.addValue(resultSet.getBigDecimal("ULTIMATE_BATCH_ID"));
			rowBuilder.addValue(ObjectUtil.convertToRowDateTime(resultSet.getTimestamp("START_DT")));
			rowBuilder.addValue(resultSet.getString("BATCH_NAME"));
			rowBuilder.addValue(resultSet.getBigDecimal("FAIL_CNT"));
			rowBuilder.addValue(resultSet.getString("FILE_NAME"));
			rowBuilder.addValue(ObjectUtil.convertToRowDateTime(resultSet.getTimestamp("END_DT")));
			rowBuilder.addValue(resultSet.getBigDecimal("REC_CNT"));
			rowBuilder.addValue(resultSet.getString("COMMENTS"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}
}
