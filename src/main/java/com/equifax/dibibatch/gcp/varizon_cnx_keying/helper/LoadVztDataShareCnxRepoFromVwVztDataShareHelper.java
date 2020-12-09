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
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class LoadVztDataShareCnxRepoFromVwVztDataShareHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadVztDataShareCnxRepoFromVwVztDataShareHelper.class);

	public static void setParamLoadVztDataShareCnxRepoFromVwVztDataShareRow(PreparedStatement preparedStatement, ValueProvider<Integer> batchId) throws SQLException {

		try {
			System.out.println("@#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			
			preparedStatement.setLong(1, batchId.get());

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

	public static Row readLoadVztDataShareCnxRepoFromVwVztDataShareRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepo());
System.out.println("@#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
			rowBuilder.addValue(resultSet.getString("DATASHARE_ID"));
			rowBuilder.addValue(resultSet.getString("FIRST_NAME"));
			rowBuilder.addValue(resultSet.getString("MIDDLE_NAME"));
			rowBuilder.addValue(resultSet.getString("LAST_NAME"));
			rowBuilder.addValue(resultSet.getString("ACCOUNT_NO"));
			//rowBuilder.addValue(resultSet.getString("EFX_ADDR"));
			//rowBuilder.addValue(resultSet.getString("EFX_CITY"));
			//rowBuilder.addValue(resultSet.getString("EFX_STATE"));
			//rowBuilder.addValue(resultSet.getString("EFX_ZIP"));
			rowBuilder.addValue(resultSet.getString("EFX_CNX_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_HHLD_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_ADDR_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_SOURCE_OF_MATCH"));
			rowBuilder.addValue(resultSet.getString("EFX_BEST_KEY_SOURCE"));
			rowBuilder.addValue(ObjectUtil.convertToRowDateTime(resultSet.getTimestamp("EFX_CNX_MODIFY_DT")));
			rowBuilder.addValue(resultSet.getString("EFX_CONF_CD"));
		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void updateLoadVztDataShareCnxRepoFromVwVztDataShareRow(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId) throws SQLException {

try {

JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 1);
JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 2);
JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 3);
JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 4);
//JdbcCommons.setFromString(row, "EFX_ADDR", preparedStatement, 5);
//JdbcCommons.setFromString(row, "EFX_CITY", preparedStatement, 6);
//JdbcCommons.setFromString(row, "EFX_STATE", preparedStatement, 7);
//JdbcCommons.setFromString(row, "EFX_ZIP", preparedStatement, 8);
JdbcCommons.setFromString(row, "EFX_CNX_ID", preparedStatement, 5);
JdbcCommons.setFromString(row, "EFX_HHLD_ID", preparedStatement, 6);
JdbcCommons.setFromString(row, "EFX_ADDR_ID", preparedStatement, 7);
JdbcCommons.setFromString(row, "EFX_SOURCE_OF_MATCH", preparedStatement, 8);
JdbcCommons.setFromString(row, "EFX_BEST_KEY_SOURCE", preparedStatement, 9);
JdbcCommons.setFromDateTimeToTimestamp(row, "EFX_CNX_MODIFY_DT", preparedStatement, 10);
JdbcCommons.setFromString(row, "EFX_CONF_CD", preparedStatement, 11);
preparedStatement.setTimestamp(12,
		CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
preparedStatement.setBigDecimal(13, BigDecimal.valueOf(batchId.get()));
JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 14);



} catch (SQLException ex) {

	log.error("SQLException occurred: " + ex.getMessage(), ex);

	throw ex;
}

}

	public static void insertLoadVztDataShareCnxRepoFromVwVztDataShare(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId) throws SQLException {
	try {

JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 1);
JdbcCommons.setFromString(row, "FIRST_NAME", preparedStatement, 2);
JdbcCommons.setFromString(row, "MIDDLE_NAME", preparedStatement, 3);
JdbcCommons.setFromString(row, "LAST_NAME", preparedStatement, 4);
JdbcCommons.setFromString(row, "ACCOUNT_NO", preparedStatement, 5);
//JdbcCommons.setFromString(row, "EFX_ADDR", preparedStatement, 6);
//JdbcCommons.setFromString(row, "EFX_CITY", preparedStatement, 7);
//JdbcCommons.setFromString(row, "EFX_STATE", preparedStatement, 8);
//JdbcCommons.setFromString(row, "EFX_ZIP", preparedStatement, 9);
JdbcCommons.setFromString(row, "EFX_CNX_ID", preparedStatement, 6);
JdbcCommons.setFromString(row, "EFX_HHLD_ID", preparedStatement, 7);
JdbcCommons.setFromString(row, "EFX_ADDR_ID", preparedStatement, 8);
JdbcCommons.setFromString(row, "EFX_SOURCE_OF_MATCH", preparedStatement, 9);
JdbcCommons.setFromString(row, "EFX_BEST_KEY_SOURCE", preparedStatement, 10);
JdbcCommons.setFromDateTimeToTimestamp(row, "EFX_CNX_MODIFY_DT", preparedStatement, 11);
JdbcCommons.setFromString(row, "EFX_CONF_CD", preparedStatement, 12);
preparedStatement.setTimestamp(13,
		CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
preparedStatement.setBigDecimal(14, BigDecimal.valueOf(batchId.get()));
preparedStatement.setTimestamp(15,
		CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
preparedStatement.setBigDecimal(16, BigDecimal.valueOf(batchId.get()));

} catch (SQLException ex) {

	log.error("SQLException occurred: " + ex.getMessage(), ex);

	throw ex;
}

}

}
