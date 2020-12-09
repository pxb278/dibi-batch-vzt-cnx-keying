package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class ExtractFromStDsOutToLoadVztDataShareTableHelper {

	private static final Logger log = LoggerFactory.getLogger(ExtractFromStDsOutToLoadVztDataShareTableHelper.class);

	public static void updateVztDataShareRowFromStDsOut(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws SQLException {
		try {

			JdbcCommons.setFromString(row, "EFX_BILL_CNX_ERROR_CODE", preparedStatement, 1);
			JdbcCommons.setFromString(row, "EFX_BILL_CNX_ID", preparedStatement, 2);
			JdbcCommons.setFromString(row, "EFX_BILL_HHLD_ID", preparedStatement, 3);
			JdbcCommons.setFromString(row, "EFX_BILL_ADDR_ID", preparedStatement, 4);
			JdbcCommons.setFromString(row, "EFX_BILL_SOURCE_OF_MATCH", preparedStatement, 5);
			JdbcCommons.setFromString(row, "EFX_BILL_CONF_CD", preparedStatement, 6);
			JdbcCommons.setFromString(row, "EFX_SVC_CNX_ERROR_CODE", preparedStatement, 7);
			JdbcCommons.setFromString(row, "EFX_SVC_CNX_ID", preparedStatement, 8);
			JdbcCommons.setFromString(row, "EFX_SVC_HHLD_ID", preparedStatement, 9);
			JdbcCommons.setFromString(row, "EFX_SVC_ADDR_ID", preparedStatement, 10);
			JdbcCommons.setFromString(row, "EFX_SVC_SOURCE_OF_MATCH", preparedStatement, 11);
			JdbcCommons.setFromString(row, "EFX_SVC_CONF_CD", preparedStatement, 12);
			JdbcCommons.setFromString(row, "EFX_BEST_KEY_SOURCE", preparedStatement, 13);
			preparedStatement.setString(14, processName.get());
			preparedStatement.setTimestamp(15,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "yyyy-MM-dd'T'HH:mm:ss"));
			preparedStatement.setBigDecimal(16, BigDecimal.valueOf(batchId.get()));
			JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 17);
			JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 18);
		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}
	}
}