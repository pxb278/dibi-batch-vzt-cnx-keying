package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class ExtractStDsFuzzyRespToLoadVztDataShareHelper {
	private static final Logger log = LoggerFactory.getLogger(ExtractStDsFuzzyRespToLoadVztDataShareHelper.class);

	public static void updateVztDataShareRowFromStDsOut(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp, ValueProvider<Integer> batchId, ValueProvider<String> processName)
			throws SQLException {
		if (null != row.getString("B_CONF_CD") || null != row.getString("S_CONF_CD")) {
			try {
				JdbcCommons.setFromString(row, "B_MATCHED_DS_ID", preparedStatement, 1);
				JdbcCommons.setFromString(row, "B_MATCHED_DS_ID", preparedStatement, 2);
				JdbcCommons.setFromString(row, "B_CNX_ID", preparedStatement, 3);
				JdbcCommons.setFromString(row, "B_CNX_ID", preparedStatement, 4);
				JdbcCommons.setFromString(row, "B_CNX_ID", preparedStatement, 5);
				JdbcCommons.setFromString(row, "B_HHLD_ID", preparedStatement, 6);
				JdbcCommons.setFromString(row, "B_CNX_ID", preparedStatement, 7);
				JdbcCommons.setFromString(row, "B_ADDR_ID", preparedStatement, 8);
				JdbcCommons.setFromString(row, "B_CONF_CD", preparedStatement, 9);
				JdbcCommons.setFromString(row, "B_CONF_CD", preparedStatement, 10);
				JdbcCommons.setFromString(row, "B_CONF_CD", preparedStatement, 11);
				JdbcCommons.setFromString(row, "B_CNX_ID", preparedStatement, 12);
				JdbcCommons.setFromString(row, "S_MATCHED_DS_ID", preparedStatement, 13);
				JdbcCommons.setFromString(row, "S_MATCHED_DS_ID", preparedStatement, 14);
				JdbcCommons.setFromString(row, "S_CNX_ID", preparedStatement, 15);
				JdbcCommons.setFromString(row, "S_CNX_ID", preparedStatement, 16);
				JdbcCommons.setFromString(row, "S_CNX_ID", preparedStatement, 17);
				JdbcCommons.setFromString(row, "S_HHLD_ID", preparedStatement, 18);
				JdbcCommons.setFromString(row, "S_CNX_ID", preparedStatement, 19);
				JdbcCommons.setFromString(row, "S_ADDR_ID", preparedStatement, 20);
				JdbcCommons.setFromString(row, "S_CONF_CD", preparedStatement, 21);
				JdbcCommons.setFromString(row, "S_CONF_CD", preparedStatement, 22);
				JdbcCommons.setFromString(row, "S_CONF_CD", preparedStatement, 23);
				JdbcCommons.setFromString(row, "S_CNX_ID", preparedStatement, 24);
				JdbcCommons.setFromString(row, "B_CONF_CD", preparedStatement, 25);
				JdbcCommons.setFromString(row, "S_CONF_CD", preparedStatement, 26);
				preparedStatement.setBigDecimal(27, BigDecimal.valueOf(batchId.get()));
				preparedStatement.setString(28, processName.get());
				JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 29);

			} catch (Exception ex) {

				log.error("Exception occurred: " + ex.getMessage(), ex);

				throw ex;
			}
		}
	}
}
