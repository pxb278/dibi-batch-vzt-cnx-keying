package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.CalendarUtil;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class LoadFromInactiveOvrerrideRecordsHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadFromInactiveOvrerrideRecordsHelper.class);

	public static void updateVztDataShareRow(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp) throws Exception {

		try {

			preparedStatement.setString(1, "Inactive");
			JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 2);

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}
	}

	public static void updateConnexusKeyOverrideRow(Row row, PreparedStatement preparedStatement,
			ValueProvider<String> currentTimestamp) throws Exception {

		try {

			preparedStatement.setString(1, "CNX");
			preparedStatement.setString(2, "Inactive");
			preparedStatement.setTimestamp(3,
					CalendarUtil.convertStringToTimestamp(currentTimestamp.get(), "MM-dd-yyyy"));
			JdbcCommons.setFromString(row, "DATASHARE_ID", preparedStatement, 4);
		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}
	}

}
