package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;

public class LoadVzDataShareRepoHelper {

	private static final Logger log = LoggerFactory.getLogger(LoadVzDataShareRepoHelper.class);

	public static void deleteVzDataShareRepoTableRow(Row row, PreparedStatement preparedStatement) throws SQLException {
		try {

			JdbcCommons.setFromDecimal(row, "VZ_DS_FK_ID", preparedStatement, 1);

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

}
