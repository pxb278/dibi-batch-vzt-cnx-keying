package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;

public class ExtractDataShareSsaConInputHelper {

	private static final Logger log = LoggerFactory.getLogger(ExtractDataShareSsaConInputHelper.class);

	public static Row readExtractDataShareSsaConInputRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(ExtractDataShareSsaConInputSchema.ExtractVzDataShareCnxRepo());

			rowBuilder.addValue(resultSet.getBigDecimal("VZ_DATASHARE_CNX_REPO_ID"));
			rowBuilder.addValue(resultSet.getString("FIRST_NAME"));
			rowBuilder.addValue(resultSet.getString("LAST_NAME"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	public static void setParameterExtractDataShareSsaConInputRow(PreparedStatement preparedStatement,
			ValueProvider<Integer> batchId) throws SQLException {
		try {
			preparedStatement.setLong(1, batchId.get());
		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}
	}

}
