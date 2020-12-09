package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDivzcommsR00VztDsProdKrstSchema;

public class ExtractDivzcommsR00VztDsProdKrsHelper {
	private static final Logger log = LoggerFactory.getLogger(ExtractDivzcommsR00VztDsProdKrsHelper.class);

	public static Row readExtractDivzcommsR00VztDsProdKrsRow(ResultSet resultSet) throws Exception {

		Row.Builder rowBuilder = null;

		try {

			rowBuilder = Row.withSchema(ExtractDivzcommsR00VztDsProdKrstSchema.ExtractVzDsFuzzyReqExtract());

			rowBuilder.addValue(resultSet.getString("DATASHARE_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_CNX_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_HHLD_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_ADDR_ID"));
			rowBuilder.addValue(resultSet.getString("EFX_ERROR_CODE"));
			rowBuilder.addValue(resultSet.getString("EFX_BEST_KEY_SOURCE"));
			rowBuilder.addValue(resultSet.getString("EFX_CONF_CD"));

		} catch (Exception ex) {

			log.error("Exception occurred: " + ex.getMessage(), ex);

			throw ex;
		}

		return rowBuilder.build();
	}

	

	public static void setParamExtractDivzcommsR00VztDsProdKrsRow(PreparedStatement preparedStatement,
			ValueProvider<Integer> batchId)throws SQLException {

		try {

			preparedStatement.setLong(1, batchId.get());

		} catch (SQLException ex) {

			log.error("SQLException occurred: " + ex.getMessage(), ex);

			throw ex;
		}

	}

}
