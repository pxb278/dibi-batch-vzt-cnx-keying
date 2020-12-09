package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractStDsFuzzyRespToLoadVztDataShareSchema;

public class ApplyJoinSchemaFn extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) {

		Row inputRow = c.element();
		Row.Builder outputRowBuilder = Row
				.withSchema(ExtractStDsFuzzyRespToLoadVztDataShareSchema.extractStDsFuzzyRespToLoadVztDataShareFile());
		outputRowBuilder.addValue(inputRow.getString("DATASHARE_ID"));
		outputRowBuilder.addValue(inputRow.getString("B_MATCHED_DS_ID"));
		outputRowBuilder.addValue(inputRow.getString("B_CNX_ID"));
		outputRowBuilder.addValue(inputRow.getString("B_HHLD_ID"));
		outputRowBuilder.addValue(inputRow.getString("B_ADDR_ID"));
		outputRowBuilder.addValue(inputRow.getString("B_CONF_CD"));
		outputRowBuilder.addValue(inputRow.getString("S_MATCHED_DS_ID"));
		outputRowBuilder.addValue(inputRow.getString("S_CNX_ID"));
		outputRowBuilder.addValue(inputRow.getString("S_HHLD_ID"));
		outputRowBuilder.addValue(inputRow.getString("S_ADDR_ID"));
		outputRowBuilder.addValue(inputRow.getString("S_CONF_CD"));
		c.output(outputRowBuilder.build());
	}
}
