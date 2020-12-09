package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractFromStDsOutToLoadVztDataShareFileSchema;

public class ApplyStDsOutJoinSchemaFn extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) {

		Row inputRow = c.element();
		Row.Builder outputRowBuilder = Row
				.withSchema(ExtractFromStDsOutToLoadVztDataShareFileSchema.extractFromStDsOutToLoadVztDataShareFile());

		outputRowBuilder.addValue(inputRow.getString("DATASHARE_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_CNX_ERROR_CODE"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_CNX_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_HHLD_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_ADDR_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_SOURCE_OF_MATCH"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BILL_CONF_CD"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_CNX_ERROR_CODE"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_CNX_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_HHLD_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_ADDR_ID"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_SOURCE_OF_MATCH"));
		outputRowBuilder.addValue(inputRow.getString("EFX_SVC_CONF_CD"));
		outputRowBuilder.addValue(inputRow.getString("EFX_BEST_KEY_SOURCE"));
		c.output(outputRowBuilder.build());
	}
}
