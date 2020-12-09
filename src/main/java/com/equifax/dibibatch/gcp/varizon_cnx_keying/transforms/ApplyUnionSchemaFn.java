package com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractVzDsFuzzyReqExtractSchema;

public class ApplyUnionSchemaFn  extends DoFn<Row, Row> {

	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) {

		Row inputRow = c.element();
		Row.Builder outputRowBuilder = Row
				.withSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtract());

		outputRowBuilder.addValue(inputRow.getString("SURROGATE_KEY"));
		outputRowBuilder.addValue(inputRow.getString("FIRST_NAME"));
		outputRowBuilder.addValue(inputRow.getString("MIDDLE_NAME"));
		outputRowBuilder.addValue(inputRow.getString("LAST_NAME"));
		outputRowBuilder.addValue(inputRow.getString("ADDRESS"));
		outputRowBuilder.addValue(inputRow.getString("CITY"));
		outputRowBuilder.addValue(inputRow.getString("STATE"));
		outputRowBuilder.addValue(inputRow.getString("ZIP"));
		c.output(outputRowBuilder.build());
		
		
	}
}