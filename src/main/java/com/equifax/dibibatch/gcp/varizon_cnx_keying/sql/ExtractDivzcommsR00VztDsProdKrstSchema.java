package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractDivzcommsR00VztDsProdKrstSchema {

	public static Schema ExtractVzDsFuzzyReqExtract() {
		return Schema.builder()
				.addNullableField("UNIQUE_VZT_ID", FieldType.STRING)
				.addNullableField("INDIVIDUAL_ID", FieldType.STRING)
				.addNullableField("HOUSEHOLD_ID", FieldType.STRING)
				.addNullableField("ADDRESS_ID", FieldType.STRING)
				.addNullableField("ERROR_CODE", FieldType.STRING)
				.addNullableField("BEST_ADDR_FLAG", FieldType.STRING)
				.addNullableField("CONFIDENCELEVEL", FieldType.STRING)
				.build();
	}

}
