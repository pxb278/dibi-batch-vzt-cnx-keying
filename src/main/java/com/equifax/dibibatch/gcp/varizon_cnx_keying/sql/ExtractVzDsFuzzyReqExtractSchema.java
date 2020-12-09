package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractVzDsFuzzyReqExtractSchema {

	public static Schema ExtractVzDsFuzzyReqExtract() {
		return Schema.builder()
				.addNullableField("SURROGATE_KEY", FieldType.STRING)
				.addNullableField("FIRST_NAME", FieldType.STRING)
				.addNullableField("MIDDLE_NAME", FieldType.STRING)
				.addNullableField("LAST_NAME", FieldType.STRING)
				.addNullableField("ADDRESS", FieldType.STRING)
				.addNullableField("CITY", FieldType.STRING)
				.addNullableField("STATE", FieldType.STRING)
				.addNullableField("ZIP", FieldType.STRING)
				.build();
	}
	
	
	public static Schema ExtractVzDsFuzzyReqExtractToDecrypt() {
		return Schema.builder()
				.addNullableField("SURROGATE_KEY", FieldType.STRING)
				.addNullableField("FIRST_NAME", FieldType.STRING)
				.addNullableField("MIDDLE_NAME", FieldType.STRING)
				.addNullableField("LAST_NAME", FieldType.STRING)
				.addNullableField("BILL_STREET_NO", FieldType.STRING)
				.addNullableField("BILL_STREET_NAME", FieldType.STRING)
				.addNullableField("BILL_CITY", FieldType.STRING)
				.addNullableField("BILL_STATE", FieldType.STRING)
				.addNullableField("BILL_ZIP", FieldType.STRING)
				.addNullableField("SVC_STREET_NO", FieldType.STRING)
				.addNullableField("SVC_STREET_NAME", FieldType.STRING)
				.addNullableField("SVC_CITY", FieldType.STRING)
				.addNullableField("SVC_STATE", FieldType.STRING)
				.addNullableField("SVC_ZIP", FieldType.STRING)
				.build();
	}
	

}
