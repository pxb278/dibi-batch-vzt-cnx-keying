package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractDICNX0000002EfxVzwFileSchema {

	public static Schema extractDICNX0000002EfxVzwFile() {
		return Schema.builder()
				.addNullableField("SURROGATE_KEY", FieldType.STRING)
				.addNullableField("CURR_CNX_ID", FieldType.STRING)
				.addNullableField("CLEAN_SSN", FieldType.STRING)
				.addNullableField("CLEAN_DOB", FieldType.STRING)
				.addNullableField("NAME", FieldType.STRING)
				.addNullableField("ADDR1", FieldType.STRING)
				.addNullableField("ADDR2", FieldType.STRING)
				.addNullableField("CITY", FieldType.STRING)
				.addNullableField("STATE", FieldType.STRING)
				.addNullableField("ZIP", FieldType.STRING)
				.addNullableField("COUNTRY_CD", FieldType.STRING)
				.addNullableField("CLEAN_PHONE", FieldType.STRING)
				.addNullableField("ADDR_ID", FieldType.STRING)
				
				.build();

	}

}
