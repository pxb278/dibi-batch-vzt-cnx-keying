package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractDataShareSsaConInputSchema {

	public static Schema ExtractExtractDataShareSsaConInput() {
		return Schema.builder()
				.addNullableField("EFX_UNIQ_ID", FieldType.DECIMAL)
				.addNullableField("EFX_NM", FieldType.STRING)
				//.addNullableField("EFX_ADDR", FieldType.STRING)
				.build();
	}
	
	public static Schema ExtractVzDataShareCnxRepo() {
		return Schema.builder()
				.addNullableField("EFX_UNIQ_ID", FieldType.DECIMAL)
				.addNullableField("FIRST_NAME", FieldType.STRING)
				.addNullableField("LAST_NAME", FieldType.STRING)
				.build();
	}
	public static Schema ExtractExtractDataShareSsaConInputbeforeDecrypt() {
		return Schema.builder()
				.addNullableField("EFX_UNIQ_ID", FieldType.DECIMAL)
				.addNullableField("EFX_NM", FieldType.STRING)
				//.addNullableField("EFX_ADDR", FieldType.STRING)
				.build();
	}

}
