package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractInactivatedOverrideRecordsSchema {

	public static Schema ExtractInactivatedOverrideRecords() {
		return Schema.builder()
				.addNullableField("DATASHARE_ID", FieldType.STRING)
				.addNullableField("OLD_BEST_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_HHLD_ID", FieldType.STRING)
				.addNullableField("EFX_ADDR_ID", FieldType.STRING)
				.addNullableField("EFX_OVERRIDE_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_OVERRIDE_HHLD_ID", FieldType.STRING)
				.build();
	}

}
