package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class LoadVzDataShareRepoTransformSchema {

	public static Schema extractFromDataSharSsaConInputFile() {
		return Schema.builder()
				.addNullableField("VZ_DS_FK_ID", FieldType.DECIMAL).build();

	}
}
