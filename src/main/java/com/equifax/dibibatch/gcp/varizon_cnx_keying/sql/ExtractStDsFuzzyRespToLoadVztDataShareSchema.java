package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractStDsFuzzyRespToLoadVztDataShareSchema {

	public static Schema extractStDsFuzzyRespToLoadVztDataShareFile() {
		return Schema.builder()
				.addNullableField("DATASHARE_ID", FieldType.STRING)
				.addNullableField("B_MATCHED_DS_ID", FieldType.STRING)
				.addNullableField("B_CNX_ID", FieldType.STRING)
				.addNullableField("B_HHLD_ID", FieldType.STRING)
				.addNullableField("B_ADDR_ID", FieldType.STRING)
				.addNullableField("B_CONF_CD", FieldType.STRING)
				.addNullableField("S_MATCHED_DS_ID", FieldType.STRING)
				.addNullableField("S_CNX_ID", FieldType.STRING)
				.addNullableField("S_HHLD_ID", FieldType.STRING)
				.addNullableField("S_ADDR_ID", FieldType.STRING)
				.addNullableField("S_CONF_CD", FieldType.STRING)
				
				.build();
	}

	

}
