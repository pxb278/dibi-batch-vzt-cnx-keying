package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class ExtractFromStDsOutToLoadVztDataShareFileSchema {

	public static Schema extractFromStDsOutToLoadVztDataShareFile() {
		return Schema.builder()
				.addNullableField("DATASHARE_ID", FieldType.STRING)
				.addNullableField("EFX_BILL_CNX_ERROR_CODE", FieldType.STRING)
				.addNullableField("EFX_BILL_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_BILL_HHLD_ID", FieldType.STRING)
				.addNullableField("EFX_BILL_ADDR_ID", FieldType.STRING)
				.addNullableField("EFX_BILL_SOURCE_OF_MATCH", FieldType.STRING)
				.addNullableField("EFX_BILL_CONF_CD", FieldType.STRING)
				.addNullableField("EFX_SVC_CNX_ERROR_CODE", FieldType.STRING)
				.addNullableField("EFX_SVC_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_SVC_HHLD_ID", FieldType.STRING)
				.addNullableField("EFX_SVC_ADDR_ID", FieldType.STRING)
				.addNullableField("EFX_SVC_SOURCE_OF_MATCH", FieldType.STRING)
				.addNullableField("EFX_SVC_CONF_CD", FieldType.STRING)
				.addNullableField("EFX_BEST_KEY_SOURCE", FieldType.STRING)
							
				.build();

	}

}
