package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class LoadVztDataShareCnxRepoFromVwVztDataShareSchema {

	public static Schema LoadVztDataShareCnxRepo() {
		return Schema.builder()
				.addNullableField("DATASHARE_ID", FieldType.STRING)
				.addNullableField("FIRST_NAME", FieldType.STRING)
				.addNullableField("MIDDLE_NAME", FieldType.STRING)
				.addNullableField("LAST_NAME", FieldType.STRING)
				.addNullableField("ACCOUNT_NO", FieldType.STRING)
				//.addNullableField("EFX_ADDR", FieldType.STRING)
				//.addNullableField("EFX_CITY", FieldType.STRING)
				//.addNullableField("EFX_STATE", FieldType.STRING)
				//.addNullableField("EFX_ZIP", FieldType.STRING)
				.addNullableField("EFX_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_HHLD_ID", FieldType.STRING)
				.addNullableField("EFX_ADDR_ID", FieldType.STRING)
				.addNullableField("EFX_SOURCE_OF_MATCH", FieldType.STRING)
				.addNullableField("EFX_BEST_KEY_SOURCE", FieldType.STRING)
				.addNullableField("EFX_CNX_MODIFY_DT", FieldType.DATETIME)
				.addNullableField("EFX_CONF_CD", FieldType.STRING)
				.build();
	}

	public static Schema LoadVztDataShareCnxRepoToInsertUnProcessedRows() {
		return Schema.builder()
				.addNullableField("DATASHARE_ID", FieldType.STRING)
				.addNullableField("FIRST_NAME", FieldType.STRING)
				.addNullableField("MIDDLE_NAME", FieldType.STRING)
				.addNullableField("LAST_NAME", FieldType.STRING)
				.addNullableField("ACCOUNT_NO", FieldType.STRING)
				//.addNullableField("EFX_ADDR", FieldType.STRING)
				//.addNullableField("EFX_CITY", FieldType.STRING)
				//.addNullableField("EFX_STATE", FieldType.STRING)
				//.addNullableField("EFX_ZIP", FieldType.STRING)
				.addNullableField("EFX_CNX_ID", FieldType.STRING)
				.addNullableField("EFX_HHLD_ID", FieldType.STRING)
				.addNullableField("EFX_ADDR_ID", FieldType.STRING)
				.addNullableField("EFX_SOURCE_OF_MATCH", FieldType.STRING)
				.addNullableField("EFX_BEST_KEY_SOURCE", FieldType.STRING)
				.addNullableField("EFX_CNX_MODIFY_DT", FieldType.DATETIME)
				.addNullableField("EFX_CONF_CD", FieldType.STRING)
				.addNullableField("EFX_CREATE_DT", FieldType.DATETIME)
				.addNullableField("EFX_CREATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("EFX_MNODIFY_DT", FieldType.DATETIME)
				.addNullableField("EFX_MODIFY_BATCH_ID", FieldType.DECIMAL)
				.build();
	}

}
