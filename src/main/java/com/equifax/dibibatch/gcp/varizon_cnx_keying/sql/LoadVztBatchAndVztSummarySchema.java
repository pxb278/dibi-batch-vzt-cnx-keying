package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class LoadVztBatchAndVztSummarySchema {
	
	public static Schema readForRowsToLoadVztBatchSummerytables() {
		return Schema.builder()
				.addNullableField("LOAD_CNT", FieldType.DECIMAL)
				.build();
	}

	public static Schema loadVztSummary() {
		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("CNX_RESPONSE_FILE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("CNX_RESPONSE_FILE_NAME", FieldType.STRING)
				.addNullableField("CNX_RESPONSE_REC_CNT", FieldType.DECIMAL)
				.addNullableField("CNX_RESPONSE_LOAD_CNT", FieldType.DECIMAL)
				.addNullableField("CNX_RESPONSE_FILE_LOAD_DT", FieldType.DATETIME)
				.build();
	}

	public static Schema loadVztBatchTable() {
		return Schema.builder()
				.addNullableField("BATCH_ID", FieldType.DECIMAL)
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("START_DT", FieldType.DATETIME)
				.addNullableField("BATCH_NAME", FieldType.STRING)
				.addNullableField("PROCESS_CNT", FieldType.DECIMAL)
				.addNullableField("FAIL_CNT", FieldType.DECIMAL)
				.addNullableField("FILE_NAME", FieldType.STRING)
				.addNullableField("END_DT", FieldType.DATETIME)
				.addNullableField("REC_CNT", FieldType.DECIMAL)
				.addNullableField("COMMENTS", FieldType.STRING)
				.build();
	}

}
