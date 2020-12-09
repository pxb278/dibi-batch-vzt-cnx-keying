package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class LoadTablesAsPerRowGeneratorSchema {

	
	public static Schema vztBatchSchema() {
		
		return Schema.builder()
				.addNullableField("BATCH_ID", FieldType.DECIMAL)
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("START_DT", FieldType.DATETIME)
				.addNullableField("BATCH_NAME", FieldType.STRING)
				.addNullableField("FAIL_CNT", FieldType.DECIMAL)
				.addNullableField("FILE_NAME", FieldType.STRING)
				.addNullableField("END_DT", FieldType.DATETIME)
				.addNullableField("REC_CNT", FieldType.DECIMAL)
				.addNullableField("COMMENTS", FieldType.STRING)
				.build();
		

	}

	public static Schema vztBatchStat() {
		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUT_FILE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUR_FILE_NAME", FieldType.STRING)
				.addNullableField("VZT_OUTPUT_FILE_EXTRACT_CNT", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUT_FILE_EXTRACT_DT", FieldType.DATETIME)
				.build();
		

	}

	
	/*
	 * public static Schema vztBatchSummary() { return Schema.builder()
	 * .addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
	 * .addNullableField("HIT_CNT", FieldType.DECIMAL)
	 * .addNullableField("NOHIT_CNT", FieldType.DECIMAL) .build();
	 * 
	 * 
	 * }
	 */
	public static Schema vztBatchSummary() {
		System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");

		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUT_FILE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUT_FILE_NAME", FieldType.STRING)
				.addNullableField("VZT_OUTPUT_FILE_EXTRACT_CNT", FieldType.DECIMAL)
				.addNullableField("VZT_OUTPUT_FILE_EXTRACT_DT", FieldType.DATETIME)
				
				.build();
		
	}
	


	public static Schema readEnvVariables() {
		return Schema.builder()
				.addNullableField("BATCH_ID", FieldType.DECIMAL)
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.DECIMAL)
				.addNullableField("CURRENT_TIMESTAMP", FieldType.DATETIME)
				.addNullableField("VZT_DS_RESP_FILE", FieldType.STRING)
				.addNullableField("VZT_DS_RESP_FILE_CNT", FieldType.DECIMAL)
				.addNullableField("HIT_CNT", FieldType.DECIMAL)
				.addNullableField("NOHIT_CNT", FieldType.DECIMAL)
				.build();
	}

}
