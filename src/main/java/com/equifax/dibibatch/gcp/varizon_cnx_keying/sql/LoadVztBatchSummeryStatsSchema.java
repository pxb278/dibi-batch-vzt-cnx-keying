package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class LoadVztBatchSummeryStatsSchema {

	public static Schema readRowsToLoadVztBatchSummeryStatstables() {
		return Schema.builder()
				.addNullableField("LOAD_CNT", FieldType.DECIMAL)
				.addNullableField("INPUT_FILE_DT", FieldType.STRING)
				.build();
	}
	

	public static Schema loadVztBatchStasTable() {
		return Schema.builder()
				
				.addNullableField("BATCH_ID", FieldType.STRING)
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.STRING)
				.addNullableField("START_DT", FieldType.STRING)
				.addNullableField("BATCH_NAME", FieldType.STRING)
				.addNullableField("PROCESS_CNT", FieldType.STRING)
				.addNullableField("FAIL_CNT", FieldType.STRING)
				.addNullableField("FILE_NAME", FieldType.STRING)
				.addNullableField("END_DT", FieldType.STRING)
				.addNullableField("REC_CNT", FieldType.STRING)
				.addNullableField("COMMENTS", FieldType.STRING)
				.addNullableField("LOAD_FAIL_CNT", FieldType.STRING)
				.build();
	
	}

	public static Schema loadVztBatchSummaryTable() {
		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.STRING)
				.addNullableField("VZT_INPUT_FILE_BATCH_ID", FieldType.STRING)
				.addNullableField("VZT_INPUT_FILE_NAME", FieldType.STRING)
				.addNullableField("VZT_INPUT_FILE_DT", FieldType.STRING)
				.addNullableField("VZT_INPUT_REC_CNT", FieldType.STRING)
				.addNullableField("VZT_INPUT_LOAD_CNT", FieldType.STRING)
				.addNullableField("VZT_INPUT_FAIL_CNT", FieldType.STRING)
				.addNullableField("VZT_KEYING_FLAG_T", FieldType.STRING)
				.addNullableField("VZT_KEYING_FLAG_F", FieldType.STRING)
				.addNullableField("VZT_INPUT_FILE_LOAD_DT", FieldType.STRING)
				.addNullableField("CNX_INPUT_FILE_BATCH_ID", FieldType.STRING)
				.addNullableField("CNX_INPUT_FILE_NAME", FieldType.STRING)
				.addNullableField("CNX_INPUT_FILE_EXTRACT_CNT", FieldType.STRING)
				.addNullableField("CNX_INPUT_FILE_EXTRACT_DT", FieldType.STRING)
				.build();
	}
	

	public static Schema loadVztBatchTable() {
		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.STRING)
				.addNullableField("INPUT_FILE_NAME", FieldType.STRING)
				.addNullableField("INPUT_FILE_DT", FieldType.STRING)
				.addNullableField("INPUT_REC_CNT", FieldType.STRING)
				.addNullableField("INPUT_LOAD_CNT", FieldType.STRING)
				.addNullableField("INPUT_FAIL_CNT", FieldType.STRING)
				.addNullableField("INSER_REC_CNT", FieldType.STRING)
				.addNullableField("UPDATE_REC_CNT", FieldType.STRING)
				.addNullableField("SOURCE_VZT_CNT", FieldType.STRING)
				.addNullableField("SOURCE_VZT_VZW_CNT", FieldType.STRING)
				.addNullableField("SOURCE_VZB_CNT", FieldType.STRING)
				.addNullableField("SOURCE_VBB_CNT", FieldType.STRING)
				.addNullableField("LIVE_CNT", FieldType.STRING)
				.addNullableField("FINALS_CNT", FieldType.STRING)
				.addNullableField("HIT_CNT", FieldType.STRING)
				.addNullableField("NOHIT_CNT", FieldType.STRING)
				.build();
	}


	

}
