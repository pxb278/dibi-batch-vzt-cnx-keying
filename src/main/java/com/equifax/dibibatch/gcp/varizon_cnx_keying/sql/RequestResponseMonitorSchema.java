package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class RequestResponseMonitorSchema {

	public static Schema schemaBatchResponseFileMonitor() {

		return Schema.builder()
				.addNullableField("ULTIMATE_BATCH_ID", FieldType.INT32)
				.addNullableField("CNX_INPUT_FILE_NAME", FieldType.STRING)
				
				.build();
	}

}
