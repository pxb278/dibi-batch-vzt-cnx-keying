package com.equifax.dibibatch.gcp.varizon_cnx_keying.sql;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;

public class MonthlyCnxBatchSalesSchema {
	public static Schema selectMonthlyCnxBatchSalesTrack() {
		return Schema.builder()
				.addNullableField("YEAR", FieldType.STRING)
				.addNullableField("CURR_MONTH", FieldType.STRING)
				.addNullableField("VZ_CNX_Offline_MONTHLY_COUNT", FieldType.STRING)
				.addNullableField("RUNNING_SUM", FieldType.STRING)
				.build();
	}	
}
