package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface LoadTablesAsPerRowGeneratorOptions
		extends GcpOptions, JdbcIOOptions, JdbcExecutionOptions {

	@Description("DB Schema for VZt_STAT")
	@Validation.Required
	String getVztStatDbSchema();

	void setVztStatDbSchema(String value);

	@Description("DB Schema for VZt_BATCH")
	@Validation.Required
	String getVztBatchDbSchema();

	void setVztBatchDbSchema(String value);

	@Description("DB Schema for VZt_BATCH_SUMMARY")
	@Validation.Required
	String getVztSummaryDbSchema();

	void setVztSummaryDbSchema(String value);

	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);

	@Description("Current Timestamp for the Job with format => MM-dd-yyyy'T'HH:mm:ss")
	@Validation.Required
	ValueProvider<String> getCurrentTimestamp();

	void setCurrentTimestamp(ValueProvider<String> value);

	@Description("VztDSRespFile Name")
	@Validation.Required
	ValueProvider<String> getVztDSRespFileName();

	void setVztDSRespFileName(ValueProvider<String> value);

	@Description("VztDSRespFileCount for the Job")
	@Validation.Required
	ValueProvider<Integer> getVztDSRespFileCount();

	void setVztDSRespFileCount(ValueProvider<Integer> value);

	@Description("VztHitCount for the Job")
	@Validation.Required
	ValueProvider<Integer> getVztHitCount();

	void setVztHitCount(ValueProvider<Integer> value);

	@Description("VztNotHitCount for the Job")
	@Validation.Required
	ValueProvider<Integer> getVztNoHitCount();

	void setVztNoHitCount(ValueProvider<Integer> value);

	@Description("Ultimate Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getUltimateBatchId();

	void setUltimateBatchId(ValueProvider<Integer> value);

}
