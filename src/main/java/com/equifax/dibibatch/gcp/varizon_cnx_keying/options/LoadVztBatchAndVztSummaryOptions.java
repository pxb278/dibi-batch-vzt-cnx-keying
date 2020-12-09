package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface LoadVztBatchAndVztSummaryOptions
		extends GcpOptions, PGPCryptorOptions, HashiCorpVaultOptions, JdbcIOOptions, JdbcExecutionOptions {
	
	@Description("DB Schema for VZT_DATASHARE")
	@Validation.Required
	String getVztDataShareDbSchema();

	void setVztDataShareDbSchema(String value);

	@Description("DB Schema for VZT_BATCH")
	@Validation.Required
	String getVztBatchDbSchema();

	void setVztBatchDbSchema(String value);

	@Description("DB Schema for VZT_BATCH_SUMMARY")
	@Validation.Required
	String getVztBatchSummaryDbSchema();

	void setVztBatchSummaryDbSchema(String value);
	
	@Description("Vzt_Cnx_Resp file name")
	@Validation.Required
	ValueProvider<String> getVztCnxRespFile();

	void setVztCnxRespFile(ValueProvider<String> value);

	@Description("Current Timestamp for the Job with format => MM-dd-yyyy'T'HH:mm:ss")
	@Validation.Required
	ValueProvider<String> getCurrentTimestamp();

	void setCurrentTimestamp(ValueProvider<String> value);

	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);

	@Description("Ultimate Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getUltimateBatchId();

	void setUltimateBatchId(ValueProvider<Integer> value);


	@Description("Vzt_Cnx_Resp_File_Cnt for the Job")
	@Validation.Required
	ValueProvider<Integer> getVztCnxRespFileCnt();

	void setVztCnxRespFileCnt(ValueProvider<Integer> value);
}
