package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.BarricaderOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileInputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileOutputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface LoadVztDataShareTablePipeLineOptions extends GcpOptions, PGPCryptorOptions, HashiCorpVaultOptions,
		FileOutputOptions, FileInputOptions, JdbcIOOptions, JdbcExecutionOptions, BarricaderOptions, MftPgpCryptorOptions {

	@Description("DB Schema for VZT_DATASHARE")
	@Validation.Required
	String getDbSchema();

	void setDbSchema(String value);

	@Description("Current Timestamp for the Job with format => MM-dd-yyyy'T'HH:mm:ss")
	@Validation.Required
	ValueProvider<String> getCurrentTimestamp();

	void setCurrentTimestamp(ValueProvider<String> value);

	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);

	@Description("Process Name for the Job")
	@Validation.Required
	ValueProvider<String> getProcessName();

	void setProcessName(ValueProvider<String> value);

}
