package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileOutputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface ExtractInactivatedOverrideRecordsOptions extends GcpOptions, PGPCryptorOptions, HashiCorpVaultOptions,
		FileOutputOptions, JdbcIOOptions, JdbcExecutionOptions {

	@Description("DB Schema for VW_VZT_DATASHARE")
	@Validation.Required
	String getVwVztDatashareDbSchema();

	void setVwVztDatashareDbSchema(String value);

	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);
}
