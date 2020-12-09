package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileInputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface LoadVzDataShareRepoOptions extends GcpOptions, PGPCryptorOptions, HashiCorpVaultOptions, JdbcIOOptions,
		JdbcExecutionOptions, FileInputOptions, SSAHashiCorpVaultOptions {

	@Description("DB Schema for VZ_DATASHARE_CNX_REPO")
	@Validation.Required
	String getVztDataShareCnxRepoDbSchema();

	void setVztDataShareCnxRepoDbSchema(String value);
	
	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);

}
