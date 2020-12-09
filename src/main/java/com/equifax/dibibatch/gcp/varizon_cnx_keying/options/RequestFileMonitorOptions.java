package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileOutputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface RequestFileMonitorOptions
		extends GcpOptions, JdbcIOOptions, FileOutputOptions, PGPCryptorOptions, HashiCorpVaultOptions {

	@Description("VZT Batch Sumary Schema Name")
	@Validation.Required
	String getVztBatchSummarySchemaName();

	void setVztBatchSummarySchemaName(String value);
	

	@Description("Request File Alert Email File Name")
	@Validation.Required
	ValueProvider<String> getEmailFileName();

	void setEmailFileName(ValueProvider<String> value);

}
