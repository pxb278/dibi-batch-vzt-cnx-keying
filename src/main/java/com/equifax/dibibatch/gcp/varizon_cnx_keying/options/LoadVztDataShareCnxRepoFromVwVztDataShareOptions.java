package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.BarricaderOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileOutputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;

public interface LoadVztDataShareCnxRepoFromVwVztDataShareOptions
		extends GcpOptions, FileOutputOptions, JdbcIOOptions, JdbcExecutionOptions, BarricaderOptions {

	@Description("DB Schema for VW_VZT_DATASHARE")
	@Validation.Required
	String getVwVztDatashareDbSchema();

	void setVwVztDatashareDbSchema(String value);

	@Description("DB Schema for VZ_DATASHARE_CNX_REPO")
	@Validation.Required
	String getVzDataShareCnxRepodbSchema();

	void setVzDataShareCnxRepodbSchema(String value);

	@Description("Current Timestamp for the Job with format => MM-dd-yyyy'T'HH:mm:ss")
	@Validation.Required
	ValueProvider<String> getCurrentTimestamp();

	void setCurrentTimestamp(ValueProvider<String> value);

	@Description("Batch Id for the Job")
	@Validation.Required
	ValueProvider<Integer> getBatchId();

	void setBatchId(ValueProvider<Integer> value);

}
