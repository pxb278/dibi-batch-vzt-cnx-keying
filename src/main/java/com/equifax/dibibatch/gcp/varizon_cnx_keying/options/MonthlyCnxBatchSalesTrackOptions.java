package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface MonthlyCnxBatchSalesTrackOptions extends GcpOptions, JdbcIOOptions, JdbcExecutionOptions, PGPCryptorOptions,
HashiCorpVaultOptions{
	
	@Description("Schema Name")
	@Validation.Required
	String getVerizonDataShareSchema();
	void setVerizonDataShareSchema(String value);
    
    @Description("Report File Path")
    @Validation.Required
    ValueProvider<String> getReportGenPath();
    void setReportGenPath(ValueProvider<String> value);

    @Description("Report summary update file name")
    @Validation.Required
    ValueProvider<String> getSummaryUpdateFileName();
    void setSummaryUpdateFileName(ValueProvider<String> value);
    
    @Description("Report .xls file name")
    @Validation.Required
    ValueProvider<String> getWriteToXlsFileName();
    void setWriteToXlsFileName(ValueProvider<String> value);
    
    @Description("Verizon date")
    @Validation.Required
    ValueProvider<String> getDate();
    void setDate(ValueProvider<String> value);
}
