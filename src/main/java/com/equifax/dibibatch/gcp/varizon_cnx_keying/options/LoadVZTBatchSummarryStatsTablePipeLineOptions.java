package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.BarricaderOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcExecutionOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.JdbcIOOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface LoadVZTBatchSummarryStatsTablePipeLineOptions extends GcpOptions, PGPCryptorOptions,
		HashiCorpVaultOptions, JdbcIOOptions, JdbcExecutionOptions {

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

	@Description("DB Schema for VZT_BATCH_STATS")
	@Validation.Required
	String getVztBatchStatsDbSchema();

	void setVztBatchStatsDbSchema(String value);

	@Description("DB Schema for Src_file_name")
	@Validation.Required
	ValueProvider<String> getSrcFileName();

	void setSrcFileName(ValueProvider<String> value);

	@Description("Vzt_Cnx_Req file name")
	@Validation.Required
	ValueProvider<String> getVztCnxReqFile();

	void setVztCnxReqFile(ValueProvider<String> value);

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

	@Description("Keying_Flag_Value_T for the Job")
	@Validation.Required
	ValueProvider<Integer> getKeyingFlagValueT();

	void setKeyingFlagValueT(ValueProvider<Integer> value);

	@Description("Keying_Flag_Value_F for the Job")
	@Validation.Required
	ValueProvider<Integer> getKeyingFlagValueF();

	void setKeyingFlagValueF(ValueProvider<Integer> value);

	@Description("Source_Vzt_Record_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getSourceVztRecordCount();

	void setSourceVztRecordCount(ValueProvider<Integer> value);

	@Description("Source_Vzw_Record_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getSourceVzwRecordCount();

	void setSourceVzwRecordCount(ValueProvider<Integer> value);

	@Description("Source_Vbb_Record_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getSourceVbbRecordCount();

	void setSourceVbbRecordCount(ValueProvider<Integer> value);

	@Description("Source_Vzb_Record_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getSourceVzbRecordCount();

	void setSourceVzbRecordCount(ValueProvider<Integer> value);

	@Description("Input_file_Live_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getInputfileLiveCount();

	void setInputfileLiveCount(ValueProvider<Integer> value);

	@Description("Input_file_Final_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getInputfileFinalCount();

	void setInputfileFinalCount(ValueProvider<Integer> value);

	@Description("CNX_Extract_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getCNXExtractCount();

	void setCNXExtractCount(ValueProvider<Integer> value);

	@Description("Vzt_Insert_Cnt for the Job")
	@Validation.Required
	ValueProvider<Integer> getVztInsertCnt();

	void setVztInsertCnt(ValueProvider<Integer> value);

	@Description("Input_file_Rec_Count for the Job")
	@Validation.Required
	ValueProvider<Integer> getInputFileRecCount();

	void setInputFileRecCount(ValueProvider<Integer> value);
	
	@Description("Input_file_date for the Job")
	@Validation.Required
	ValueProvider<String> getInputFileDate();

	void setInputFileDate(ValueProvider<String> value);


}
