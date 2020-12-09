package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVZTBatchSummarryStatsTablePipeLineOptions;

@RunWith(JUnit4.class)
public class LoadVZTBatchSummarryStatsTablePipeLineTest {
	
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	static final String[] args = new String[] {"--runner=DirectRunner",
			"--driverClass=org.postgresql.Driver",
			"--connectionString=jdbc:postgresql://localhost:5432/postgres",
			"--username=postgres",
			"--password=1786",
			"--vztDataShareDbSchema==dibi_batch",
			"--vztBatchSummaryDbSchema=dibi_batch", 
			"--vztBatchDbSchema=dibi_batch",
			"--vztBatchStatsDbSchema=dibi_batch", 
			"--batchId=80",
			"--srcFileName=VZT_DS_PROD.KREQ",
			"--currentTimestamp=2020-10-07'T'16:09:00", 
			"--ultimateBatchId=1", 
			"--CNXExtractCount=9", 
			"--sourceVbbRecordCount=8", 
			"--sourceVzbRecordCount=7", 
			"--sourceVztRecordCount=6", 
			"--sourceVzwRecordCount=5", 
			"--inputfileFinalCount=9", 
			"--inputfileLiveCount=10", 
			"--inputFileRecCount=8", 
			"--keyingFlagValueF=1", 
			"--keyingFlagValueT=7", 
			"--vztInsertCnt=6", 
			"--vztCnxReqFile=DICNX0000002-efx_vzw_20200710160900.snd"};
	
	@Test
	public void executePipelineTest() throws Exception {

		LoadVZTBatchSummarryStatsTablePipeLineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVZTBatchSummarryStatsTablePipeLineOptions.class);

		Pipeline pipeline = LoadVZTBatchSummarryStatsTablePipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);
	}

}

