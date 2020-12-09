package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztBatchAndVztSummaryOptions;

@RunWith(JUnit4.class)
public class LoadVztBatchAndVztSummaryPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner", "--vztBatchDbSchema=dibi_batch",
			"--vztDataShareDbSchema=dibi_batch", "--vztBatchSummaryDbSchema=dibi_batch", "--batchId=80",
			"--currentTimestamp=2020-10-07'T'16:09:00", "--vztCnxRespFileCnt=5", "--ultimateBatchId=6",
			"--vztCnxRespFile=DICNX0000002-efx_vzw_20200710160900.snd" };

	// smoke test
	@Test
	public void executePipelineTest() throws Exception {

		LoadVztBatchAndVztSummaryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVztBatchAndVztSummaryOptions.class);

		Pipeline pipeline = LoadVztBatchAndVztSummaryPipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);
	}

}
