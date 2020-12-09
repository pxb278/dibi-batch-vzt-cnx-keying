package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadTablesAsPerRowGeneratorOptions;

@RunWith(JUnit4.class)
public class LoadTablesAsPerRowGeneratorPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner", "--vztStatDbSchema=vztStatDbSchema",
			"--vztBatchDbSchema=vztBatchDbSchema", "--vztSummaryDbSchema=vztSummaryDbSchema",
			"--currentTimestamp=07-07-2020T16:09:00",
			"--vztDSRespFileName=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--vztDSRespFileCount=6",
			"--vztHitCount=7",
			"--vztNoHitCount=9",
			"--batchId=8" ,
			"--ultimateBatchId=8"};

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		LoadTablesAsPerRowGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadTablesAsPerRowGeneratorOptions.class);

		Pipeline pipeline = LoadTablesAsPerRowGeneratorPipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);

	}

}
