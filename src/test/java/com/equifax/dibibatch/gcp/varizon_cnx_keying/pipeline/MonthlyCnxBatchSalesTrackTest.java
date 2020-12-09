package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.MonthlyCnxBatchSalesTrackOptions;

@RunWith(JUnit4.class)
public class MonthlyCnxBatchSalesTrackTest {
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner","--verizonDataShareSchema=verizonds"};

	@Test
	public void executePipelineTest() throws Exception {
		MonthlyCnxBatchSalesTrackOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(MonthlyCnxBatchSalesTrackOptions.class);
		Pipeline pipeline = MonthlyCnxBatchSalesTrack.executePipeline(options);
		Assert.assertNotEquals(pipeline, null);
	}
	
	@Test(expected = Exception.class)
	public void mainTest() throws Exception {
		MonthlyCnxBatchSalesTrack.main(args);
		p.run().waitUntilFinish();
	}
}
