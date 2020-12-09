package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztDataShareCnxRepoFromVwVztDataShareOptions;

public class LoadVztDataShareCnxRepoFromVwVztDataSharePipeLineTest {
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner",
			"--vwVztDatashareDbSchema=vwVztDatashareDbSchema",
			"--vzDataShareCnxRepodbSchema=vzDataShareCnxRepodbSchema", "--currentTimestamp=07-07-2020T16:09:00",
			"--batchId=8" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		LoadVztDataShareCnxRepoFromVwVztDataShareOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(LoadVztDataShareCnxRepoFromVwVztDataShareOptions.class);

		Pipeline pipeline = LoadVztDataShareCnxRepoFromVwVztDataSharePipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);

	}
}
