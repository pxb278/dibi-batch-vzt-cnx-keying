package com.equifax.dibibatch.gcp.varizon_cnx_keying.testpipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDICNX0000002EfxVzwFileOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline.ExtractDICNX0000002EfxVzwFilePipeLine;

@RunWith(JUnit4.class)
public class ExtractDICNX0000002EfxVzwFilePipeLineTest {
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner" };

	// Smoke Test
	@Test
	public void loadCmfDataTest() throws Exception {

		ExtractDICNX0000002EfxVzwFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDICNX0000002EfxVzwFileOptions.class);

		//Pipeline pipeline = ExtractDICNX0000002EfxVzwFilePipeLine.class(options);

		//Assert.assertNotEquals(pipeline, null);

	}

}
