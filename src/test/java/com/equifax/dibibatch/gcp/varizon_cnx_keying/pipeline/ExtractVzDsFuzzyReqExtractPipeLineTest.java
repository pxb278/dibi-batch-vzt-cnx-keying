package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractVzDsFuzzyReqExtractOptions;

@RunWith(JUnit4.class)
public class ExtractVzDsFuzzyReqExtractPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner", "--vwVztDatashareDbSchema=verizon",
			"--batchId=8", "--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=VZ_DS_Fuzzy_Req_Extract_80.txt.txt", "--pgpCryptionEnabled=false", "--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		ExtractVzDsFuzzyReqExtractOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractVzDsFuzzyReqExtractOptions.class);

		Pipeline pipeline = ExtractVzDsFuzzyReqExtractPipeLine.ExtractVzDsFuzzyReqExtractFile(options);

		Assert.assertNotEquals(pipeline, null);
	}
}