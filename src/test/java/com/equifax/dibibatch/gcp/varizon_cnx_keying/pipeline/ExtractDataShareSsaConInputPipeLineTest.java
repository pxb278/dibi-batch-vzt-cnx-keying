package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDICNX0000002EfxVzwFileOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDataShareSsaConInputOptions;

@RunWith(JUnit4.class)
public class ExtractDataShareSsaConInputPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	public static String[] args = new String [] { "--runner=DirectRunner" ,
			"--vzDatashareCnxRepoDbSchema=vzDatashareCnxRepoDbSchema",
			"--batchId=8",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=DATASHARE_SSA_CON_INPUT_80.txt",
			"--pgpCryptionEnabled=false",
			"--pgpSigned=false"};
	
	// Smoke Test
		@Test
		public void loadDataTest() throws Exception {

			ExtractDataShareSsaConInputOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
					.as(ExtractDataShareSsaConInputOptions.class);

			Pipeline pipeline = ExtractDataShareSsaConInputPipeLine.executePipeline(options);

			Assert.assertNotEquals(pipeline, null);

		}
}
