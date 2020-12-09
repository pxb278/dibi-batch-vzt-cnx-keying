package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDivzcommsR00VztDsProdKrspOptions;

@RunWith(JUnit4.class)
public class ExtractDivzcommsR00VztDsProdKrspPipeLineTest {
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner",
			"--vwVztDataShareCnxRespDbSchema=vwVztDataShareCnxRespDbSchema",
			"--batchId=8",
			"--currentTimestamp=07-07-2020T16:09:00",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=DIVZCOMMSR00-VZT_DS_PROD.KRSP.${VZT_SEQ}.${DATE}${TIME}", "--pgpCryptionEnabled=false",
			"--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		ExtractDivzcommsR00VztDsProdKrspOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDivzcommsR00VztDsProdKrspOptions.class);

		Pipeline pipeline = ExtractDivzcommsR00VztDsProdKrspPipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);

	}
}
