package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractFromStDsOutToLoadVztDataShareOptions;

@RunWith(JUnit4.class)
public class ExtractFromStDsOutToLoadVztDataSharePipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner", "--dbSchema=verizon",
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/ST_DS_CNX_IN_07072020091600.txt",
			"--currentTimestamp=07-07-2020T16:09:00",
			"--processName=att-dm-keying-request",
			"--pgpCryptionEnabled=false",
			"--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {
		ExtractFromStDsOutToLoadVztDataShareOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractFromStDsOutToLoadVztDataShareOptions.class);

		Pipeline pipeline = ExtractFromStDsOutToLoadVztDataSharePipeLine
				.ExtractFromStDsOutToLoadVztDataShareFile(options);

		Assert.assertNotEquals(pipeline, null);
	}
}
