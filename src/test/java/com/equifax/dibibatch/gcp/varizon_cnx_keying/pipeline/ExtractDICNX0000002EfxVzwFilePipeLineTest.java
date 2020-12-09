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

@RunWith(JUnit4.class)
public class ExtractDICNX0000002EfxVzwFilePipeLineTest {
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner" ,
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/ST_DS_CNX_IN_07072020091600.txt",
			"--currentTimestamp=07-07-2020T16:09:00",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=DICNX0000002-efx_vzw_07072020160000.snd",
			"--pgpCryptionEnabled=false",
			"--pgpSigned=false"};

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		ExtractDICNX0000002EfxVzwFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDICNX0000002EfxVzwFileOptions.class);

		Pipeline pipeline = ExtractDICNX0000002EfxVzwFilePipeLine.ExtractDICNX0000002EfxVzwFile(options);

		Assert.assertNotEquals(pipeline, null);

	}

}
