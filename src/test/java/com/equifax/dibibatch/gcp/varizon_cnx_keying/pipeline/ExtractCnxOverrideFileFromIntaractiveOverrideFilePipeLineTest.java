package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractCnxOverrideFileOptions;

@RunWith(JUnit4.class)
public class ExtractCnxOverrideFileFromIntaractiveOverrideFilePipeLineTest {

	static final String[] args = new String[] { "--runner=DirectRunner",
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/Inactivated_Override_Records.txt",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=cnx_override.txt", "--pgpCryptionEnabled=false", "--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		ExtractCnxOverrideFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractCnxOverrideFileOptions.class);

		Pipeline pipeline = ExtractCnxOverrideFileFromIntaractiveOverrideFilePipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);

	}
}
