package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractStDsFuzzyRespToLoadVztDataShareOptions;

@RunWith(JUnit4.class)
public class ExtractStDsFuzzyRespToLoadVztDataSharePipeLineTest {

	static final String[] args = new String[] { "--runner=DirectRunner", "--vztDatashareDbSchema=vztDatashareDbSchema",
			"--batchId=8", "--currentTimestamp=07-07-2020T16:09:00",
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/ST_DS_FUZZY_RESP_8.txt",
			"--processName=varizon-cnx-keying", "--outputFileName=cnx_override.txt", "--pgpCryptionEnabled=false",
			"--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {

		ExtractStDsFuzzyRespToLoadVztDataShareOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractStDsFuzzyRespToLoadVztDataShareOptions.class);

		Pipeline pipeline = ExtractStDsFuzzyRespToLoadVztDataSharePipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);

	}
}
