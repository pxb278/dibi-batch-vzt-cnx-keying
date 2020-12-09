package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractInactivatedOverrideRecordsOptions;

@RunWith(JUnit4.class)
public class ExtractInactivatedOverrideRecordsPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

	static final String[] args = new String[] { "--runner=DirectRunner", "--vwVztDatashareDbSchema=verizon",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=Inactivated_Override_Records.txt", "--pgpCryptionEnabled=false", "--pgpSigned=false" };

	// Smoke Test
	@Test
	public void loadDataTest() throws Exception {
		ExtractInactivatedOverrideRecordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractInactivatedOverrideRecordsOptions.class);
		Pipeline pipeline = ExtractInactivatedOverrideRecordsPipeLine.executePipeline(options);
		Assert.assertNotEquals(pipeline, null);
	}
}
