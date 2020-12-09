package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadFromInactiveOvrerrideRecordsOptions;

public class LoadFromInactiveOvrerrideRecordsPipeLineTest {

	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	static final String[] args = new String[] {"--runner=DirectRunner",
			"--driverClass=org.postgresql.Driver",
			"--connectionString=jdbc:postgresql://localhost:5432/postgres",
			"--username=postgres",
			"--password=1786",
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/Inactivated_Override_Records.txt",
			"--vztDataShareDbSchema=dibi_batch",
			"--connexusKeyOverrideDbSchema=dibi_batch",
			"--currentTimestamp=07-07-2020T16:09:00",
			"--outputPath=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources",
			"--outputFileName=cnxid_delta.txt",
			"--pgpCryptionEnabled=false",
			"--pgpSigned=false"};
	
	@Test
	public void executePipelineTest() throws Exception {

		LoadFromInactiveOvrerrideRecordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadFromInactiveOvrerrideRecordsOptions.class);

		Pipeline pipeline = LoadFromInactiveOvrerrideRecordsPipeLine.executePipeline(options);

		Assert.assertNotEquals(pipeline, null);
	}
}
