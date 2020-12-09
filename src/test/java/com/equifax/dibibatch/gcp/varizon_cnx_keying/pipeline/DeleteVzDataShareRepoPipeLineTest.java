package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Rule;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVzDataShareRepoOptions;

public class DeleteVzDataShareRepoPipeLineTest {
	
	@Rule
	public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
	
	static final String[] args = new String[] {"--runner=DirectRunner",
			"--driverClass=org.postgresql.Driver",
			"--connectionString=jdbc:postgresql://localhost:5432/postgres",
			"--username=postgres",
			"--password=1786",
			"--input=C:/Users/pxb278/Desktop/workspace/dibi-batch/varizon-cnx-keying/Resources/DATASHARE_SSA_CON_INPUT_80.txt",
			"--vztDataShareCnxRepoDbSchema=dibi_batch",
			"--pgpCryptionEnabled=false",
			"--pgpSigned=false"};
	
	@Test
	public void executePipelineTest() throws Exception {

		LoadVzDataShareRepoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVzDataShareRepoOptions.class);

		/*
		 * Pipeline pipeline = LoadVzDataShareRepoPipeLine.executePipeline(options);
		 * 
		 * Assert.assertNotEquals(pipeline, null);
		 */
	}

}
