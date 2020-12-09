package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztDataShareTablePipeLineOptions;

public class LoadVztDataShareTablePipeLineTest {

		
		@Rule
		public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);
		
		static final String[] args = new String[] {"--runner=DirectRunner",
				"--driverClass=org.postgresql.Driver",
				"--connectionString=jdbc:postgresql://localhost:5432/postgres",
				"--username=postgres",
				"--password=1786",
				"--input=C:\\Users\\Documents\\Backupfoldar\\Verizon\\VZT_DS_PROD.KREQ.0001222.20200916170129test.txt",
				"--dbSchema=dibi_batch",
				"--encryptionEnabled=false",
				"--kmsKeyReference=C:\\Users\\Documents\\my_keyset_two.json",
				"--wrappedDekBucketName=usis-dibi-landing-zone-npe",
				"--wrappedDekFilePath=C:\\Users\\Documents\\my_keyset_two.json",
				"--validateAndProvisionWrappedDek=false",
				"--pgpCryptionEnabled=false",
				"--pgpSigned=false"};
		
		@Test
		public void executePipelineTest() throws Exception {

			LoadVztDataShareTablePipeLineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
					.as(LoadVztDataShareTablePipeLineOptions.class);

			Pipeline pipeline = LoadVztDataShareTablePipeLine.executePipeline(options);

			Assert.assertNotEquals(pipeline, null);
		}

	}


