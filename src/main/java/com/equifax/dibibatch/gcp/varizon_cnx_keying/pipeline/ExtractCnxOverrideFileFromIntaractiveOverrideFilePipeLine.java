package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractCnxOverrideFileOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadFromInactiveOvrerrideRecordsFileSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.LoadFromInactiveOvrerrideRecordsTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 6: JOB_DatashareInactiveOverrideConneuxKeyUpdate	*/

public class ExtractCnxOverrideFileFromIntaractiveOverrideFilePipeLine {
	private static final Logger log = LoggerFactory
			.getLogger(ExtractCnxOverrideFileFromIntaractiveOverrideFilePipeLine.class);
	private static final String HEADER = "DATASHAREID|STATUS|INDIVIDUALKEY|HOUSEHOLDKEY|OVERRIDEREQUESTOR|DATEMODIFIED";

	public static void main(String[] args) {

		ExtractCnxOverrideFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractCnxOverrideFileOptions.class);
		log.info("Running the pipeline");
		Pipeline pipeline = executePipeline(options);
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ExtractCnxOverrideFileOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();

		Supplier<PGPCryptorInfo> cryptorInfoSupplierForDIBI = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		Supplier<PGPCryptorInfo> cryptorInfoSupplierForMFTToSendIC = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHcvKeystoreGcsBucketName(options.getHcvMFTKeystoreGcsBucketName())
				.withHcvKeystoreGcsFilePath(options.getHcvMFTKeystoreGcsFilePath()).withHcvUrl(options.getHcvMFTUrl())
				.withHcvNamespace(options.getHcvMFTNamespace()).withHcvSecretPath(options.getHcvMFTSecretPath())
				.withHcvEngineVersion(options.getHcvMFTEngineVersion())
				.withHcvConnectionTimeout(options.getHcvMFTConnectionTimeout())
				.withHcvReadTimeout(options.getHcvMFTReadTimeout())
				.withHcvReadRetryCount(options.getHcvMFTReadRetryCount())
				.withHcvReadRetryInterval(options.getHcvMFTReadRetryInterval())
				.withHcvGcpRole(options.getHcvMFTGcpRole()).withHcvGcpProjectId(options.getHcvMFTGcpProjectId())
				.withHcvGcpServiceAccount(options.getHcvMFTGcpServiceAccount());

		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info(
				"Applying transform(s): (1) Read from Inactivated_Override_Records.txt File (2) Convert to Schema Row and Validate");

		PCollectionTuple extractFileRowsTuple = pipeline
				.apply("Read from Inactivated_Override_Records.txt} File",
						PGPDecryptFileIO.read().from(options.getInput())
								.withCryptorInfoSupplier(cryptorInfoSupplierForDIBI))
				.apply("Convert to Schema Row and Validate", ParDo.of(
						new LoadFromInactiveOvrerrideRecordsTransform(validRowsTag, invalidRowsTag, currentTimestamp))
						.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> extractFileValidRows = extractFileRowsTuple.get(validRowsTag.getId());
		extractFileValidRows
				.setRowSchema(LoadFromInactiveOvrerrideRecordsFileSchema.extractInactiveOvrerrideRecordsFile());

		// Extract cnx_override.txt Output file
		log.info("Applying transform(s): Extract cnx_override.txt Onput file");
		extractFileValidRows
				.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplierForMFTToSendIC, options.getOutputFileName(),
						options.getOutputPath(), WriteFileConfigHelper.getCnxOverrideFile(), "|", HEADER, null, false));

		return pipeline;
	}
}
