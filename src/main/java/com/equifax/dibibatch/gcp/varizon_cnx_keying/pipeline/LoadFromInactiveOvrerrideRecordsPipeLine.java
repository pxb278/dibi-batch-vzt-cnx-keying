package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadFromInactiveOvrerrideRecordsHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadFromInactiveOvrerrideRecordsOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadFromInactiveOvrerrideRecordsFileSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadFromInactiveOvrerrideRecordsFileSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.HeaderFn;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.LoadFromInactiveOvrerrideRecordsTransform;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.TrailerFn;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 6: JOB_DatashareInactiveOverrideConneuxKeyUpdate*/

public class LoadFromInactiveOvrerrideRecordsPipeLine {
	private static final Logger log = LoggerFactory.getLogger(LoadFromInactiveOvrerrideRecordsPipeLine.class);

	public static void main(String[] args) {

		LoadFromInactiveOvrerrideRecordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadFromInactiveOvrerrideRecordsOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadFromInactiveOvrerrideRecordsOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		String vztDataShareDbSchema = options.getVztDataShareDbSchema();
		String connexusKeyOverrideDbSchema = options.getconnexusKeyOverrideDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);

		// For Verizon
		Supplier<PGPCryptorInfo> cryptorInfoSupplierVZT = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHcvKeystoreGcsBucketName(options.getHcvVZKeystoreGcsBucketName())
				.withHcvKeystoreGcsFilePath(options.getHcvVZKeystoreGcsFilePath()).withHcvUrl(options.getHcvVZUrl())
				.withHcvNamespace(options.getHcvVZNamespace()).withHcvSecretPath(options.getHcvVZSecretPath())
				.withHcvEngineVersion(options.getHcvVZEngineVersion())
				.withHcvConnectionTimeout(options.getHcvVZConnectionTimeout())
				.withHcvReadTimeout(options.getHcvVZReadTimeout())
				.withHcvReadRetryCount(options.getHcvVZReadRetryCount())
				.withHcvReadRetryInterval(options.getHcvVZReadRetryInterval()).withHcvGcpRole(options.getHcvVZGcpRole())
				.withHcvGcpProjectId(options.getHcvVZGcpProjectId())
				.withHcvGcpServiceAccount(options.getHcvVZGcpServiceAccount());

		Supplier<PGPCryptorInfo> cryptorInfoSupplierDIBI = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info(
				"Applying transform(s): (1) Read from Inactivated_Override_Records.txt File (2) Convert to Schema Row and Validate");

		PCollectionTuple extractFileRowsTuple = pipeline
				.apply("Read from Inactivated_Override_Records.txt} File",
						PGPDecryptFileIO.read().from(options.getInput())
								.withCryptorInfoSupplier(cryptorInfoSupplierDIBI))
				.apply("Convert to Schema Row and Validate", ParDo.of(
						new LoadFromInactiveOvrerrideRecordsTransform(validRowsTag, invalidRowsTag, currentTimestamp))
						.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> extractFileValidRows = extractFileRowsTuple.get(validRowsTag.getId());
		extractFileValidRows
				.setRowSchema(LoadFromInactiveOvrerrideRecordsFileSchema.extractInactiveOvrerrideRecordsFile());

		TupleTag<Row> rejectedRowsTag = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_DATASHARE");
		TupleTag<Row> unprocessedRowsTag = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_DATASHARE");

		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZT_DATASHARE'");
		new JdbcWriter<Row>("Update into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadFromInactiveOvrerrideRecordsFileSql.UPDATE_VZT_DATASHARE_TABLE,
					Arrays.asList(vztDataShareDbSchema));
		}, (row, preparedStatement) -> {
			LoadFromInactiveOvrerrideRecordsHelper.updateVztDataShareRow(row, preparedStatement, currentTimestamp);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTag, unprocessedRowsTag)
				.execute(extractFileValidRows);

		TupleTag<Row> rejectedRowsTagTwo = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_DATASHARE");
		TupleTag<Row> unprocessedRowsTagTwo = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_DATASHARE");

		// Update into table CONNEXUSKEYOVERRIDE
		log.info("Applying transform: Update into table 'CONNEXUSKEYOVERRIDE'");
		new JdbcWriter<Row>("Update into table 'CONNEXUSKEYOVERRIDE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(
					LoadFromInactiveOvrerrideRecordsFileSql.UPDATE_CONNEXUSKEYOVERRIDE_TABLE,
					Arrays.asList(connexusKeyOverrideDbSchema));
		}, (row, preparedStatement) -> {
			LoadFromInactiveOvrerrideRecordsHelper.updateConnexusKeyOverrideRow(row, preparedStatement,
					currentTimestamp);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTagTwo,
				unprocessedRowsTagTwo).execute(extractFileValidRows);

		ValueProvider<String> outputFileName = options.getOutputFileName();

		// Generate Header
		PCollectionView<String> fileHeaderView = extractFileValidRows.apply("Generate Header",
				new HeaderFn<Row>(outputFileName));

		// Generate Trailer
		PCollectionView<String> fileTrailerView = extractFileValidRows.apply("Generate Trailer",
				new TrailerFn<Row>(outputFileName));

		// Extract cnxid_delta.txt Output file
		log.info("Applying transform(s): Extract cnxid_delta.txt Onput file");
		extractFileValidRows.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplierVZT, outputFileName,
				options.getOutputPath(), WriteFileConfigHelper.getCnxIdDeltaFile(), "|", null, null, false,fileHeaderView, fileTrailerView));

		/*
		 * // Extract cnx_override.txt Output file
		 * log.info("Applying transform(s): Extract cnx_override.txt Onput file");
		 * extractFileValidRows.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplier,
		 * options.getOutputFileName(), options.getOutputPath(),
		 * WriteFileConfigHelper.getCnxOverrideFile(), "|", null, null, false));
		 */

		return pipeline;

	}

}
