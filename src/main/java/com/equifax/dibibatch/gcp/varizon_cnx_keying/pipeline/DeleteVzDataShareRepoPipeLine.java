package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVzDataShareRepoHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVzDataShareRepoOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVzDataShareRepoSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVzDataShareRepoTransformSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.LoadVzDataShareRepoTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 11: VZ_Datashare_Cnx_Repo_SSA_Delete											
*/

public class DeleteVzDataShareRepoPipeLine {

	private static final Logger log = LoggerFactory.getLogger(DeleteVzDataShareRepoPipeLine.class);

	public static void main(String[] args) {
		LoadVzDataShareRepoOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVzDataShareRepoOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadVzDataShareRepoOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		String vztDataShareCnxRepoDbSchema = options.getVztDataShareCnxRepoDbSchema();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		Supplier<PGPCryptorInfo> cryptorInfoSupplierForSSA = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHcvKeystoreGcsBucketName(options.getHcvSSAKeystoreGcsBucketName())
				.withHcvKeystoreGcsFilePath(options.getHcvSSAKeystoreGcsFilePath()).withHcvUrl(options.getHcvSSAUrl())
				.withHcvNamespace(options.getHcvSSANamespace()).withHcvSecretPath(options.getHcvSSASecretPath())
				.withHcvEngineVersion(options.getHcvSSAEngineVersion())
				.withHcvConnectionTimeout(options.getHcvSSAConnectionTimeout())
				.withHcvReadTimeout(options.getHcvSSAReadTimeout())
				.withHcvReadRetryCount(options.getHcvSSAReadRetryCount())
				.withHcvReadRetryInterval(options.getHcvSSAReadRetryInterval())
				.withHcvGcpRole(options.getHcvSSAGcpRole()).withHcvGcpProjectId(options.getHcvSSAGcpProjectId())
				.withHcvGcpServiceAccount(options.getHcvSSAGcpServiceAccount());

		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info(
				"Applying transform(s): (1) Read from DATASHARE_SSA_CON_INPUT_${VERIZON_BATCHID}.txt File (2) Convert to Schema Row and Validate");

		PCollectionTuple extractFileRowsTuple = pipeline
				.apply("Read from DATASHARE_SSA_CON_INPUT_${VERIZON_BATCHID}.txt} File",
						PGPDecryptFileIO.read().from(options.getInput())
								.withCryptorInfoSupplier(cryptorInfoSupplierForSSA))
				.apply("Convert to Schema Row and Validate",
						ParDo.of(new LoadVzDataShareRepoTransform(validRowsTag, invalidRowsTag))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> extractFileValidRows = extractFileRowsTuple.get(validRowsTag.getId());
		extractFileValidRows.setRowSchema(LoadVzDataShareRepoTransformSchema.extractFromDataSharSsaConInputFile());

		TupleTag<Row> rejectedRowsTag = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZ_DATASHARE_CNX_REPO");
		TupleTag<Row> unprocessedRowsTag = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZ_DATASHARE_CNX_REPO");

		// Delete from VZ_DATASHARE_CNX_REPO Table
		log.info("Applying transform: Update into table 'VZ_DATASHARE_CNX_REPO'");
		new JdbcWriter<Row>("Delete table 'VZ_DATASHARE_CNX_REPO'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVzDataShareRepoSql.DELETE_VZ_DATASHARE_CNX_REPO_TABLE,
					Arrays.asList(vztDataShareCnxRepoDbSchema));
		}, (row, preparedStatement) -> {
			LoadVzDataShareRepoHelper.deleteVzDataShareRepoTableRow(row, preparedStatement);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTag, unprocessedRowsTag)
				.execute(extractFileValidRows);

		return pipeline;

	}

}
