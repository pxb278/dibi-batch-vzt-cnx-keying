package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractDataShareSsaConInputHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDataShareSsaConInputOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDataShareSsaConInputSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.DataShareSsaConInputTransformation;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

public class ExtractDataShareSsaConInputPipeLine {

	private static final Logger log = LoggerFactory.getLogger(ExtractDataShareSsaConInputPipeLine.class);

	public static void main(String[] args) {
		ExtractDataShareSsaConInputOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDataShareSsaConInputOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();

	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ExtractDataShareSsaConInputOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vzDatashareCnxRepoDbSchema = options.getVzDatashareCnxRepoDbSchema();
		BarricaderInfo barricaderInfo = BarricaderInfo.create().withBarricaderOptions(options);
		ValueProvider<Integer> batchId = options.getBatchId();
		/*
		 * Supplier<PGPCryptorInfo> cryptorInfoSupplier =
		 * HashiCorpVaultPGPCryptorInfoSupplier.create()
		 * .withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);
		 */

		// SSA Crypto Supplier
		Supplier<PGPCryptorInfo> ssaCryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
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

		// Read from VZ_DATASHARE_CNX_REPO Table
		log.info("Applying transform(s): Retrieve rows from VZ_DATASHARE_CNX_REPO Table");
		PCollection<Row> readFromTablesRows = new JdbcReader<Row>("Read from table VZ_DATASHARE_CNX_REPO",
				dbConnectionInfo, SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(ExtractDataShareSsaConInputSql.SELECT_VZ_DATASHARE_CNX_REPO_TABLE,
						Arrays.asList(vzDatashareCnxRepoDbSchema)),
				(preparedStatement) -> {
					ExtractDataShareSsaConInputHelper.setParameterExtractDataShareSsaConInputRow(preparedStatement,
							batchId);
				}, (resultSet) -> {
					return ExtractDataShareSsaConInputHelper.readExtractDataShareSsaConInputRow(resultSet);
				}).execute(pipeline);

		PCollection<Row> rowDecrypt = readFromTablesRows.apply(" Decrypt the  Data",
				ParDo.of(new DataShareSsaConInputTransformation(barricaderInfo)));

		rowDecrypt.setRowSchema(ExtractDataShareSsaConInputSchema.ExtractExtractDataShareSsaConInput());

		// Extract DATASHARE_SSA_CON_INPUT_${VERIZON_BATCHID}.txt file
		log.info("Applying transform(s): Extract DATASHARE_SSA_CON_INPUT_${VERIZON_BATCHID}.txt Onput file");
		rowDecrypt.apply(
				new PGPEncryptFileIO<Row>(ssaCryptorInfoSupplier, options.getOutputFileName(), options.getOutputPath(),
						WriteFileConfigHelper.getConfigForExtractDataShareSsaConInputFile(), "|", null, null, false));

		return pipeline;

	}

}
