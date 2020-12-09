package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractDivzcommsR00VztDsProdKrsHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDivzcommsR00VztDsProdKrspOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDivzcommsR00VztDsProdKrspSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDivzcommsR00VztDsProdKrstSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.HeaderFn;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.TrailerFn;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 7: Job_Vzt_Datashare_Resp_Extract		*/

public class ExtractDivzcommsR00VztDsProdKrspPipeLine {

	private static final Logger log = LoggerFactory.getLogger(ExtractDivzcommsR00VztDsProdKrspPipeLine.class);

	public static void main(String[] args) {
		ExtractDivzcommsR00VztDsProdKrspOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDivzcommsR00VztDsProdKrspOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ExtractDivzcommsR00VztDsProdKrspOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vwVztDataShareCnxRespDbSchema = options.getVwVztDataShareCnxRespDbSchema();
		ValueProvider<Integer> batchId = options.getBatchId();
		Supplier<PGPCryptorInfo> cryptorInfoSupplierForVZT = HashiCorpVaultPGPCryptorInfoSupplier.create()
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

		// Read from VW_VZT_DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VW_VZT_DATASHARE view");
		PCollection<Row> readFromViewRows = new JdbcReader<Row>("Read from table VZT-DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(ExtractDivzcommsR00VztDsProdKrspSql.SELECT_FROM_VW_VZT_DATASHARE,
						Arrays.asList(vwVztDataShareCnxRespDbSchema)),
				(preparedStatement) -> {
					ExtractDivzcommsR00VztDsProdKrsHelper.setParamExtractDivzcommsR00VztDsProdKrsRow(preparedStatement,
							batchId);
				}, (resultSet) -> {
					return ExtractDivzcommsR00VztDsProdKrsHelper.readExtractDivzcommsR00VztDsProdKrsRow(resultSet);
				}).execute(pipeline);
		readFromViewRows.setRowSchema(ExtractDivzcommsR00VztDsProdKrstSchema.ExtractVzDsFuzzyReqExtract());

		ValueProvider<String> outputFileName = options.getOutputFileName();

		// Generate Header
		PCollectionView<String> fileHeaderView = readFromViewRows.apply("Generate Header",
				new HeaderFn<Row>(outputFileName));

		// Generate Trailer
		PCollectionView<String> fileTrailerView = readFromViewRows.apply("Generate Trailer",
				new TrailerFn<Row>(outputFileName));

		// Extract DIVZCOMMSR00-VZT_DS_PROD.KRSP.${VZT_SEQ}.${DATE}${TIME} file
		log.info("Applying transform(s): Extract DIVZCOMMSR00-VZT_DS_PROD.KRSP.${VZT_SEQ}.${DATE}${TIME} Onput file");
		readFromViewRows.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplierForVZT, outputFileName,
				options.getOutputPath(), WriteFileConfigHelper.getConfigForExtractDivzcommsR00VztDsProdKrspF(), "|",
				null, null, false, fileHeaderView, fileTrailerView));

		return pipeline;

	}

}
