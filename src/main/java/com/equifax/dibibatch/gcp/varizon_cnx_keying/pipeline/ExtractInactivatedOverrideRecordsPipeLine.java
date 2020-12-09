package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 5: JOB_DatashareInactiveOverrideConneuxKeyExtract	*/

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractInactivatedOverrideRecordsHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractInactivatedOverrideRecordsOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractInactivatedOverrideRecordsSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractInactivatedOverrideRecordsSql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

public class ExtractInactivatedOverrideRecordsPipeLine {
	private static final Logger log = LoggerFactory.getLogger(ExtractDICNX0000002EfxVzwFilePipeLine.class);
	private static final String HEADER = "DATASHRE_ID|OLD_BEST_CNX_ID|EFX_CNX_ID|EFX_HHLD_ID|EFX_ADDR_ID|EFX_OVERRIDE_CNX_ID|EFX_OVERRIDE_HHLD_ID";

	public static void main(String[] args) {
		ExtractInactivatedOverrideRecordsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractInactivatedOverrideRecordsOptions.class);

		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();

	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ExtractInactivatedOverrideRecordsOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);
		ValueProvider<Integer> batchId = options.getBatchId();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vwVztDatashareDbSchema = options.getVwVztDatashareDbSchema();
		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		log.info("Applying transform(s): (1) Read from VW_VZT_DATASHARE File (2) Convert to Schema Row and Validate");

		// Read from VW_VZT_DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VW_VZT_DATASHARE view");
		PCollection<Row> readFromTablesRows = new JdbcReader<Row>("Read from table VW_VZT_DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(ExtractInactivatedOverrideRecordsSql.SELECT_FROM_VW_VZT_DATASHARE,
						Arrays.asList(vwVztDatashareDbSchema)),
				(preparedStatement) -> {
					ExtractInactivatedOverrideRecordsHelper
							.setParametersExtractInactivatedOverrideRecordsRow(preparedStatement, batchId);
				}, (resultSet) -> {
					return ExtractInactivatedOverrideRecordsHelper.readExtractInactivatedOverrideRecordsRow(resultSet);
				}).execute(pipeline);
		readFromTablesRows.setRowSchema(ExtractInactivatedOverrideRecordsSchema.ExtractInactivatedOverrideRecords());

		// Extract Inactivated_Override_Records.txt file
		log.info("Applying transform(s): Extract Inactivated_Override_Records.txt Onput file");
		readFromTablesRows.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplier, options.getOutputFileName(),
				options.getOutputPath(), WriteFileConfigHelper.getConfigForExtractInactivatedOverrideRecordsFile(), "|",
				HEADER, null, false));

		return pipeline;

	}

}
