package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
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

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractStDsFuzzyRespToLoadVztDataShareHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractStDsFuzzyRespToLoadVztDataShareOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractStDsFuzzyRespToLoadVztDataShareSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractStDsFuzzyRespToLoadVztDataShareSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.ApplyJoinSchemaFn;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.ExtractStDsFuzzyRespToLoadVztDataShareTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 3: Job_DatashareCnxidFuzzyResponseUpdate																						
*/
public class ExtractStDsFuzzyRespToLoadVztDataSharePipeLine {

	private static final Logger log = LoggerFactory.getLogger(ExtractStDsFuzzyRespToLoadVztDataSharePipeLine.class);

	public static void main(String[] args) {
		ExtractStDsFuzzyRespToLoadVztDataShareOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractStDsFuzzyRespToLoadVztDataShareOptions.class);

		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ExtractStDsFuzzyRespToLoadVztDataShareOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		String vztDatashareDbSchema = options.getVztDatashareDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		ValueProvider<Integer> batchId = options.getBatchId();
		ValueProvider<String> processName = options.getProcessName();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info(
				"Applying transform(s): (1) Read from ST_DS_FUZZY_RESP_${VERIZON_BATCHID} File (2) Convert to Schema Row and Validate");

		PCollectionTuple extractFileRowsTuple = pipeline
				.apply("Read from ST_DS_FUZZY_RESP_${VERIZON_BATCHID}} File",
						PGPDecryptFileIO.read().from(options.getInput()).withCryptorInfoSupplier(cryptorInfoSupplier))
				.apply("Convert to Schema Row and Validate",
						ParDo.of(new ExtractStDsFuzzyRespToLoadVztDataShareTransform(validRowsTag, invalidRowsTag))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> extractFileValidRows = extractFileRowsTuple.get(validRowsTag.getId());
		extractFileValidRows.setRowSchema(
				ExtractStDsFuzzyRespToLoadVztDataShareSchema.extractStDsFuzzyRespToLoadVztDataShareFile());

		PCollectionTuple extractFileTuples = PCollectionTuple.of("A", extractFileValidRows).and("B",
				extractFileValidRows);
		PCollection<Row> rowsAfterRightJoin = extractFileTuples
				.apply("Right Outer Join",
						SqlTransform.query(ExtractStDsFuzzyRespToLoadVztDataShareSql.SELECT_JOIN_SOURCE_DS_ID))
				.apply("Set Joined Table  schema", ParDo.of(new ApplyJoinSchemaFn()));
		rowsAfterRightJoin.setRowSchema(
				ExtractStDsFuzzyRespToLoadVztDataShareSchema.extractStDsFuzzyRespToLoadVztDataShareFile());

		TupleTag<Row> rejectedRowsTag = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_DATASHARE");
		TupleTag<Row> unprocessedRowsTag = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_DATASHARE");

		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZT_DATASHARE'");
		new JdbcWriter<Row>("Update into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(ExtractStDsFuzzyRespToLoadVztDataShareSql.UPDATE_VZT_DATASHARE_TABLE,
					Arrays.asList(vztDatashareDbSchema));
		}, (row, preparedStatement) -> {
			ExtractStDsFuzzyRespToLoadVztDataShareHelper.updateVztDataShareRowFromStDsOut(row, preparedStatement,
					currentTimestamp, batchId, processName);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTag, unprocessedRowsTag)
				.execute(rowsAfterRightJoin);

		return pipeline;

	}

}
