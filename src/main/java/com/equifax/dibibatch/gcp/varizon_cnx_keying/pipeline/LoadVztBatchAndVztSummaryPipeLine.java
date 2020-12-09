package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadToVztBatchSummeryHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztBatchAndVztSummaryOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchAndVztSummarySchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchAndVztSummarySql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 4: Job_Vzt_Datashare_Cnx_Resp_Sum_Upd	*/

public class LoadVztBatchAndVztSummaryPipeLine {

	private static final Logger log = LoggerFactory.getLogger(LoadVztBatchAndVztSummaryPipeLine.class);

	public static void main(String[] args) {

		LoadVztBatchAndVztSummaryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVztBatchAndVztSummaryOptions.class);

		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadVztBatchAndVztSummaryOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vztDataShareDbSchema = options.getVztDataShareDbSchema();
		String vztBatchSummaryDbSchema = options.getVztBatchSummaryDbSchema();
		String vztBatchDbSchema = options.getVztBatchDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		ValueProvider<String> vztCnxRespFile = options.getVztCnxRespFile();
		ValueProvider<Integer> batchId = options.getBatchId();
		ValueProvider<Integer> ultimateBatchId = options.getUltimateBatchId();
		ValueProvider<Integer> vztCnxRespFileCnt = options.getVztCnxRespFileCnt();

		// Read from VZT-DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VZT-DATASHARE table");
		PCollection<Row> readFromTablesRows = new JdbcReader<Row>("Read from table VZT-DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(LoadVztBatchAndVztSummarySql.SELECT_FROM_VZT_DATASHARE,
						Arrays.asList(vztDataShareDbSchema)),
				(preparedStatement) -> {
					LoadToVztBatchSummeryHelper.setParameterLoadVztBatchSummeryStatsRow(preparedStatement, batchId);
				}, (resultSet) -> {
					return LoadToVztBatchSummeryHelper.readForLoadVztBatchSummeryRow(resultSet);
				}).execute(pipeline);
		readFromTablesRows.setRowSchema(LoadVztBatchAndVztSummarySchema.readForRowsToLoadVztBatchSummerytables());

		// Insert rows into table VZT_BATCH
		log.info("Applying transform: Insert  into table 'VZT_BATCH'");
		new JdbcWriter<Row>("Insert into table 'VZT_BATCH'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztBatchAndVztSummarySql.INSERT_VZT_BATCH,
					Arrays.asList(vztBatchDbSchema));
		}, (row, preparedStatement) -> {
			LoadToVztBatchSummeryHelper.insertIntoVztBatch(row, preparedStatement, currentTimestamp, batchId,
					vztCnxRespFile, vztCnxRespFileCnt, ultimateBatchId);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("Insert_Into_VZT_BATCH")).executeForOnlyRejectedOutput(readFromTablesRows)
						.setRowSchema(LoadVztBatchAndVztSummarySchema.loadVztBatchTable());

		TupleTag<Row> rejectedRowsTag = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_SUMMARY");
		TupleTag<Row> unprocessedRowsTag = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_SUMMARY");
		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZT_SUMMARY'");
		PCollectionTuple updateVztDataSummaryRows = new JdbcWriter<Row>("Update into table 'VZT_SUMMARY'",
				dbConnectionInfo, () -> {
					return JdbcCommons.applySchemaToQuery(LoadVztBatchAndVztSummarySql.UPDATE_VZT_SUMMARY,
							Arrays.asList(vztBatchSummaryDbSchema));
				}, (row, preparedStatement) -> {
					LoadToVztBatchSummeryHelper.updateVztDataSummary(row, preparedStatement, currentTimestamp, batchId,
							vztCnxRespFile, vztCnxRespFileCnt, ultimateBatchId);
				}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTag,
				unprocessedRowsTag).execute(readFromTablesRows);

		PCollection<Row> unprocessedRows = updateVztDataSummaryRows.get(unprocessedRowsTag.getId());
		unprocessedRows.setRowSchema(LoadVztBatchAndVztSummarySchema.readForRowsToLoadVztBatchSummerytables());

		// Insert Unprocessed row into table VZT_DATASHARE
		log.info("Applying transform: Insert Unprocessed into table 'VZT_SUMMARY'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_SUMMARY'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztBatchAndVztSummarySql.INSERT_VZT_SUMMARY,
					Arrays.asList(vztBatchSummaryDbSchema));
		}, (row, preparedStatement) -> {
			LoadToVztBatchSummeryHelper.insertIntoVztDataSummary(row, preparedStatement, currentTimestamp, batchId,
					vztCnxRespFile, vztCnxRespFileCnt, ultimateBatchId);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZT_SUMMARY"))
						.executeForOnlyRejectedOutput(unprocessedRows)
						.setRowSchema(LoadVztBatchAndVztSummarySchema.loadVztSummary());

		log.info("Running the pipeline");
		return pipeline;
	}
}
