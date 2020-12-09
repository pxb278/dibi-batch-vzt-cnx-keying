package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadTablesAsPerRowGeneratorHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadToVztBatchSummeryHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVztBatchSummeryStatsHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadTablesAsPerRowGeneratorOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadTablesAsPerRowGeneratorSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadTablesAsPerRowGeneratorSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchAndVztSummarySchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchAndVztSummarySql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSchema;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 8: Job_Vzt_Datashare_Resp_Sum_Upd											
*/

public class LoadTablesAsPerRowGeneratorPipeLine {
	private static final Logger log = LoggerFactory.getLogger(LoadVztDataShareTablePipeLine.class);
	public volatile static Boolean isUpdated = false;

	public static void main(String[] args) {

		LoadTablesAsPerRowGeneratorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadTablesAsPerRowGeneratorOptions.class);

		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings({ "serial", "null" })
	static Pipeline executePipeline(LoadTablesAsPerRowGeneratorOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);
		String vztStatDbSchema = options.getVztStatDbSchema();
		String vztBatchDbSchema = options.getVztBatchDbSchema();
		String vztSummaryDbSchema = options.getVztSummaryDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		ValueProvider<String> vztDSRespFileName = options.getVztDSRespFileName();
		ValueProvider<Integer> vztDSRespFileCount = options.getVztDSRespFileCount();
		ValueProvider<Integer> vztHitCount = options.getVztHitCount();
		ValueProvider<Integer> vztNoHitCount = options.getVztNoHitCount();
		ValueProvider<Integer> batchId = options.getBatchId();
		ValueProvider<Integer> ultimateBatchId = options.getUltimateBatchId();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);

		/*
		 * // Read from VZT-DATASHARE Table
		 * log.info("Applying transform(s): Retrieve rows from VZT-DATASHARE table");
		 * PCollection<Row> readFromTablesRows = new
		 * JdbcReader<Row>("Read from table VZT-DATASHARE", dbConnectionInfo,
		 * SerializableCoder.of(Row.class),
		 * JdbcCommons.applySchemaToQuery(LoadTablesAsPerRowGeneratorSql.
		 * SELECT_VZT_SUMMARY, Arrays.asList(vztSummaryDbSchema)), (preparedStatement)
		 * -> {
		 * 
		 * }, (resultSet) -> { return
		 * LoadTablesAsPerRowGeneratorHelper.readForLoadVztBatchSummeryRow(resultSet);
		 * }).execute(pipeline);
		 * readFromTablesRows.setRowSchema(LoadTablesAsPerRowGeneratorSchema.
		 * vztBatchSummary());
		 */

		// Read from SELECT_DUMMY_ROWS
		log.info("Applying transform(s): Retrieve rows from SELECT_DUMMY_ROWS table");
		// Read from VZT-DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VZT-DATASHARE table");
		PCollection<Row> readFromDummyTablesRows = new JdbcReader<Row>("Read from table VZT-DATASHARE",
				dbConnectionInfo, SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(LoadVztBatchSummeryStatsSql.SELECT_FROM_VZT_DATASHARE,
						Arrays.asList(vztBatchDbSchema)),
				(preparedStatement) -> {
					LoadVztBatchSummeryStatsHelper.setParameterLoadVztBatchSummeryStatsRow(preparedStatement,
							currentTimestamp, batchId);
				}, (resultSet) -> {
					return LoadVztBatchSummeryStatsHelper.readLoadVztBatchSummeryStatsRow(resultSet);
				}).execute(pipeline);
		readFromDummyTablesRows.setRowSchema(LoadVztBatchSummeryStatsSchema.readRowsToLoadVztBatchSummeryStatstables());
		// Insert Unprocessed row into table VZT_BATCH_SUMMARY
		log.info("Applying transform: Upsert into table 'VZT_BATCH_SUMMARY'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_BATCH_SUMMARY'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadTablesAsPerRowGeneratorSql.UPSERT_VZT_SUMMARY,
					Arrays.asList(vztSummaryDbSchema));
		}, (row, preparedStatement) -> {
			LoadTablesAsPerRowGeneratorHelper.insertVztBatchSummary(row, preparedStatement, currentTimestamp,
					ultimateBatchId, batchId, vztDSRespFileName, vztDSRespFileCount);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZT_BATCH_SUMMARY"))
						.executeForOnlyRejectedOutput(readFromDummyTablesRows)
						.setRowSchema(LoadTablesAsPerRowGeneratorSchema.vztBatchSummary());

		TupleTag<Row> rejectedRowsTagForVztStat = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_BATCH_SUMMARY");
		TupleTag<Row> unprocessedRowsTagForVztStat = JdbcWriter
				.newTupleTag("CaptureUnprocessedForUpdate_VZT_BATCH_SUMMARY");

// Insert Unprocessed row into table VZT_BATCH

		log.info("Applying transform: Insert  into table 'VZT_BATCH'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_BATCH'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadTablesAsPerRowGeneratorSql.INSERT_INTO_VZT_BATCH,
					Arrays.asList(vztBatchDbSchema));
		}, (row, preparedStatement) -> {

			LoadTablesAsPerRowGeneratorHelper.insertVztBatch(row, preparedStatement, currentTimestamp, ultimateBatchId,
					batchId, vztDSRespFileCount, vztDSRespFileName);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZT_BATCH")).execute(readFromDummyTablesRows);

		// Update into table VZT_STAT
		log.info("Applying transform: Update into table 'VZT_STAT'");
		new JdbcWriter<Row>("Update into table 'VZT_STAT'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadTablesAsPerRowGeneratorSql.UPDATE_VZT_STAT,
					Arrays.asList(vztStatDbSchema));
		}, (row, preparedStatement) -> {
			LoadTablesAsPerRowGeneratorHelper.updateVztStat(row, preparedStatement, ultimateBatchId, vztHitCount,
					vztNoHitCount);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTagForVztStat,
				unprocessedRowsTagForVztStat).execute(readFromDummyTablesRows);

		log.info("Running the pipeline");
		return pipeline;
	}
}