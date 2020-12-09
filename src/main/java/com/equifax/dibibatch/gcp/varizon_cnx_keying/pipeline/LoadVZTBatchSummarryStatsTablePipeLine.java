package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVztBatchSummeryStatsHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVZTBatchSummarryStatsTablePipeLineOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztBatchSummeryStatsSql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_file_load.sh											
Job 3: Job_Vzt_Datashare_Req_Sum_Upd*/

public class LoadVZTBatchSummarryStatsTablePipeLine {

	private static final Logger log = LoggerFactory.getLogger(LoadVZTBatchSummarryStatsTablePipeLine.class);

	public static void main(String[] args) {

		LoadVZTBatchSummarryStatsTablePipeLineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVZTBatchSummarryStatsTablePipeLineOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
		executePipeline(options);
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadVZTBatchSummarryStatsTablePipeLineOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vztDataShareDbSchema = options.getVztDataShareDbSchema();
		String vztBatchSummaryDbSchema = options.getVztBatchSummaryDbSchema();
		String vztBatchDbSchema = options.getVztBatchDbSchema();
		String vztBatchStatsDbSchema = options.getVztBatchStatsDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		ValueProvider<String> srcFileName = options.getSrcFileName();
		ValueProvider<Integer> batchId = options.getBatchId();
		ValueProvider<Integer> ultimateBatchId = options.getUltimateBatchId();
		ValueProvider<Integer> CNXExtractCount = options.getCNXExtractCount();
		ValueProvider<Integer> sourceVbbRecordCount = options.getSourceVbbRecordCount();
		ValueProvider<Integer> sourceVzbRecordCount = options.getSourceVzbRecordCount();
		ValueProvider<Integer> sourceVztRecordCount = options.getSourceVztRecordCount();
		ValueProvider<Integer> sourceVzwRecordCount = options.getSourceVzwRecordCount();
		ValueProvider<Integer> inputfileFinalCount = options.getInputfileFinalCount();
		ValueProvider<Integer> inputfileLiveCount = options.getInputfileLiveCount();
		ValueProvider<Integer> inputFileRecCount = options.getInputFileRecCount();
		ValueProvider<Integer> keyingFlagValueF = options.getKeyingFlagValueF();
		ValueProvider<Integer> keyingFlagValueT = options.getKeyingFlagValueT();
		ValueProvider<Integer> vztInsertCnt = options.getVztInsertCnt();
		ValueProvider<String> vztCnxReqFile = options.getVztCnxReqFile();
		ValueProvider<String> inputFileDate = options.getInputFileDate();

		// Read from VZT-DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VZT-DATASHARE table");
		PCollection<Row> readFromTablesRows = new JdbcReader<Row>("Read from table VZT-DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(LoadVztBatchSummeryStatsSql.SELECT_FROM_VZT_DATASHARE,
						Arrays.asList(vztDataShareDbSchema)),
				(preparedStatement) -> {
					LoadVztBatchSummeryStatsHelper.setParameterLoadVztBatchSummeryStatsRow(preparedStatement,
							inputFileDate, batchId);
				}, (resultSet) -> {
					return LoadVztBatchSummeryStatsHelper.readLoadVztBatchSummeryStatsRow(resultSet);
				}).execute(pipeline);
		readFromTablesRows.setRowSchema(LoadVztBatchSummeryStatsSchema.readRowsToLoadVztBatchSummeryStatstables());

		// Insert rows into table VZT_BATCH
		log.info("Applying transform: Insert Unprocessed into table 'VZT_DATASHARE'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztBatchSummeryStatsSql.INSERT_VZT_BATCH,
					Arrays.asList(vztBatchDbSchema));
		}, (row, preparedStatement) -> {
			LoadVztBatchSummeryStatsHelper.insertVztBatch(row, preparedStatement, currentTimestamp, batchId,
					srcFileName, ultimateBatchId);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("Insert_Into_VZT_BATCH")).executeForOnlyRejectedOutput(readFromTablesRows)
						.setRowSchema(LoadVztBatchSummeryStatsSchema.loadVztBatchTable());

		// Insert rows into table VZT_BATCH_SUMMARY
		log.info("Applying transform: Insert  into table 'VZT_BATCH_SUMMARY'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztBatchSummeryStatsSql.INSERT_VZT_BATCH_SUMMARY,
					Arrays.asList(vztBatchSummaryDbSchema));
		}, (row, preparedStatement) -> {
			LoadVztBatchSummeryStatsHelper.insertVztBatchSummary(row, preparedStatement, currentTimestamp, batchId,
					ultimateBatchId, srcFileName, inputFileRecCount, keyingFlagValueF, keyingFlagValueT,
					CNXExtractCount, vztCnxReqFile);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("Insert_Into_VZT_BATCH_SUMMARY"))
						.executeForOnlyRejectedOutput(readFromTablesRows)
						.setRowSchema(LoadVztBatchSummeryStatsSchema.loadVztBatchSummaryTable());

		// Insert rows into table VZT_BATCH_STATS
		log.info("Applying transform: Insert  into table 'VZT_BATCH_STATS'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztBatchSummeryStatsSql.INSERT_VZT_BATCH_STATS,
					Arrays.asList(vztBatchStatsDbSchema));
		}, (row, preparedStatement) -> {
			LoadVztBatchSummeryStatsHelper.insertVztBatchStats(row, preparedStatement, currentTimestamp, batchId,
					ultimateBatchId, srcFileName, inputFileRecCount, vztInsertCnt, sourceVbbRecordCount,
					sourceVzbRecordCount, sourceVztRecordCount, sourceVzwRecordCount, inputfileFinalCount,
					inputfileLiveCount);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("Insert_Into_VZT_BATCH_STATS")).executeForOnlyRejectedOutput(readFromTablesRows)
						.setRowSchema(LoadVztBatchSummeryStatsSchema.loadVztBatchStasTable());

		log.info("Running the pipeline");
		return pipeline;
	}
}
