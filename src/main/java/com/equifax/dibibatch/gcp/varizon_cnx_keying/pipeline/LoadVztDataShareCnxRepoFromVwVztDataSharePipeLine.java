package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.LoadVztDataShareCnxRepoFromVwVztDataShareHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztDataShareCnxRepoFromVwVztDataShareOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareCnxRepoFromVwVztDataShareSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.VzDataShareCnxRepoDecryptTransformation;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 9: VZ_Datashare_Cnx_Repo_Load*/

public class LoadVztDataShareCnxRepoFromVwVztDataSharePipeLine {

	private static final Logger log = LoggerFactory.getLogger(LoadVztDataShareCnxRepoFromVwVztDataSharePipeLine.class);

	public static void main(String[] args) {
		LoadVztDataShareCnxRepoFromVwVztDataShareOptions options = PipelineOptionsFactory.fromArgs(args)
				.withValidation().as(LoadVztDataShareCnxRepoFromVwVztDataShareOptions.class);
		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadVztDataShareCnxRepoFromVwVztDataShareOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vwVztDatashareDbSchema = options.getVwVztDatashareDbSchema();
		String vzDataShareCnxRepodbSchema = options.getVzDataShareCnxRepodbSchema();
		ValueProvider<Integer> batchId = options.getBatchId();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		BarricaderInfo barricaderInfo = BarricaderInfo.create().withBarricaderOptions(options);

		// Read from VW_VZT_DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VW_VZT_DATASHARE view");
		PCollection<Row> readFromViewRows = new JdbcReader<Row>("Read from table  VW_VZT_DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(
						LoadVztDataShareCnxRepoFromVwVztDataShareSql.SELECT_FROM_VW_VZT_DATASHARE,
						Arrays.asList(vwVztDatashareDbSchema)),
				(preparedStatement) -> {
					LoadVztDataShareCnxRepoFromVwVztDataShareHelper
							.setParamLoadVztDataShareCnxRepoFromVwVztDataShareRow(preparedStatement, batchId);
				}, (resultSet) -> {
					return LoadVztDataShareCnxRepoFromVwVztDataShareHelper
							.readLoadVztDataShareCnxRepoFromVwVztDataShareRow(resultSet);
				}).execute(pipeline);
		readFromViewRows.setRowSchema(LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepo());

		PCollection<Row> rowDecrypt = readFromViewRows.apply(" Decrypt the  Data",
				ParDo.of(new VzDataShareCnxRepoDecryptTransformation(barricaderInfo)));

		rowDecrypt.setRowSchema(LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepo());

		TupleTag<Row> rejectedRowsTag = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZ_DATASHARE_CNX_REPO");
		TupleTag<Row> unprocessedRowsTag = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZ_DATASHARE_CNX_REPO");

		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZ_DATASHARE_CNX_REPO'");
		PCollectionTuple updateVztDataShareRows = new JdbcWriter<Row>("Update into table 'VZ_DATASHARE_CNX_REPO'",
				dbConnectionInfo, () -> {
					return JdbcCommons.applySchemaToQuery(
							LoadVztDataShareCnxRepoFromVwVztDataShareSql.UPDATE_VZ_DATASHARE_CNX_REPO_TABLE,
							Arrays.asList(vzDataShareCnxRepodbSchema));
				}, (row, preparedStatement) -> {
					LoadVztDataShareCnxRepoFromVwVztDataShareHelper.updateLoadVztDataShareCnxRepoFromVwVztDataShareRow(
							row, preparedStatement, currentTimestamp, batchId);
				}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTag,
				unprocessedRowsTag).execute(rowDecrypt);

		PCollection<Row> unprocessedRows = updateVztDataShareRows.get(unprocessedRowsTag.getId());
		unprocessedRows.setRowSchema(
				LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepoToInsertUnProcessedRows());

		// Insert Unprocessed row into table VZT_DATASHARE
		log.info("Applying transform: Insert Unprocessed into table 'VZ_DATASHARE_CNX_REPO'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZ_DATASHARE_CNX_REPO'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(
					LoadVztDataShareCnxRepoFromVwVztDataShareSql.INSERT_VZ_DATASHARE_CNX_REPO_TABLE,
					Arrays.asList(vzDataShareCnxRepodbSchema));
		}, (row, preparedStatement) -> {
			LoadVztDataShareCnxRepoFromVwVztDataShareHelper.insertLoadVztDataShareCnxRepoFromVwVztDataShare(row,
					preparedStatement, currentTimestamp, batchId);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZ_DATASHARE_CNX_REPO"))
						.executeForOnlyRejectedOutput(unprocessedRows)
						.setRowSchema(LoadVztDataShareCnxRepoFromVwVztDataShareSchema.LoadVztDataShareCnxRepo());

		return pipeline;

	}
}
