package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.VztDataShareTableHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.LoadVztDataShareTablePipeLineOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.CustomSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.LoadVztDataShareSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.VztDsProdKreqFileTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcWriter;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_file_load.sh											
Job1 : Job_Vzt_Datashare_Req_File_Load											
*/
public class LoadVztDataShareTablePipeLine {

	private static final Logger log = LoggerFactory.getLogger(LoadVztDataShareTablePipeLine.class);
	public static int count =0;

	public static void main(String[] args) {

		LoadVztDataShareTablePipeLineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(LoadVztDataShareTablePipeLineOptions.class);

		Pipeline pipeline = executePipeline(options);
		log.info("Running the pipeline");
		pipeline.run();
		executePipeline(options);
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(LoadVztDataShareTablePipeLineOptions options) {

		log.info("Creating the pipeline");
		Pipeline pipeline = Pipeline.create(options);
		String dbSchema = options.getDbSchema();
		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		ValueProvider<String> processName = options.getProcessName();
		ValueProvider<Integer> batchId = options.getBatchId();
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		BarricaderInfo barricaderInfo = BarricaderInfo.create().withBarricaderOptions(options);
		Supplier<PGPCryptorInfo> cryptorInfoSupplierMFTToExtractSrcFile = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withHashiCorpVaultOptions(options).withPgpCryptionEnabled(options.getMftPgpCryptionEnabled())
				.withPgpSigned(options.getMftPgpSigned()).withPgpArmored(options.getMftPgpArmored())
				.withPgpObjectFactory(options.getMftPgpObjectFactory())
				.withPgpVaultPassphraseFieldName(options.getMftPgpVaultPassphraseFieldName())
				.withPgpVaultPrivatekeyFieldName(options.getMftPgpVaultPrivatekeyFieldName())
				.withPgpVaultPublickeyFieldName(options.getMftPgpVaultPublickeyFieldName());
		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info("Applying transform(s): (1) Read from file (2) Convert to Schema Row and Validate");

		PCollectionTuple inputVztDsProdKreqFileRowsTuple = pipeline
				.apply("Read from VZT_DS_PROD.KREQ* File",
						PGPDecryptFileIO.read().from(options.getInput()).withCryptorInfoSupplier(cryptorInfoSupplierMFTToExtractSrcFile))
				.apply("Convert to Schema Row and Validate",
						ParDo.of(new VztDsProdKreqFileTransform(barricaderInfo, validRowsTag, invalidRowsTag))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> inputVztDsProdKreqFileValidRows = inputVztDsProdKreqFileRowsTuple.get(validRowsTag.getId());
		inputVztDsProdKreqFileValidRows.setRowSchema(CustomSchema.extrDataForVztDataShareTable());
		
		
		PCollection<Row> fRow = inputVztDsProdKreqFileValidRows.apply(
				  "Compute F Rows",                     // the transform name
				  ParDo.of(new DoFn<Row, Row>() {    // a DoFn as an anonymous inner class instance
				      @ProcessElement
				      public void processElement(@Element Row row, OutputReceiver<Row> outRow) {
				    	  if(row.getString("KEYING_FLAG").equalsIgnoreCase("F")) {
				    		  	outRow.output(row);
				    	  }
				      }
				    }));

		TupleTag<Row> rejectedRowsTagTargetOne = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_DATASHARE");
		TupleTag<Row> unprocessedRowsTagTargetOne = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_DATASHARE");

		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZT_DATASHARE'");
		PCollectionTuple updateVztDataShareRowsTargetOne = new JdbcWriter<Row>("Update into table 'VZT_DATASHARE' For Flag F",
				dbConnectionInfo, () -> {
					return JdbcCommons.applySchemaToQuery(LoadVztDataShareSql.UPDATE_VZT_DATASHARE_TABLE_KEYING_FLAG_F,
							Arrays.asList(dbSchema));
				}, (row, preparedStatement) -> {
					VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagF(row, preparedStatement,
							currentTimestamp, batchId, processName);
				}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTagTargetOne,
				unprocessedRowsTagTargetOne).execute(fRow);

		PCollection<Row> unprocessedRowsTargetOne = updateVztDataShareRowsTargetOne
				.get(unprocessedRowsTagTargetOne.getId());
		unprocessedRowsTargetOne.setRowSchema(CustomSchema.extrDataForVztDataShareTable());

		// Insert Unprocessed row into table VZT_DATASHARE
		log.info("Applying transform: Insert Unprocessed into table 'VZT_DATASHARE'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztDataShareSql.INSERT_VZT_DATASHARE_TABLE_KEYING_FLAG_F,
					Arrays.asList(dbSchema));
		}, (row, preparedStatement) -> {
			VztDataShareTableHelper.insertVztDataShareRowWithKeyingFlagF(row, preparedStatement, currentTimestamp,
					batchId,processName);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZT_DATASHARE"))
						.executeForOnlyRejectedOutput(unprocessedRowsTargetOne)
						.setRowSchema(CustomSchema.loadVztDataShareTableForKEYING_FLAG_F());

		TupleTag<Row> rejectedRowsTagTargetTwo = JdbcWriter.newTupleTag("CaptureRejectedForUpdate_VZT_DATASHARE");
		TupleTag<Row> unprocessedRowsTagTargetTwo = JdbcWriter.newTupleTag("CaptureUnprocessedForUpdate_VZT_DATASHARE");
				
		PCollection<Row> tRow = inputVztDsProdKreqFileValidRows.apply(
				  "Compute T Rows",                     // the transform name
				  ParDo.of(new DoFn<Row, Row>() {    // a DoFn as an anonymous inner class instance
				      @ProcessElement
				      public void processElement(@Element Row row, OutputReceiver<Row> outRow) {
				    	  if(row.getString("KEYING_FLAG").equalsIgnoreCase("T")) {
				    		  	outRow.output(row);
				    	  }
				      }
				    }));

		// Update into table VZT_DATASHARE
		log.info("Applying transform: Update into table 'VZT_DATASHARE'");
		PCollectionTuple updateVztDataShareRowsTargetTwo = new JdbcWriter<Row>("Update  table 'VZT_DATASHARE For Flag T'",
				dbConnectionInfo, () -> {
					return JdbcCommons.applySchemaToQuery(LoadVztDataShareSql.UPDATE_VZT_DATASHARE_TABLE_KEYING_FLAG_T,
							Arrays.asList(dbSchema));
				}, (row, preparedStatement) -> {
					VztDataShareTableHelper.updateVztDataShareRowWithKeyingFlagT(row, preparedStatement,
							currentTimestamp, batchId, processName);
				}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class), rejectedRowsTagTargetTwo,
				unprocessedRowsTagTargetTwo).execute(tRow);

		PCollection<Row> unprocessedRowsTargetTwo = updateVztDataShareRowsTargetTwo
				.get(unprocessedRowsTagTargetTwo.getId());
		unprocessedRowsTargetTwo.setRowSchema(CustomSchema.extrDataForVztDataShareTable());

		// Insert Unprocessed row into table VZT_DATASHARE
		log.info("Applying transform: Insert Unprocessed into table 'VZT_DATASHARE'");
		new JdbcWriter<Row>("Insert Unprocessed into table 'VZT_DATASHARE'", dbConnectionInfo, () -> {
			return JdbcCommons.applySchemaToQuery(LoadVztDataShareSql.INSERT_VZT_DATASHARE_TABLE_KEYING_FLAG_T,
					Arrays.asList(dbSchema));
		}, (row, preparedStatement) -> {
			VztDataShareTableHelper.insertVztDataShareRowWithKeyingFlagT(row, preparedStatement, currentTimestamp,
					batchId, processName);
		}, jdbcExecutionInfo.getWriteBatchSize(), SerializableCoder.of(Row.class),
				JdbcWriter.newTupleTag("CaptureRejectedForInsert_VZT_DATASHARE"))
						.executeForOnlyRejectedOutput(unprocessedRowsTargetTwo)
						.setRowSchema(CustomSchema.loadVztDataShareTableForKEYING_FLAG_T());

		log.info("Running the pipeline");
		return pipeline;
	}
}
