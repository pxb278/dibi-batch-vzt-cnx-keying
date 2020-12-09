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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.ExtractVzDsFuzzyReqExtractHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractVzDsFuzzyReqExtractOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractVzDsFuzzyReqExtractSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractVzDsFuzzyReqExtractSql;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.ApplyUnionSchemaFn;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.VzDsFuzzyReqExtractDecryptTransformation;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.BarricaderInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.JdbcExecutionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_cnx_key_rcv.sh											
Job 2: Job_DatashareCnxidFuzzyExtract											
*/
public class ExtractVzDsFuzzyReqExtractPipeLine {
	private static final Logger log = LoggerFactory.getLogger(ExtractVzDsFuzzyReqExtractPipeLine.class);

	public static void main(String[] args) {
		ExtractVzDsFuzzyReqExtractOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractVzDsFuzzyReqExtractOptions.class);
		Pipeline pipeline = ExtractVzDsFuzzyReqExtractFile(options);
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline ExtractVzDsFuzzyReqExtractFile(ExtractVzDsFuzzyReqExtractOptions options) {

		log.info("Creating the pipeline");

		Pipeline pipeline = Pipeline.create(options);

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);
		JdbcExecutionInfo jdbcExecutionInfo = JdbcExecutionInfo.create().withJdbcExecutionOptions(options);
		String vwVztDatashareDbSchema = options.getVwVztDatashareDbSchema();
		BarricaderInfo barricaderInfo = BarricaderInfo.create().withBarricaderOptions(options);
		ValueProvider<Integer> batchId = options.getBatchId();
		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		// Read from VW_VZT_DATASHARE Table
		log.info("Applying transform(s): Retrieve rows from VW_VZT_DATASHARE view");
		PCollection<Row> readFromViewRows = new JdbcReader<Row>("Read from table VZT-DATASHARE", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(ExtractVzDsFuzzyReqExtractSql.SELECT_FROM_VW_VZT_DATASHARE,
						Arrays.asList(vwVztDatashareDbSchema)),
				(preparedStatement) -> {
					ExtractVzDsFuzzyReqExtractHelper.setExtractVzDsFuzzyReqExtractRow(preparedStatement, batchId);
				}, (resultSet) -> {
					return ExtractVzDsFuzzyReqExtractHelper.readExtractVzDsFuzzyReqExtractRow(resultSet);
				}).execute(pipeline);
		readFromViewRows.setRowSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtractToDecrypt());

		PCollection<Row> rowDecrypt = readFromViewRows.apply(" Decrypt the  Data",
				ParDo.of(new VzDsFuzzyReqExtractDecryptTransformation(barricaderInfo)));

		rowDecrypt.setRowSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtractToDecrypt());

		PCollectionTuple extractToUnionTuples = PCollectionTuple.of("A", rowDecrypt).and("B", rowDecrypt);
		PCollection<Row> rowsAfterUnionAll = extractToUnionTuples
				.apply("Union All Join", SqlTransform.query(ExtractVzDsFuzzyReqExtractSql.UNION_ALL))
				.apply("Set After Applying Union All schema", ParDo.of(new ApplyUnionSchemaFn()));
		rowsAfterUnionAll.setRowSchema(ExtractVzDsFuzzyReqExtractSchema.ExtractVzDsFuzzyReqExtract());

		// Extract VZ_DS_Fuzzy_Req_Extract_${VERIZON_BATCHID}.txt file
		log.info("Applying transform(s): Extract VZ_DS_Fuzzy_Req_Extract_${VERIZON_BATCHID}.txt Onput file");
		rowsAfterUnionAll.apply(
				new PGPEncryptFileIO<Row>(cryptorInfoSupplier, options.getOutputFileName(), options.getOutputPath(),
						WriteFileConfigHelper.getConfigForExtractVzDsFuzzyReqFile(), "|", null, null, false));

		log.info("Running the pipeline");
		return pipeline;

	}

}
