package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.RequestResponseMonitorHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ResponseFileMonitorOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.RequestResponseFileMonitorSql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

public class ResponseFileMonitor {

	public static void main(String[] args) {

		ResponseFileMonitorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ResponseFileMonitorOptions.class);

		Pipeline pipeline = executePipeline(options);
		
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(ResponseFileMonitorOptions options) {

		final Logger log = LoggerFactory.getLogger(ResponseFileMonitor.class);
		Pipeline pipeline = Pipeline.create(options);

		String schemaNameVztBatchSummary = options.getVztBatchSummarySchemaName();

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);


		PCollection<Row> rowsBatchSummary = new JdbcReader<Row>("Retrieve Records From VZT_BATCH_SUMMARY",
				dbConnectionInfo, SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(RequestResponseFileMonitorSql.SELECT_RESPONSE_FILE_BATCH_ID_FILE_NAME,
						Arrays.asList(schemaNameVztBatchSummary)),
				(preparedStatement) -> {
				}, (resultSet) -> {
					return RequestResponseMonitorHelper.rowBuilderVztBatchSummaryResponseMonitor(resultSet);
				}).execute(pipeline);

		
		/*
		 * Response File Monitor
		 */

		PCollection<String> responseFileAlert = rowsBatchSummary.apply("", ParDo.of(new EmailFn()));

		// Write Delta Statistics to Email file
		log.info("Applying transform(s): Write Response File Alert Email file");
		responseFileAlert.apply(
				new PGPEncryptFileIO<String>(cryptorInfoSupplier, options.getEmailFileName(), options.getOutputPath(),
						getConfigForResponseAlertEmailContent(), "", null, null, false));
		
		
		return pipeline;

	}
	
	static final class EmailFn extends DoFn<Row, String> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			Row row = c.element();
			
			Integer ultimateBatchId = row.getInt32("ULTIMATE_BATCH_ID");
			String cnxInputFileName = row.getString("CNX_INPUT_FILE_NAME");
			
			if(cnxInputFileName != null) {
				StringBuilder sb = new StringBuilder();

				sb.append("MAIL_SUB::ALERT: CNX Response file has not been received for ultimate batch id " + ultimateBatchId);
				sb.append("\n");
				sb.append("\n");
				sb.append("Alert mail for pending VZT Key response send file " + getCurrentDate());
				sb.append("\n");
				sb.append("--------------------------------------------------- ");
				sb.append("\n");
				sb.append(" CNX Request File Name = " + cnxInputFileName);
				sb.append("\n");
				sb.append("--------------------------------------------------- ");
				sb.append("\n");

				c.output(sb.toString());
			}

		}

	}
	
	private static Map<String, PGPEncryptFileIO.FieldFn<String>> getConfigForResponseAlertEmailContent() {

		Map<String, PGPEncryptFileIO.FieldFn<String>> config = new LinkedHashMap<>();

		config.put("", (c) -> c.element());

		return config;
	}

	private static String getCurrentDate() {
		final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		String sDate = sdf.format(timestamp);
		return sDate;

	}

}
