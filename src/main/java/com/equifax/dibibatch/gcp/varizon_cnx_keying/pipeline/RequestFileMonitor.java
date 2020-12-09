package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.RequestResponseMonitorHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.RequestFileMonitorOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.RequestResponseFileMonitorSql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

public class RequestFileMonitor {

	public static void main(String[] args) {

		RequestFileMonitorOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(RequestFileMonitorOptions.class);

		Pipeline pipeline = executePipeline(options);
		
		pipeline.run();
	}

	@SuppressWarnings("serial")
	static Pipeline executePipeline(RequestFileMonitorOptions options) {

		final Logger log = LoggerFactory.getLogger(RequestFileMonitor.class);
		Pipeline pipeline = Pipeline.create(options);

		String schemaNameVztBatchSummary = options.getVztBatchSummarySchemaName();

		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);


		PCollection<Double> rowsdateDiff = new JdbcReader<Double>("Retrieve Records From VZT_BATCH_SUMMARY",
				dbConnectionInfo, SerializableCoder.of(Double.class),
				JdbcCommons.applySchemaToQuery(RequestResponseFileMonitorSql.SELECT_REQUEST_FILE_DATE_DIFF,
						Arrays.asList(schemaNameVztBatchSummary)),
				(preparedStatement) -> {
				}, (resultSet) -> {
					return RequestResponseMonitorHelper.rowBuilderVztBatchSummaryMonitor(resultSet);
				}).execute(pipeline);

		
		/*
		 * Request File Monitor
		 */

		PCollection<String> requestFileAlert = rowsdateDiff.apply("", ParDo.of(new EmailFn()));

		// Write Delta Statistics to Email file
		log.info("Applying transform(s): Write Request File Alert Email file");
		requestFileAlert.apply(
				new PGPEncryptFileIO<String>(cryptorInfoSupplier, options.getEmailFileName(), options.getOutputPath(),
						getConfigForRequestAlertEmailContent(), "", null, null, false));
		
		
		return pipeline;

	}
	
	static final class EmailFn extends DoFn<Double, String> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			Double dateDiff = c.element();
			
			Double noticeCount = (dateDiff - 2) / 1;
			
			if(dateDiff > 3) {
				StringBuilder sb = new StringBuilder();

				sb.append("MAIL_SUB::ALERT: Verizon CNX Consumer Request Process Files Not Received/Delayed - Notice " + noticeCount.intValue());
				sb.append("\n");
				sb.append("\n");
				sb.append("Alert: Verizon CNX Consumer Process Request has not received the files from Verizon for over 3 days.This is day ");
				sb.append(noticeCount.intValue());
				sb.append(" of the notice");
				sb.append("\n");
				sb.append("\n");
				sb.append("Thanks");
				sb.append("\n");
				sb.append("Equifax DI Team ");

				c.output(sb.toString());
			}

		}

	}
	
	private static Map<String, PGPEncryptFileIO.FieldFn<String>> getConfigForRequestAlertEmailContent() {

		Map<String, PGPEncryptFileIO.FieldFn<String>> config = new LinkedHashMap<>();

		config.put("", (c) -> c.element());

		return config;
	}

}
