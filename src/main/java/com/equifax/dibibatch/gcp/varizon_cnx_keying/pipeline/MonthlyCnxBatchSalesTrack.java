package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.Arrays;
import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.MonthlyCnxBatchSalesTrackHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.MonthlyCnxBatchSalesTrackOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.MonthlyCnxBatchSalesSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.MonthlyCnxBatchSalesSql;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.DBConnectionInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcCommons;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.JdbcReader;

public class MonthlyCnxBatchSalesTrack {

	public static void main(String[] args) {
		MonthlyCnxBatchSalesTrackOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(MonthlyCnxBatchSalesTrackOptions.class);
		executePipeline(options).run();
	}

	public static Pipeline executePipeline(MonthlyCnxBatchSalesTrackOptions options) {

		final Logger log = LoggerFactory.getLogger(MonthlyCnxBatchSalesTrack.class);

		Pipeline pipeline = Pipeline.create(options);

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		// Getting dataShareDbConInfo
		DBConnectionInfo dbConnectionInfo = DBConnectionInfo.create().withJdbcIOOptions(options);

		String verizonDatashareSchema = options.getVerizonDataShareSchema();

		log.info("Applying transform: Retrieve Records From (1).VW_MNTHLY_CNXBTCH_SALES_TRACK");

		PCollection<Row> cnxbatchSales = new JdbcReader<Row>(
				"Retrieve Records From {{1}}.VW_MNTHLY_CNXBTCH_SALES_TRACK", dbConnectionInfo,
				SerializableCoder.of(Row.class),
				JdbcCommons.applySchemaToQuery(MonthlyCnxBatchSalesSql.SELECT_VW_MNTHLY_CNXBTCH_SALES_TRACK,
						Arrays.asList(verizonDatashareSchema)),
				(preparedStatement) -> {

				}, (resultSet) -> {
					return MonthlyCnxBatchSalesTrackHelper.rowBuilderMonthlyCnxBatchSales(resultSet);
				}).execute(pipeline);
		if (cnxbatchSales == null) {
			log.info("cnxbatchSales is null");
		}
		cnxbatchSales.setRowSchema(MonthlyCnxBatchSalesSchema.selectMonthlyCnxBatchSalesTrack());
		
		String headers="YEAR,CURR_MONTH,VZ_CNX_Offline_MONTHLY_COUNT,RUNNING_SUM";

		cnxbatchSales.apply(new PGPEncryptFileIO<Row>(cryptorInfoSupplier, options.getWriteToXlsFileName(),
				options.getReportGenPath(), MonthlyCnxBatchSalesTrackHelper.getMonthyCnxBatchSalesConfig(), ",", headers, null, false));
        
		ValueProvider<String> date=options.getDate();
        
		PCollection<String> monthlyCnxBatchSalesEmailContent = pipeline.apply("", Create.of("monthlyCnxBatchSalesEmailContent")).apply("",
				ParDo.of(new EmailFn(date)));

		log.info("Applying transform(s): Write to Email file");
		monthlyCnxBatchSalesEmailContent.apply(new PGPEncryptFileIO<String>(cryptorInfoSupplier,
				options.getSummaryUpdateFileName(), options.getReportGenPath(),
				MonthlyCnxBatchSalesTrackHelper.getMonthyCnxBatchSalesSummaryUpdate(), "", null, null, false));
		return pipeline;
	}
	
	static final class EmailFn extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;
		private ValueProvider<String> date;

		EmailFn(ValueProvider<String> date) {
			this.date=date;
		}

		@ProcessElement
		public void processElement(ProcessContext c) {
			StringBuilder sb = new StringBuilder();
			sb.append("Attached is the monthly Verizon offline batch CNX counts. VZT Monthly_CNX_Batch_Sales_Track on "
					+ this.date.get() + ".");
			sb.append("\n");
			sb.append("\n");
			c.output(sb.toString());
		}
	}

}
