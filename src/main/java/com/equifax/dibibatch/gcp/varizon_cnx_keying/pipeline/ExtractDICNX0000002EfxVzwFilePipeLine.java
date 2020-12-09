package com.equifax.dibibatch.gcp.varizon_cnx_keying.pipeline;

import java.util.function.Supplier;

import org.apache.beam.sdk.Pipeline;
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

import com.equifax.dibibatch.gcp.varizon_cnx_keying.helper.WriteFileConfigHelper;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.options.ExtractDICNX0000002EfxVzwFileOptions;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.sql.ExtractDICNX0000002EfxVzwFileSchema;
import com.equifax.dibibatch.gcp.varizon_cnx_keying.transforms.ExtractDICNX0000002EfxVzwFileTransform;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.helper.HashiCorpVaultPGPCryptorInfoSupplier;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPDecryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.io.PGPEncryptFileIO;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;

/*Process : Verizon CNX Keying											
Script: vzt_datashare_file_load.sh											
Job 2: Job_Vzt_Datashare_Cnx_Key_Extract	*/										

public class ExtractDICNX0000002EfxVzwFilePipeLine {
	private static final Logger log = LoggerFactory.getLogger(ExtractDICNX0000002EfxVzwFilePipeLine.class);

	public static void main(String[] args) {
		ExtractDICNX0000002EfxVzwFileOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
				.as(ExtractDICNX0000002EfxVzwFileOptions.class);
		Pipeline pipeline = ExtractDICNX0000002EfxVzwFile(options);
		log.info("Running the pipeline");
		pipeline.run();
	}

	static Pipeline ExtractDICNX0000002EfxVzwFile(ExtractDICNX0000002EfxVzwFileOptions options) {
		log.info("Creating the pipeline");
		Pipeline p = Pipeline.create(options);

		final TupleTag<String> invalidInputRecordsTag = new TupleTag<String>() {

			private static final long serialVersionUID = 1L;
		};

		Supplier<PGPCryptorInfo> cryptorInfoSupplier = HashiCorpVaultPGPCryptorInfoSupplier.create()
				.withPGPCryptorOptions(options).withHashiCorpVaultOptions(options);

		ValueProvider<String> currentTimestamp = options.getCurrentTimestamp();
		final TupleTag<String> invalidRowsTag = new TupleTag<String>() {
		};
		final TupleTag<Row> validRowsTag = new TupleTag<Row>() {
		};

		log.info("Applying transform(s): (1) Read from ComCust File (2) Convert to Schema Row and Validate");

		PCollectionTuple extractFileRowsTuple = p
				.apply("Read from ST_DS_CNX_IN_${DATE}_${TIME} File",
						PGPDecryptFileIO.read().from(options.getInput()).withCryptorInfoSupplier(cryptorInfoSupplier))
				.apply("Convert to Schema Row and Validate",
						ParDo.of(new ExtractDICNX0000002EfxVzwFileTransform(validRowsTag, invalidRowsTag))
								.withOutputTags(validRowsTag, TupleTagList.of(invalidRowsTag)));
		// Retrieve Input - Valid Rows
		PCollection<Row> extractFileValidRows = extractFileRowsTuple.get(validRowsTag.getId());
		extractFileValidRows.setRowSchema(ExtractDICNX0000002EfxVzwFileSchema.extractDICNX0000002EfxVzwFile());

		// Extract DICNX0000002-efx_vzw_${DATE}_${TIME}.snd Onput file
		log.info("Applying transform(s): Extract DICNX0000002-efx_vzw_${DATE}_${TIME}.snd Onput file");
		extractFileValidRows.apply(
				new PGPEncryptFileIO<Row>(cryptorInfoSupplier, options.getOutputFileName(), options.getOutputPath(),
						WriteFileConfigHelper.getConfigForExtractDICNX0000002EfxVzwFile(), "|", null, null, false));

		return p;

	}

}
