package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileInputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.FileOutputOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface ExtractDICNX0000002EfxVzwFileOptions extends GcpOptions, PGPCryptorOptions, HashiCorpVaultOptions,
FileOutputOptions, FileInputOptions {

	@Description("Current Timestamp for the Job with format => MM-dd-yyyy'T'HH:mm:ss")
	@Validation.Required
	ValueProvider<String> getCurrentTimestamp();

	void setCurrentTimestamp(ValueProvider<String> value);

}
