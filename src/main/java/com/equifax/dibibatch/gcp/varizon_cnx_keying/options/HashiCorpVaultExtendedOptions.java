package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;

public interface HashiCorpVaultExtendedOptions extends PGPCryptorOptions, HashiCorpVaultOptions {

	// Second Key

	@Description("HashiCorpVault - Keystore GCS Bucket Name")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeystoreGcsBucketName();

	void setHcvSecondKeystoreGcsBucketName(ValueProvider<String> value);

	@Description("HashiCorpVault - Keystore GCS File Path")
	@Default.String("security/truststore/cacerts")
	ValueProvider<String> getHcvSecondKeystoreGcsFilePath();

	void setHcvSecondKeystoreGcsFilePath(ValueProvider<String> value);

	@Description("HashiCorpVault - URL")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeyUrl();

	void setHcvSecondKeyUrl(ValueProvider<String> value);

	@Description("HashiCorpVault - Namespace")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeyNamespace();

	void setHcvSecondKeyNamespace(ValueProvider<String> value);

	@Description("HashiCorpVault - Secret Path")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeySecretPath();

	void setHcvSecondKeySecretPath(ValueProvider<String> value);

	@Description("HashiCorpVault - EngineVersion")
	@Default.Integer(2)
	ValueProvider<Integer> getHcvEngineSecondKeyVersion();

	void setHcvEngineSecondKeyVersion(ValueProvider<Integer> input);

	@Description("HashiCorpVault - Connection Timeout (seconds)")
	@Default.Integer(5)
	ValueProvider<Integer> getHcvSecondKeyConnectionTimeout();

	void setHcvSecondKeyConnectionTimeout(ValueProvider<Integer> input);

	@Description("HashiCorpVault - Read Timeout (seconds)")
	@Default.Integer(10)
	ValueProvider<Integer> getHcvSecondKeyReadTimeout();

	void setHcvSecondKeyReadTimeout(ValueProvider<Integer> input);

	@Description("HashiCorpVault - Read Retry Count")
	@Default.Integer(3)
	ValueProvider<Integer> getHcvSecondKeyReadRetryCount();

	void setHcvSecondKeyReadRetryCount(ValueProvider<Integer> input);

	@Description("HashiCorpVault - Read Retry Interval (milliseconds)")
	@Default.Integer(1000)
	ValueProvider<Integer> getHcvSecondKeyReadRetryInterval();

	void setHcvSecondKeyReadRetryInterval(ValueProvider<Integer> input);

	@Description("HashiCorpVault - GCP IAM Role")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeyGcpRole();

	void setHcvSecondKeyGcpRole(ValueProvider<String> value);

	@Description("HashiCorpVault - GCP Project Id")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeyGcpProjectId();

	void setHcvSecondKeyGcpProjectId(ValueProvider<String> value);

	@Description("HashiCorpVault - GCP Service Account")
	@Validation.Required
	ValueProvider<String> getHcvSecondKeyGcpServiceAccount();

	void setHcvSecondKeyGcpServiceAccount(ValueProvider<String> value);

}
