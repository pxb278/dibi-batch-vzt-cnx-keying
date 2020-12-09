package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface ICHashiCorpVaultOptions extends PipelineOptions {

	@Description("IC HashiCorpVault - Keystore GCS Bucket Name")
	@Validation.Required
	ValueProvider<String> getHcvMFTKeystoreGcsBucketName();

	void setHcvMFTKeystoreGcsBucketName(ValueProvider<String> value);

	@Description("IC HashiCorpVault - Keystore GCS File Path")
	@Default.String("security/truststore/cacerts")
	ValueProvider<String> getHcvMFTKeystoreGcsFilePath();

	void setHcvMFTKeystoreGcsFilePath(ValueProvider<String> value);

	@Description("IC HashiCorpVault - URL")
	@Validation.Required
	ValueProvider<String> getHcvMFTUrl();

	void setHcvMFTUrl(ValueProvider<String> value);

	@Description("IC HashiCorpVault - Namespace")
	@Validation.Required
	ValueProvider<String> getHcvMFTNamespace();

	void setHcvMFTNamespace(ValueProvider<String> value);

	@Description("IC HashiCorpVault - Secret Path")
	@Validation.Required
	ValueProvider<String> getHcvMFTSecretPath();

	void setHcvMFTSecretPath(ValueProvider<String> value);

	@Description("IC HashiCorpVault - EngineVersion")
	@Default.Integer(2)
	ValueProvider<Integer> getHcvMFTEngineVersion();

	void setHcvMFTEngineVersion(ValueProvider<Integer> input);

	@Description("IC HashiCorpVault - Connection Timeout (seconds)")
	@Default.Integer(5)
	ValueProvider<Integer> getHcvMFTConnectionTimeout();

	void setHcvMFTConnectionTimeout(ValueProvider<Integer> input);

	@Description("IC HashiCorpVault - Read Timeout (seconds)")
	@Default.Integer(10)
	ValueProvider<Integer> getHcvMFTReadTimeout();

	void setHcvMFTReadTimeout(ValueProvider<Integer> input);

	@Description("IC HashiCorpVault - Read Retry Count")
	@Default.Integer(3)
	ValueProvider<Integer> getHcvMFTReadRetryCount();

	void setHcvMFTReadRetryCount(ValueProvider<Integer> input);

	@Description("IC HashiCorpVault - Read Retry Interval (milliseconds)")
	@Default.Integer(1000)
	ValueProvider<Integer> getHcvMFTReadRetryInterval();

	void setHcvMFTReadRetryInterval(ValueProvider<Integer> input);

	@Description("IC HashiCorpVault - GCP IAM Role")
	@Validation.Required
	ValueProvider<String> getHcvMFTGcpRole();

	void setHcvMFTGcpRole(ValueProvider<String> value);

	@Description("IC HashiCorpVault - GCP Project Id")
	@Validation.Required
	ValueProvider<String> getHcvMFTGcpProjectId();

	void setHcvMFTGcpProjectId(ValueProvider<String> value);

	@Description("IC HashiCorpVault - GCP Service Account")
	@Validation.Required
	ValueProvider<String> getHcvMFTGcpServiceAccount();

	void setHcvMFTGcpServiceAccount(ValueProvider<String> value);
}
