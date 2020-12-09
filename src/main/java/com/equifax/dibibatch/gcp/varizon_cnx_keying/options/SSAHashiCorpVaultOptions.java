package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface SSAHashiCorpVaultOptions extends PipelineOptions {

	@Description("SSA HashiCorpVault - Keystore GCS Bucket Name")
	@Validation.Required
	ValueProvider<String> getHcvSSAKeystoreGcsBucketName();

	void setHcvSSAKeystoreGcsBucketName(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - Keystore GCS File Path")
	@Default.String("security/truststore/cacerts")
	ValueProvider<String> getHcvSSAKeystoreGcsFilePath();

	void setHcvSSAKeystoreGcsFilePath(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - URL")
	@Validation.Required
	ValueProvider<String> getHcvSSAUrl();

	void setHcvSSAUrl(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - Namespace")
	@Validation.Required
	ValueProvider<String> getHcvSSANamespace();

	void setHcvSSANamespace(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - Secret Path")
	@Validation.Required
	ValueProvider<String> getHcvSSASecretPath();

	void setHcvSSASecretPath(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - EngineVersion")
	@Default.Integer(2)
	ValueProvider<Integer> getHcvSSAEngineVersion();

	void setHcvSSAEngineVersion(ValueProvider<Integer> input);

	@Description("SSA HashiCorpVault - Connection Timeout (seconds)")
	@Default.Integer(5)
	ValueProvider<Integer> getHcvSSAConnectionTimeout();

	void setHcvSSAConnectionTimeout(ValueProvider<Integer> input);

	@Description("SSA HashiCorpVault - Read Timeout (seconds)")
	@Default.Integer(10)
	ValueProvider<Integer> getHcvSSAReadTimeout();

	void setHcvSSAReadTimeout(ValueProvider<Integer> input);

	@Description("SSA HashiCorpVault - Read Retry Count")
	@Default.Integer(3)
	ValueProvider<Integer> getHcvSSAReadRetryCount();

	void setHcvSSAReadRetryCount(ValueProvider<Integer> input);

	@Description("SSA HashiCorpVault - Read Retry Interval (milliseconds)")
	@Default.Integer(1000)
	ValueProvider<Integer> getHcvSSAReadRetryInterval();

	void setHcvSSAReadRetryInterval(ValueProvider<Integer> input);

	@Description("SSA HashiCorpVault - GCP IAM Role")
	@Validation.Required
	ValueProvider<String> getHcvSSAGcpRole();

	void setHcvSSAGcpRole(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - GCP Project Id")
	@Validation.Required
	ValueProvider<String> getHcvSSAGcpProjectId();

	void setHcvSSAGcpProjectId(ValueProvider<String> value);

	@Description("SSA HashiCorpVault - GCP Service Account")
	@Validation.Required
	ValueProvider<String> getHcvSSAGcpServiceAccount();

	void setHcvSSAGcpServiceAccount(ValueProvider<String> value);
}
