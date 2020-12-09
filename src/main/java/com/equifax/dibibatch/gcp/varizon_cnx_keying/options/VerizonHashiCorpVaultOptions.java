package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;

public interface VerizonHashiCorpVaultOptions extends PipelineOptions{ 

@Description("Verizon HashiCorpVault - Keystore GCS Bucket Name")
@Validation.Required
ValueProvider<String> getHcvVZKeystoreGcsBucketName();

void setHcvVZKeystoreGcsBucketName(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - Keystore GCS File Path")
@Default.String("security/truststore/cacerts")
ValueProvider<String> getHcvVZKeystoreGcsFilePath();

void setHcvVZKeystoreGcsFilePath(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - URL")
@Validation.Required
ValueProvider<String> getHcvVZUrl();

void setHcvVZUrl(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - Namespace")
@Validation.Required
ValueProvider<String> getHcvVZNamespace();

void setHcvVZNamespace(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - Secret Path")
@Validation.Required
ValueProvider<String> getHcvVZSecretPath();

void setHcvVZSecretPath(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - EngineVersion")
@Default.Integer(2)
ValueProvider<Integer> getHcvVZEngineVersion();

void setHcvVZEngineVersion(ValueProvider<Integer> input);

@Description("Verizon HashiCorpVault - Connection Timeout (seconds)")
@Default.Integer(5)
ValueProvider<Integer> getHcvVZConnectionTimeout();

void setHcvVZConnectionTimeout(ValueProvider<Integer> input);

@Description("Verizon HashiCorpVault - Read Timeout (seconds)")
@Default.Integer(10)
ValueProvider<Integer> getHcvVZReadTimeout();

void setHcvVZReadTimeout(ValueProvider<Integer> input);

@Description("Verizon HashiCorpVault - Read Retry Count")
@Default.Integer(3)
ValueProvider<Integer> getHcvVZReadRetryCount();

void setHcvVZReadRetryCount(ValueProvider<Integer> input);

@Description("Verizon HashiCorpVault - Read Retry Interval (milliseconds)")
@Default.Integer(1000)
ValueProvider<Integer> getHcvVZReadRetryInterval();

void setHcvVZReadRetryInterval(ValueProvider<Integer> input);

@Description("Verizon HashiCorpVault - GCP IAM Role")
@Validation.Required
ValueProvider<String> getHcvVZGcpRole();

void setHcvVZGcpRole(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - GCP Project Id")
@Validation.Required
ValueProvider<String> getHcvVZGcpProjectId();

void setHcvVZGcpProjectId(ValueProvider<String> value);

@Description("Verizon HashiCorpVault - GCP Service Account")
@Validation.Required
ValueProvider<String> getHcvVZGcpServiceAccount();

void setHcvVZGcpServiceAccount(ValueProvider<String> value);
}

