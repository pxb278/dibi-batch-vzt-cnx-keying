package com.equifax.dibibatch.gcp.varizon_cnx_keying.helper;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.model.PGPCustomCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.HashiCorpVaultInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.HashiCorpVaultOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.options.PGPCryptorOptions;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.HashiCorpVaultConnector;

public class HashiCorpVaultPGPCryptorInfoCustomSupplier implements Serializable, Supplier<PGPCustomCryptorInfo> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(HashiCorpVaultPGPCryptorInfoCustomSupplier.class);

	

	// PGP For VZT
	private ValueProvider<Boolean> pgpCryptionSecondKeyEnabled;
	private ValueProvider<Boolean> pgpSecondKeySigned;
	private ValueProvider<String> pgpVaultSecondKeyPassphraseFieldName;
	private ValueProvider<String> pgpVaultSecondKeyPrivatekeyFieldName;
	private ValueProvider<String> pgpVaultSecondKeyPublickeyFieldName;

	// HashiCorp Vault For VZT
	private ValueProvider<String> hcvSecondKeystoreGcsBucketName;
	private ValueProvider<String> hcvSecondKeystoreGcsFilePath;
	private ValueProvider<String> hcvSecondKeyUrl;
	private ValueProvider<String> hcvSecondKeyNamespace;
	private ValueProvider<String> hcvSecondKeySecretPath;
	private ValueProvider<Integer> hcvEngineSecondKeyVersion;
	private ValueProvider<Integer> hcvSecondKeyConnectionTimeout;
	private ValueProvider<Integer> hcvSecondKeyReadTimeout;
	private ValueProvider<Integer> hcvSecondKeyReadRetryCount;
	private ValueProvider<Integer> hcvSecondKeyReadRetryInterval;
	private ValueProvider<String> hcvSecondKeyGcpRole;
	private ValueProvider<String> hcvSecondKeyGcpProjectId;
	private ValueProvider<String> hcvSecondKeyGcpServiceAccount;

	private HashiCorpVaultPGPCryptorInfoCustomSupplier() {
	}

	public static HashiCorpVaultPGPCryptorInfoCustomSupplier create() {
		return new HashiCorpVaultPGPCryptorInfoCustomSupplier();
	}

	

	

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPGPCryptorExtendedOptions(PGPCryptorOptions input) {

		this.pgpCryptionSecondKeyEnabled = input.getPgpCryptionEnabled();
		this.pgpSecondKeySigned = input.getPgpSigned();
		this.pgpVaultSecondKeyPassphraseFieldName = input.getPgpVaultPassphraseFieldName();
		this.pgpVaultSecondKeyPrivatekeyFieldName = input.getPgpVaultPrivatekeyFieldName();
		this.pgpVaultSecondKeyPublickeyFieldName = input.getPgpVaultPublickeyFieldName();

		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHashiCorpVaultExtendedOptions(HashiCorpVaultOptions input) {

		this.hcvSecondKeystoreGcsBucketName = input.getHcvKeystoreGcsBucketName();
		this.hcvSecondKeystoreGcsFilePath = input.getHcvKeystoreGcsFilePath();
		this.hcvSecondKeyUrl = input.getHcvUrl();
		this.hcvSecondKeyNamespace = input.getHcvNamespace();
		this.hcvSecondKeySecretPath = input.getHcvSecretPath();
		this.hcvEngineSecondKeyVersion = input.getHcvEngineVersion();
		this.hcvSecondKeyConnectionTimeout = input.getHcvConnectionTimeout();
		this.hcvSecondKeyReadTimeout = input.getHcvReadTimeout();
		this.hcvSecondKeyReadRetryCount = input.getHcvReadRetryCount();
		this.hcvSecondKeyReadRetryInterval = input.getHcvReadRetryInterval();
		this.hcvSecondKeyGcpRole = input.getHcvGcpRole();
		this.hcvSecondKeyGcpProjectId = input.getHcvGcpProjectId();
		this.hcvSecondKeyGcpServiceAccount = input.getHcvGcpServiceAccount();

		return this;
	}

	// PGP For Second Key
	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPgpSecondKeyCryptionEnabled(ValueProvider<Boolean> input) {
		this.pgpCryptionSecondKeyEnabled = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPgpSecondKeySigned(ValueProvider<Boolean> input) {
		this.pgpSecondKeySigned = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPgpSecondKeyVaultPassphraseFieldName(
			ValueProvider<String> input) {
		this.pgpVaultSecondKeyPassphraseFieldName = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPgpSecondKeyVaultPrivatekeyFieldName(
			ValueProvider<String> input) {
		this.pgpVaultSecondKeyPrivatekeyFieldName = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withPgpSecondKeyVaultPublickeyFieldName(
			ValueProvider<String> input) {
		this.pgpVaultSecondKeyPublickeyFieldName = input;
		return this;
	}

	// HashiCorp Vault For Second Key
	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeystoreGcsBucketName(ValueProvider<String> input) {
		this.hcvSecondKeystoreGcsBucketName = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeystoreGcsFilePath(ValueProvider<String> input) {
		this.hcvSecondKeystoreGcsFilePath = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyUrl(ValueProvider<String> input) {
		this.hcvSecondKeyUrl = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyNamespace(ValueProvider<String> input) {
		this.hcvSecondKeyNamespace = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeySecretPath(ValueProvider<String> input) {
		this.hcvSecondKeySecretPath = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyEngineVersion(ValueProvider<Integer> input) {
		this.hcvEngineSecondKeyVersion = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyConnectionTimeout(ValueProvider<Integer> input) {
		this.hcvSecondKeyConnectionTimeout = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyReadTimeout(ValueProvider<Integer> input) {
		this.hcvSecondKeyReadTimeout = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyReadRetryCount(ValueProvider<Integer> input) {
		this.hcvSecondKeyReadRetryCount = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyReadRetryInterval(ValueProvider<Integer> input) {
		this.hcvSecondKeyReadRetryInterval = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyGcpRole(ValueProvider<String> input) {
		this.hcvSecondKeyGcpRole = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyGcpProjectId(ValueProvider<String> input) {
		this.hcvSecondKeyGcpProjectId = input;
		return this;
	}

	public HashiCorpVaultPGPCryptorInfoCustomSupplier withHcvSecondKeyGcpServiceAccount(ValueProvider<String> input) {
		this.hcvSecondKeyGcpServiceAccount = input;
		return this;
	}

	@Override
	public PGPCustomCryptorInfo get() {

		PGPCustomCryptorInfo pgpCustomCryptorInfo = PGPCustomCryptorInfo.create();

		if (!this.pgpCryptionSecondKeyEnabled.get()) {
			pgpCustomCryptorInfo = pgpCustomCryptorInfo.withDisabledCryption();
		}

		if (this.pgpSecondKeySigned.get()) {
			pgpCustomCryptorInfo = pgpCustomCryptorInfo.withSigned();
		}

		if (this.pgpCryptionSecondKeyEnabled.get()) {

			try {

				HashiCorpVaultInfo hashiCorpVaultInfo = HashiCorpVaultInfo.create()
						.withKeystoreGcsBucketName(this.hcvSecondKeystoreGcsBucketName)
						.withKeystoreGcsFilePath(this.hcvSecondKeystoreGcsFilePath).withUrl(this.hcvSecondKeyUrl)
						.withNamespace(this.hcvSecondKeyNamespace).withSecretPath(this.hcvSecondKeySecretPath)
						.withEngineVersion(this.hcvEngineSecondKeyVersion)
						.withConnectionTimeout(this.hcvSecondKeyConnectionTimeout)
						.withReadTimeout(this.hcvSecondKeyReadTimeout)
						.withReadRetryCount(this.hcvSecondKeyReadRetryCount)
						.withReadRetryInterval(this.hcvSecondKeyReadRetryInterval).withGcpRole(this.hcvSecondKeyGcpRole)
						.withGcpProjectId(this.hcvSecondKeyGcpProjectId)
						.withGcpServiceAccount(this.hcvSecondKeyGcpServiceAccount);

				HashiCorpVaultConnector hashiCorpVaultConnector = HashiCorpVaultConnector.create()
						.withHashiCorpVaultInfo(hashiCorpVaultInfo).andBuild();

				Map<String, String> vaultProtectedData = Optional.of(hashiCorpVaultConnector.retrieveProtectedData())
						.orElse(new HashMap<String, String>());

				pgpCustomCryptorInfo = pgpCustomCryptorInfo
						.withPassPhrase(vaultProtectedData.get(this.pgpVaultSecondKeyPassphraseFieldName.get()))
						.withPrivateKey(vaultProtectedData.get(this.pgpVaultSecondKeyPrivatekeyFieldName.get()))
						.withPublicKey(vaultProtectedData.get(this.pgpVaultSecondKeyPublickeyFieldName.get()));

			} catch (Exception ex) {
				log.error("HashiCorpVaultPGPCryptorInfoSupplier - Exception occurred : " + ex.getMessage(), ex);
			}
		}

		return pgpCustomCryptorInfo;
	}

}
