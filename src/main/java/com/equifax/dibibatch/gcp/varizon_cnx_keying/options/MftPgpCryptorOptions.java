package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;

public interface MftPgpCryptorOptions extends PipelineOptions{
	
	@Description("PGP Cryption Enabled")
	@Default.Boolean(true)
	ValueProvider<Boolean> getMftPgpCryptionEnabled();

	void setMftPgpCryptionEnabled(ValueProvider<Boolean> value);

	@Description("PGP Signed")
	@Default.Boolean(false)
	ValueProvider<Boolean> getMftPgpSigned();

	void setMftPgpSigned(ValueProvider<Boolean> value);

	@Description("PGP Armored")
	@Default.Boolean(true)
	ValueProvider<Boolean> getMftPgpArmored();

	void setMftPgpArmored(ValueProvider<Boolean> value);

	@Description("PGP Object Factory Implementation to use, possible values are: BC, JCA (default)")
	@Default.String(PGPCryptorInfo.OBJECT_FACTORY_DEFAULT)
	ValueProvider<String> getMftPgpObjectFactory();

	void setMftPgpObjectFactory(ValueProvider<String> value);

	@Description("PGP Vault Passphrase Field Name")
	@Default.String("pass-phrase")
	ValueProvider<String> getMftPgpVaultPassphraseFieldName();

	void setMftPgpVaultPassphraseFieldName(ValueProvider<String> value);

	@Description("PGP Vault Privatekey Field Name")
	@Default.String("private-key")
	ValueProvider<String> getMftPgpVaultPrivatekeyFieldName();

	void setMftPgpVaultPrivatekeyFieldName(ValueProvider<String> value);

	@Description("PGP Vault Publickey Field Name")
	@Default.String("public-key")
	ValueProvider<String> getMftPgpVaultPublickeyFieldName();

	void setMftPgpVaultPublickeyFieldName(ValueProvider<String> value);

}

