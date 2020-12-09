package com.equifax.dibibatch.gcp.varizon_cnx_keying.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PGPCryptorExtendedOptions extends PipelineOptions {

//For SecondKey
	@Description("PGP Second Key Cryption Enabled")
	@Default.Boolean(true)
	ValueProvider<Boolean> getPgpCryptionSecondKeyEnabled();

	void setPgpCryptionSecondKeyEnabled(ValueProvider<Boolean> value);

	@Description("PGP Second Key Signed")
	@Default.Boolean(false)
	ValueProvider<Boolean> getPgpSecondKeySigned();

	void setPgpSecondKeySigned(ValueProvider<Boolean> value);

	@Description("PGP Vault Passphrase Second Key Field Name")
	@Default.String("pass-phrase")
	ValueProvider<String> getPgpSecondKeyVaultPassphraseFieldName();

	void setPgpSecondKeyVaultPassphraseFieldName(ValueProvider<String> value);

	@Description("PGP Second Key Vault Privatekey Field Name")
	@Default.String("private-key")
	ValueProvider<String> getPgpSecondKeyVaultPrivatekeyFieldName();

	void setPgpSecondKeyVaultPrivatekeyFieldName(ValueProvider<String> value);

	@Description("PGP Second Key Vault Publickey Field Name")
	@Default.String("public-key")
	ValueProvider<String> getPgpSecondKeyVaultPublickeyFieldName();

	void setPgpSecondKeyVaultPublickeyFieldName(ValueProvider<String> value);

}
