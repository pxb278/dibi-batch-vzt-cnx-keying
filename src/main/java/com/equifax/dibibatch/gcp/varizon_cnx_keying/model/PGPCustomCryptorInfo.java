package com.equifax.dibibatch.gcp.varizon_cnx_keying.model;

import java.io.Serializable;

public class PGPCustomCryptorInfo implements Serializable {

	private static final long serialVersionUID = 1L;

	private boolean cryptionEnabled;
	private boolean signed;

	private String passPhrase;
	private String publicKey;
	private String privateKey;

	private boolean seconKeycryptionEnabled;
	private boolean seconKeysigned;

	private String seconKeypassPhrase;
	private String seconKeypublicKey;
	private String seconKeyprivateKey;

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

	private PGPCustomCryptorInfo() {
		this.cryptionEnabled = true;
		this.signed = false;
		this.seconKeycryptionEnabled = true;
		this.seconKeysigned = false;
	}

	public static PGPCustomCryptorInfo create() {
		return new PGPCustomCryptorInfo();
	}

	public PGPCustomCryptorInfo withDisabledCryption() {
		this.cryptionEnabled = false;
		return this;
	}

	public PGPCustomCryptorInfo withSigned() {
		this.signed = true;
		return this;
	}

	public PGPCustomCryptorInfo withPassPhrase(String input) {
		this.passPhrase = input;
		return this;
	}

	public PGPCustomCryptorInfo withPublicKey(String input) {
		this.publicKey = input;
		return this;
	}

	public PGPCustomCryptorInfo withPrivateKey(String input) {
		this.privateKey = input;
		return this;
	}

	public boolean isCryptionEnabled() {
		return cryptionEnabled;
	}

	public boolean isSigned() {
		return signed;
	}

	public String getPassPhrase() {
		return passPhrase;
	}

	public String getPublicKey() {
		return publicKey;
	}

	public String getPrivateKey() {
		return privateKey;
	}

//For Second Key
	public PGPCustomCryptorInfo withSecondKeyDisabledCryption() {
		this.seconKeycryptionEnabled = false;
		return this;
	}

	public PGPCustomCryptorInfo withSecondKeySigned() {
		this.seconKeysigned = true;
		return this;
	}

	public PGPCustomCryptorInfo withSecondKeyPassPhrase(String input) {
		this.seconKeypassPhrase = input;
		return this;
	}

	public PGPCustomCryptorInfo withSecondKeyPublicKey(String input) {
		this.seconKeypublicKey = input;
		return this;
	}

	public PGPCustomCryptorInfo withSecondKeyPrivateKey(String input) {
		this.seconKeyprivateKey = input;
		return this;
	}

	public boolean isSecondKeyCryptionEnabled() {
		return seconKeycryptionEnabled;
	}

	public boolean isSecondKeySigned() {
		return seconKeysigned;
	}

	public String getSecondKeyPassPhrase() {
		return seconKeypassPhrase;
	}

	public String getSecondKeyPublicKey() {
		return seconKeypublicKey;
	}

	public String getSecondKeyPrivateKey() {
		return seconKeyprivateKey;
	}

}
