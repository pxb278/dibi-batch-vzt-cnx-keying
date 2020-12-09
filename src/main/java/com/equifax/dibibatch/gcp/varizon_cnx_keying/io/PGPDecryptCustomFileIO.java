package com.equifax.dibibatch.gcp.varizon_cnx_keying.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.charset.Charset;
import java.security.Security;
import java.security.SignatureException;
import java.util.Iterator;
import java.util.Scanner;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileIO.MatchConfiguration;
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;
import org.bouncycastle.util.io.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.model.PGPCustomCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.model.PGPCryptorInfo;

@SuppressWarnings("deprecation")
public class PGPDecryptCustomFileIO {

	private static final Logger log = LoggerFactory.getLogger(PGPDecryptCustomFileIO.class);

	public static Read read() {
		return new Read();
	}

	static void checkArgument(boolean expression, @Nullable Object errorMessage) {
		if (!expression) {
			throw new IllegalArgumentException(String.valueOf(errorMessage));
		}
	}

	static void checkNotNull(Object check, @Nullable Object errorMessage) {
		if (null == check) {
			throw new IllegalArgumentException(String.valueOf(errorMessage));
		}
	}

	static class DefaultCryptorInfoSupplier implements Serializable, Supplier<PGPCustomCryptorInfo> {

		private static final long serialVersionUID = 1L;

		@Override
		public PGPCustomCryptorInfo get() {
			return PGPCustomCryptorInfo.create();
		}
	}

	public static class Read extends PTransform<PBegin, PCollection<String>> {

		private static final long serialVersionUID = 1L;

		ValueProvider<String> filePattern;

		MatchConfiguration matchConfiguration;
		Compression compression;

		Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier;

		Boolean parallelizeOutput = Boolean.TRUE;

		Read() {

			this.matchConfiguration = getMatchConfiguration();
			this.compression = Compression.AUTO;

			this.cryptorInfoSupplier = new DefaultCryptorInfoSupplier();

			this.parallelizeOutput = Boolean.TRUE;
		}

		private MatchConfiguration getMatchConfiguration() {
			return MatchConfiguration.create(EmptyMatchTreatment.DISALLOW);
		}

		public Read ignoreWhenNotFound() {
			return withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW);
		}

		public Read from(String filePattern) {
			checkArgument(filePattern != null, "filepattern can not be null");
			return from(StaticValueProvider.of(filePattern));
		}

		public Read from(ValueProvider<String> filePattern) {
			checkArgument(filePattern != null, "filepattern can not be null");
			this.filePattern = filePattern;
			return this;
		}

		public Read withMatchConfiguration(MatchConfiguration matchConfiguration) {
			this.matchConfiguration = matchConfiguration;
			return this;
		}

		public Read withCompression(Compression compression) {
			this.compression = compression;
			return this;
		}

		public Read withEmptyMatchTreatment(EmptyMatchTreatment treatment) {
			return withMatchConfiguration(getMatchConfiguration().withEmptyMatchTreatment(treatment));
		}

		public Read withCryptorInfoSupplier(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier) {
			checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
			this.cryptorInfoSupplier = cryptorInfoSupplier;
			return this;
		}

		public Read withNoParallelizedOutput() {
			this.parallelizeOutput = Boolean.FALSE;
			return this;
		}

		@Override
		public PCollection<String> expand(PBegin input) {

			checkNotNull(this.filePattern, "need to set the filepattern of a PGPDecryptFileIO.Read transform");

			PCollection<String> output = input
					.apply("Create filepattern", Create.ofProvider(this.filePattern, StringUtf8Coder.of()))
					.apply("Match All", FileIO.matchAll().withConfiguration(this.matchConfiguration))
					.apply("Read Matches",
							FileIO.readMatches().withCompression(this.compression)
									.withDirectoryTreatment(DirectoryTreatment.PROHIBIT))
					.apply("Decrypt and Read", ParDo.of(new DecryptAndScanFn(this.cryptorInfoSupplier)));

			// Check if need to parallelize the output
			if (this.parallelizeOutput.booleanValue()) {
				output = output.apply("Parallalize the output", new ParallelizeOutput<String>());
			}

			return output;
		}

	}

	static class DecryptAndScanFn extends DoFn<FileIO.ReadableFile, String> {

		private static final long serialVersionUID = 1L;

		private Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier;
		private PGPCustomCryptorInfo cryptorInfo;

		private InputStream encryptedStream;
		private Scanner scanner;

		public DecryptAndScanFn(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier) {
			this.cryptorInfoSupplier = cryptorInfoSupplier;
		}

		@StartBundle
		public void startBundle() throws Exception {
			this.cryptorInfo = this.cryptorInfoSupplier.get();
		}

		@ProcessElement
		public void processElement(ProcessContext context) throws Exception {

			try {

				ReadableFile readableFile = context.element();

				this.encryptedStream = Channels.newInputStream(readableFile.open());

				if (this.cryptorInfo.isCryptionEnabled()) {
					this.scanner = new Scanner(decrypt());
				} else {
					this.scanner = new Scanner(this.encryptedStream);
				}

				String line = this.readNextLine();

				while (null != line) {
					context.output(line);
					line = this.readNextLine();
				}

				close();

			} catch (Exception ex) {
				log.error(ex.getMessage(), ex);
				throw ex;
			}
		}

		private String decrypt() throws Exception {

			Decryptor decryptor = new Decryptor();

			InputStream privateKeyStream = new ByteArrayInputStream(
					this.cryptorInfo.getPrivateKey().getBytes(Charset.defaultCharset()));

			if (this.cryptorInfo.isSigned()) {

				InputStream publicKeyStream = new ByteArrayInputStream(
						this.cryptorInfo.getPublicKey().getBytes(Charset.defaultCharset()));

				return new String(decryptor.decryptDataWithSignature(this.encryptedStream, privateKeyStream,
						publicKeyStream, this.cryptorInfo.getPassPhrase().toCharArray()));

			} else {

				return new String(decryptor.decryptDataWithoutSignature(this.encryptedStream, privateKeyStream,
						this.cryptorInfo.getPassPhrase().toCharArray()));
			}

		}

		private void close() throws Exception {

			if (null != this.scanner) {
				this.scanner.close();
			}

			if (null != this.encryptedStream) {
				this.encryptedStream.close();
			}
		}

		private String readNextLine() throws Exception {

			if (this.scanner.hasNextLine()) {
				return this.scanner.nextLine();
			}

			return null;
		}

	}

	static class Decryptor {

		private static final String BC = "BC";

		public Decryptor() {
			Security.addProvider(new BouncyCastleProvider());
		}

		public byte[] decryptDataWithoutSignature(InputStream inputDataStream, InputStream secretKeyForDecryption,
				char[] passPhraseForSecretKey) throws Exception {

			OutputStream decryptedOutputStream = new ByteArrayOutputStream();

			unsignedDecryptedMessage(inputDataStream, decryptedOutputStream, secretKeyForDecryption,
					passPhraseForSecretKey);

			return ((ByteArrayOutputStream) decryptedOutputStream).toByteArray();
		}

		public byte[] decryptDataWithSignature(InputStream inputDataStream, InputStream secretKeyForDecryption,
				InputStream publicKeyForVerifySignature, char[] passPhraseForSecretKey) throws Exception {

			OutputStream decryptedOutputStream = new ByteArrayOutputStream();

			signedDecryptedMessage(inputDataStream, decryptedOutputStream, secretKeyForDecryption,
					publicKeyForVerifySignature, passPhraseForSecretKey);

			return ((ByteArrayOutputStream) decryptedOutputStream).toByteArray();
		}

		private void unsignedDecryptedMessage(InputStream encryptedInputStream, OutputStream decryptedOutputStream,
				InputStream privateKeyForDecryption, char[] passPhrase) throws Exception {

			try {

				PGPSecretKey secretKey = readSecretKey(privateKeyForDecryption);

				PGPPrivateKey pgpPrivKey = secretKey
						.extractPrivateKey(new JcePBESecretKeyDecryptorBuilder().setProvider(BC).build(passPhrase));

				decompressWithoutSignature(encryptedInputStream, decryptedOutputStream, pgpPrivKey);

			} catch (Exception ex) {
				log.error("Exception occured during decrypting the message: " + ex.getMessage(), ex);
				throw ex;
			}
		}

		private void signedDecryptedMessage(InputStream encryptedInputStream, OutputStream decryptedOutputStream,
				InputStream privateKeyForDecryption, InputStream publicKeyToVerifySignature, char[] passPhrase)
				throws Exception {

			try {

				PGPPublicKey publicKeyForVerifySign = readPublicKey(publicKeyToVerifySignature);
				PGPSecretKey secretKey = readSecretKey(privateKeyForDecryption);

				PGPPrivateKey pgpPrivKey = secretKey
						.extractPrivateKey(new JcePBESecretKeyDecryptorBuilder().setProvider(BC).build(passPhrase));

				decompressWithSignature(encryptedInputStream, decryptedOutputStream, publicKeyForVerifySign,
						pgpPrivKey);

			} catch (Exception ex) {
				log.error("Exception occured during decrypting the message: " + ex.getMessage(), ex);
				throw ex;
			}
		}

		private void decompressWithoutSignature(InputStream encryptedInputStream, OutputStream decryptedOutputStream,
				PGPPrivateKey pgpPrivKey) throws IOException, PGPException {

			encryptedInputStream = new ArmoredInputStream(encryptedInputStream);
			JcaPGPObjectFactory pgpF = new JcaPGPObjectFactory(encryptedInputStream);

			PGPEncryptedDataList enc = (PGPEncryptedDataList) pgpF.nextObject();

			JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(
					((PGPPublicKeyEncryptedData) enc.getEncryptedDataObjects().next()).getDataStream(
							new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(BC).build(pgpPrivKey)));

			Object message = plainFact.nextObject();

			ByteArrayOutputStream actualOutput = new ByteArrayOutputStream();

			while (null != message) {

				// Decompress if it is compressed
				if (message instanceof PGPCompressedData) {
					PGPCompressedData compressedData = (PGPCompressedData) message;
					plainFact = new JcaPGPObjectFactory(compressedData.getDataStream());
					message = plainFact.nextObject();
				}

				// Build the stream
				if (message instanceof PGPLiteralData) {
					Streams.pipeAll(((PGPLiteralData) message).getInputStream(), actualOutput);
				} else {
					throw new PGPException("message is not a simple encrypted file - type unknown.");
				}

				message = plainFact.nextObject();
			}

			actualOutput.close();
			byte[] outputByteArr = actualOutput.toByteArray();

			if (null != decryptedOutputStream) {

				decryptedOutputStream.write(outputByteArr);
				decryptedOutputStream.flush();
			}

			encryptedInputStream.close();
		}

		private void decompressWithSignature(InputStream encryptedInputStream, OutputStream decryptedOutputStream,
				PGPPublicKey publicKeyForVerifySign, PGPPrivateKey pgpPrivKey)
				throws IOException, PGPException, SignatureException {

			encryptedInputStream = new ArmoredInputStream(encryptedInputStream);
			JcaPGPObjectFactory pgpF = new JcaPGPObjectFactory(encryptedInputStream);

			PGPEncryptedDataList enc = (PGPEncryptedDataList) pgpF.nextObject();

			JcaPGPObjectFactory plainFact = new JcaPGPObjectFactory(
					((PGPPublicKeyEncryptedData) enc.getEncryptedDataObjects().next()).getDataStream(
							new JcePublicKeyDataDecryptorFactoryBuilder().setProvider(BC).build(pgpPrivKey)));

			Object message = plainFact.nextObject();

			PGPOnePassSignatureList onePassSignatureList = null;
			PGPSignatureList signatureList = null;

			ByteArrayOutputStream actualOutput = new ByteArrayOutputStream();

			while (null != message) {

				// Decompress if it is compressed
				if (message instanceof PGPCompressedData) {
					PGPCompressedData compressedData = (PGPCompressedData) message;
					plainFact = new JcaPGPObjectFactory(compressedData.getDataStream());
					message = plainFact.nextObject();
				}

				// Build the stream
				if (message instanceof PGPLiteralData) {
					Streams.pipeAll(((PGPLiteralData) message).getInputStream(), actualOutput);
				} else if (message instanceof PGPOnePassSignatureList) {
					onePassSignatureList = (PGPOnePassSignatureList) message;
				} else if (message instanceof PGPSignatureList) {
					signatureList = (PGPSignatureList) message;
				} else {
					throw new PGPException("message is not a simple encrypted file - type unknown.");
				}

				message = plainFact.nextObject();
			}

			actualOutput.close();
			byte[] outputByteArr = actualOutput.toByteArray();

			verifySignedMessage(publicKeyForVerifySign, onePassSignatureList, signatureList, outputByteArr);

			if (null != decryptedOutputStream) {

				decryptedOutputStream.write(outputByteArr);
				decryptedOutputStream.flush();
			}

			encryptedInputStream.close();
		}

		private void verifySignedMessage(PGPPublicKey publicKeyForVerifySign,
				PGPOnePassSignatureList onePassSignatureList, PGPSignatureList signatureList, byte[] outputByteArr)
				throws PGPException, SignatureException {

			if (null == onePassSignatureList || null == signatureList) {
				throw new PGPException("Poor PGP. Signatures not found.");
			} else {

				for (int i = 0; i < onePassSignatureList.size(); i++) {
					PGPOnePassSignature ops = onePassSignatureList.get(0);
					log.info("PGPDecryptFileIO - Verifier : " + ops.getKeyID());
					if (null != publicKeyForVerifySign) {

						ops.init(new JcaPGPContentVerifierBuilderProvider().setProvider("BC"), publicKeyForVerifySign);
						ops.update(outputByteArr);

						PGPSignature signature = signatureList.get(i);

						if (ops.verify(signature)) {

							Iterator<String> userIds = publicKeyForVerifySign.getUserIDs();

							while (userIds.hasNext()) {
								String userId = (String) userIds.next();
								log.info("PGPDecryptFileIO - Signed by " + userId);
							}

							log.info("PGPDecryptFileIO - Signature verified");

						} else {
							throw new SignatureException("Signature verification failed");
						}
					}
				}
			}
		}

		private PGPPublicKey readPublicKey(InputStream inputStream) throws Exception {

			try (InputStream decoderStream = PGPUtil.getDecoderStream(inputStream)) {

				PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(decoderStream,
						new JcaKeyFingerprintCalculator());

				Iterator<PGPPublicKeyRing> keyRingIter = pgpPub.getKeyRings();
				while (keyRingIter.hasNext()) {
					PGPPublicKeyRing keyRing = (PGPPublicKeyRing) keyRingIter.next();
					Iterator<PGPPublicKey> keyIter = keyRing.getPublicKeys();
					while (keyIter.hasNext()) {
						PGPPublicKey key = (PGPPublicKey) keyIter.next();
						if (key.isEncryptionKey()) {
							return key;
						}
					}
				}

				throw new IllegalArgumentException("Can't find encryption key in key ring.");

			} catch (Exception ex) {
				throw ex;
			}
		}

		private PGPSecretKey readSecretKey(InputStream input) throws Exception {

			PGPSecretKeyRingCollection pgpSec = null;

			try {
				pgpSec = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(input),
						new BcKeyFingerprintCalculator());
			} catch (Exception ex) {
				throw ex;
			}

			Iterator<PGPSecretKeyRing> keyRingIter = pgpSec.getKeyRings();

			while (keyRingIter.hasNext()) {
				PGPSecretKeyRing keyRing = (PGPSecretKeyRing) keyRingIter.next();

				Iterator<PGPSecretKey> keyIter = keyRing.getSecretKeys();
				while (keyIter.hasNext()) {
					PGPSecretKey key = (PGPSecretKey) keyIter.next();

					if (key.isSigningKey()) {
						return key;
					}
				}
			}

			throw new IllegalArgumentException("Can't find signing key in key ring.");
		}
	}

	static class ParallelizeOutput<T> extends PTransform<PCollection<T>, PCollection<T>> {

		private static final long serialVersionUID = 1L;

		@Override
		public PCollection<T> expand(PCollection<T> input) {

			PCollectionView<Iterable<T>> empty = input
					.apply("Consume", Filter.by(SerializableFunctions.constant(false))).apply(View.asIterable());

			PCollection<T> materialized = input.apply("Identity", ParDo.of(new DoFn<T, T>() {

				private static final long serialVersionUID = 1L;

				@ProcessElement
				public void process(ProcessContext c) {
					c.output(c.element());
				}

			}).withSideInputs(empty));

			return materialized.apply(Reshuffle.viaRandomKey());
		}
	}

}
