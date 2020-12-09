/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.equifax.dibibatch.gcp.varizon_cnx_keying.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.commons.lang3.StringUtils;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.HashAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.equifax.dibibatch.gcp.varizon_cnx_keying.model.PGPCustomCryptorInfo;
import com.equifax.usis.dibi.batch.gcp.dataflow.common.util.ObjectUtil;

public class PGPEncryptCustomFileIO<T> extends PTransform<PCollection<T>, PCollection<Void>> {

	private static final long serialVersionUID = 1L;

	private static final Logger log = LoggerFactory.getLogger(PGPEncryptCustomFileIO.class);

	public static final String DEFAULT_DELIMITER = "|";

	protected Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier;

	protected ValueProvider<String> filename;
	protected ValueProvider<String> path;
	protected Map<String, FieldFn<T>> fieldFn;
	protected String delimiter = DEFAULT_DELIMITER;
	protected String header = null;
	protected String footer = null;
	protected boolean temporary = false;

	PCollectionView<String> sideInputForPreText;
	PCollectionView<String> sideInputForPostText;

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

	public static <T> PGPEncryptCustomFileIO<T> write() {
		return new PGPEncryptCustomFileIO<T>();
	}

	public PGPEncryptCustomFileIO() {

		this.cryptorInfoSupplier = new DefaultCryptorInfoSupplier();

		this.delimiter = DEFAULT_DELIMITER;
		this.header = null;
		this.footer = null;
		this.temporary = false;
	}

	public PGPEncryptCustomFileIO(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier, ValueProvider<String> filename,
			ValueProvider<String> path, Map<String, FieldFn<T>> fieldFn) {

		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		checkNotNull(fieldFn, "fieldFn cannot be null");

		this.cryptorInfoSupplier = cryptorInfoSupplier;

		this.filename = filename;
		this.path = path;
		this.fieldFn = fieldFn;
	}

	public PGPEncryptCustomFileIO(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier, ValueProvider<String> filename,
			ValueProvider<String> path, Map<String, FieldFn<T>> fieldFn, String delimiter) {

		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		checkNotNull(fieldFn, "fieldFn cannot be null");
		checkNotNull(delimiter, "delimiter cannot be null");

		this.cryptorInfoSupplier = cryptorInfoSupplier;

		this.filename = filename;
		this.path = path;
		this.fieldFn = fieldFn;
		this.delimiter = delimiter;
	}

	public PGPEncryptCustomFileIO(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier, ValueProvider<String> filename,
			ValueProvider<String> path, Map<String, FieldFn<T>> fieldFn, String delimiter, String header,
			String footer) {

		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		checkNotNull(fieldFn, "fieldFn cannot be null");
		checkNotNull(delimiter, "delimiter cannot be null");

		this.cryptorInfoSupplier = cryptorInfoSupplier;

		this.filename = filename;
		this.path = path;
		this.fieldFn = fieldFn;
		this.delimiter = delimiter;
		this.header = header;
		this.footer = footer;
	}

	public PGPEncryptCustomFileIO(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier, ValueProvider<String> filename,
			ValueProvider<String> path, Map<String, FieldFn<T>> fieldFn, String delimiter, String header, String footer,
			boolean temporary) {

		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		checkNotNull(fieldFn, "fieldFn cannot be null");
		checkNotNull(delimiter, "delimiter cannot be null");

		this.cryptorInfoSupplier = cryptorInfoSupplier;

		this.filename = filename;
		this.path = path;
		this.fieldFn = fieldFn;
		this.delimiter = delimiter;
		this.header = header;
		this.footer = footer;
		this.temporary = temporary;
	}

	public PGPEncryptCustomFileIO(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier, ValueProvider<String> filename,
			ValueProvider<String> path, Map<String, FieldFn<T>> fieldFn, String delimiter, String header, String footer,
			boolean temporary, PCollectionView<String> sideInputForPreText,
			PCollectionView<String> sideInputForPostText) {

		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		checkNotNull(fieldFn, "fieldFn cannot be null");
		checkNotNull(delimiter, "delimiter cannot be null");
		checkNotNull(sideInputForPreText, "sideInputForPreText cannot be null");
		checkNotNull(sideInputForPostText, "sideInputForPostText cannot be null");

		this.cryptorInfoSupplier = cryptorInfoSupplier;

		this.filename = filename;
		this.path = path;
		this.fieldFn = fieldFn;
		this.delimiter = delimiter;
		this.header = header;
		this.footer = footer;
		this.temporary = temporary;

		this.sideInputForPreText = sideInputForPreText;
		this.sideInputForPostText = sideInputForPostText;
	}

	public PGPEncryptCustomFileIO<T> withCryptorInfoSupplier(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier) {
		checkNotNull(cryptorInfoSupplier, "cryptorInfoSupplier cannot be null");
		this.cryptorInfoSupplier = cryptorInfoSupplier;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withFilename(ValueProvider<String> filename) {
		this.filename = filename;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withPath(ValueProvider<String> path) {
		this.path = path;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withFieldFn(Map<String, FieldFn<T>> fieldFn) {
		checkNotNull(fieldFn, "fieldFn cannot be null");
		this.fieldFn = fieldFn;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withDelimiter(String delimiter) {
		checkNotNull(delimiter, "delimiter cannot be null");
		this.delimiter = delimiter;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withHeader(String header) {
		this.header = header;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withFooter(String footer) {
		this.footer = footer;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withTempDirectory() {
		this.temporary = true;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withSideInputForPreText(PCollectionView<String> sideInputForPreText) {
		checkNotNull(sideInputForPreText, "sideInputForPreText cannot be null");
		this.sideInputForPreText = sideInputForPreText;
		return this;
	}

	public PGPEncryptCustomFileIO<T> withSideInputForPostText(PCollectionView<String> sideInputForPostText) {
		checkNotNull(sideInputForPostText, "sideInputForPostText cannot be null");
		this.sideInputForPostText = sideInputForPostText;
		return this;
	}

	public interface FieldFn<T> extends Serializable {
		Object apply(DoFn<T, String>.ProcessContext context);
	}

	protected class BuildRowFn extends DoFn<T, String> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext c) {

			List<String> fields = new ArrayList<>();

			for (Map.Entry<String, FieldFn<T>> entry : fieldFn.entrySet()) {

				FieldFn<T> fcn = entry.getValue();

				String result = (String) fcn.apply(c);

				fields.add(result != null ? result : "");
			}

			String result = fields.stream().collect(Collectors.joining(delimiter));

			c.output(result);
		}
	}

	protected class CustomFileNaming implements FileIO.Write.FileNaming {

		private static final long serialVersionUID = 1L;

		@Override
		public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex,
				Compression compression) {

			return filename.get();
		}
	}

	@Override
	public PCollection<Void> expand(PCollection<T> records) {

		if (null == this.sideInputForPreText) {
			this.sideInputForPreText = records.getPipeline().apply(Create.empty(StringUtf8Coder.of()))
					.apply(View.asSingleton());
		}

		if (null == this.sideInputForPostText) {
			this.sideInputForPostText = records.getPipeline().apply(Create.empty(StringUtf8Coder.of()))
					.apply(View.asSingleton());
		}

		PCollection<String> rows = records.apply("ConvertToRow", ParDo.of(new BuildRowFn()))
				.apply("Combine", Combine.globally(new TextCombiner())).setCoder(StringUtf8Coder.of())
				.apply("Finalizer",
						ParDo.of(new TextFinalizerFn()).withSideInputs(this.sideInputForPreText,
								this.sideInputForPostText))
				.apply("Encryptor", ParDo.of(new TextEncryptorFn(this.cryptorInfoSupplier)));

		PCollection<KV<Void, String>> destinationFiles = null;

		if (temporary) {

			destinationFiles = rows
					.apply("Write data to temporary file", FileIO.<String>write().via(TextIO.sink())
							.withNaming(new CustomFileNaming()).withTempDirectory(path).withNumShards(1))
					.getPerDestinationOutputFilenames();

		} else {

			destinationFiles = rows
					.apply("Write data to file", FileIO.<String>write().via(TextIO.sink())
							.withNaming(new CustomFileNaming()).to(path).withNumShards(1))
					.getPerDestinationOutputFilenames();
		}

		return destinationFiles.apply("Wait to return Void", ParDo.of(new DoFn<KV<Void, String>, Void>() {

			private static final long serialVersionUID = 1L;

			@ProcessElement
			public void process(ProcessContext c) {
			}

		}));

	}

	private class TextEncryptorFn extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;

		private Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier;
		private PGPCustomCryptorInfo cryptorInfo;

		public TextEncryptorFn(Supplier<PGPCustomCryptorInfo> cryptorInfoSupplier) {
			this.cryptorInfoSupplier = cryptorInfoSupplier;
		}

		@StartBundle
		public void startBundle() throws Exception {
			this.cryptorInfo = this.cryptorInfoSupplier.get();
		}

		@ProcessElement
		public void processElement(ProcessContext context) throws Exception {

			try {

				Encryptor encryptor = new Encryptor();

				if (!ObjectUtil.isEmpty(context.element())) {

					if (this.cryptorInfo.isCryptionEnabled()) {

						InputStream inputDataStream = new ByteArrayInputStream(
								context.element().getBytes(Charset.defaultCharset()));

						InputStream publicKeyStream = new ByteArrayInputStream(
								this.cryptorInfo.getPublicKey().getBytes(Charset.defaultCharset()));

						String encryptedOutput = null;

						if (this.cryptorInfo.isSigned()) {

							InputStream privateKeyStream = new ByteArrayInputStream(
									this.cryptorInfo.getPrivateKey().getBytes(Charset.defaultCharset()));

							encryptedOutput = new String(
									encryptor.encryptDataWithSignature(inputDataStream, publicKeyStream,
											privateKeyStream, this.cryptorInfo.getPassPhrase().toCharArray()),
									Charset.defaultCharset());

						} else {

							encryptedOutput = new String(
									encryptor.encryptDataWithoutSignature(inputDataStream, publicKeyStream),
									Charset.defaultCharset());
						}

						context.output(encryptedOutput);

					} else {

						context.output(context.element());
					}
				}

			} catch (Exception ex) {

				log.error("Error while processing input", ex);

				throw ex;
			}
		}
	}

	private class TextFinalizerFn extends DoFn<String, String> {

		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(ProcessContext context) throws Exception {

			String preText = getSideInput("sideInputForPreText", context, sideInputForPreText, null);
			String postText = getSideInput("sideInputForPostText", context, sideInputForPostText, null);

			StringBuilder sb = new StringBuilder();

			if (!StringUtils.isEmpty(header) && !StringUtils.isEmpty(context.element())) {
				sb.append(header);
				sb.append("\n");
			}

			if (!StringUtils.isEmpty(preText)) {
				sb.append(preText);
				sb.append("\n");
			}

			sb.append(context.element());

			if (!StringUtils.isEmpty(postText)) {
				sb.append("\n");
				sb.append(postText);
			}

			if (!StringUtils.isEmpty(footer) && !StringUtils.isEmpty(context.element())) {
				sb.append("\n");
				sb.append(footer);
			}

			context.output(sb.toString());
		}

		private String getSideInput(String sideInputName, ProcessContext context, PCollectionView<String> sideInputView,
				String defValue) {

			try {

				return context.sideInput(sideInputView);

			} catch (Exception ex) {
				log.info("Returning default value, since error while accessing side input <" + sideInputName
						+ "> with message: " + ex.getMessage());
			}

			return defValue;
		}
	}

	static class TextCombiner
			extends Combine.AccumulatingCombineFn<String, PGPEncryptCustomFileIO.TextCombiner.TextAccumulator, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public TextAccumulator createAccumulator() {
			return new TextAccumulator();
		}

		@Override
		public Coder<TextAccumulator> getAccumulatorCoder(CoderRegistry registry, Coder<String> inputCoder) {
			return new StringAccumCoder();
		}

		static class StringAccumCoder extends AtomicCoder<TextAccumulator> {

			private static final long serialVersionUID = 1L;
			private static final Coder<String> STRING_CODER = StringUtf8Coder.of();

			@Override
			public void encode(TextAccumulator value, OutputStream outStream) throws IOException {
				STRING_CODER.encode(value.extractOutput(), outStream);
			}

			@Override
			public TextAccumulator decode(InputStream inStream) throws IOException {
				return new TextAccumulator(STRING_CODER.decode(inStream));
			}
		}

		static class TextAccumulator
				implements Combine.AccumulatingCombineFn.Accumulator<String, TextAccumulator, String> {

			private List<String> strings = new ArrayList<String>();

			public TextAccumulator() {
				super();
			}

			public TextAccumulator(String initialValue) {
				super();
				addInput(initialValue);
			}

			@Override
			public void addInput(String input) {
				strings.add(input);
			}

			@Override
			public void mergeAccumulator(TextAccumulator other) {
				strings.addAll(other.strings);
			}

			@Override
			public String extractOutput() {

				return Optional.of(this.strings).orElse(new ArrayList<String>()).stream()
						.collect(Collectors.joining("\n"));
			}
		}
	}

	private class Encryptor {

		private static final String BC = "BC";
		private static final int BUFFER_SIZE = 4096;

		public Encryptor() {
			Security.addProvider(new BouncyCastleProvider());
		}

		public byte[] encryptDataWithoutSignature(InputStream inputDataStream, InputStream publicKeyForEncryption)
				throws Exception {

			OutputStream encryptedOutputStream = new ByteArrayOutputStream();
			unsignedEncryptedMessage(inputDataStream, encryptedOutputStream, publicKeyForEncryption);

			return ((ByteArrayOutputStream) encryptedOutputStream).toByteArray();
		}

		public byte[] encryptDataWithSignature(InputStream inputDataStream, InputStream publicKeyForEncryption,
				InputStream secretKeyForSigning, char[] passPhraseForSecretKey) throws Exception {

			OutputStream encryptedOutputStream = new ByteArrayOutputStream();
			signedEncryptedMessage(inputDataStream, encryptedOutputStream, publicKeyForEncryption, secretKeyForSigning,
					passPhraseForSecretKey);

			return ((ByteArrayOutputStream) encryptedOutputStream).toByteArray();
		}

		private void unsignedEncryptedMessage(InputStream dataInputStream, OutputStream encryptedOutputStream,
				InputStream custPubEncKeyInputStream) throws Exception {

			if (null != dataInputStream) {

				PGPPublicKey encPublicKey = readPublicKey(custPubEncKeyInputStream);

				encryptedOutputStream = new ArmoredOutputStream(encryptedOutputStream);

				PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(
						new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5).setWithIntegrityPacket(true)
								.setSecureRandom(new SecureRandom()));
				encryptedDataGenerator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encPublicKey));

				try (OutputStream compressedOutputStream = new PGPCompressedDataGenerator(PGPCompressedData.ZIP).open(
						encryptedDataGenerator.open(encryptedOutputStream, new byte[BUFFER_SIZE]),
						new byte[BUFFER_SIZE])) {

					try (OutputStream pOut = new PGPLiteralDataGenerator().open(compressedOutputStream,
							PGPLiteralData.BINARY, UUID.randomUUID().toString(), new Date(), new byte[BUFFER_SIZE])) {

						byte[] buffer = new byte[BUFFER_SIZE];
						int len;

						while ((len = dataInputStream.read(buffer)) > 0) {
							pOut.write(buffer, 0, len);
						}
					}

				} catch (Exception ex) {
					log.error("Exception occured during encrypting and signing the message: " + ex.getMessage(), ex);
					throw ex;
				} finally {
					try {
						if (encryptedDataGenerator != null) {
							encryptedDataGenerator.close();
						}
						if (encryptedOutputStream != null) {
							encryptedOutputStream.close();
						}
					} catch (IOException ex) {
						log.error(ex.getMessage(), ex);
					}
				}
			}
		}

		private void signedEncryptedMessage(InputStream dataInputStream, OutputStream encryptedOutputStream,
				InputStream custPubEncKeyInputStream, InputStream secretKeyInputStream, char[] passPhraseForSecretKey)
				throws Exception {

			if (null != dataInputStream) {

				PGPPublicKey encPublicKey = readPublicKey(custPubEncKeyInputStream);
				PGPSecretKey secretKey = readSecretKey(secretKeyInputStream);
				PGPPrivateKey pgpPrivKey;

				try {
					pgpPrivKey = secretKey.extractPrivateKey(
							new JcePBESecretKeyDecryptorBuilder().setProvider(BC).build(passPhraseForSecretKey));
				} catch (PGPException ex) {
					log.error("Exception occured during extracting private key: " + ex.getMessage(), ex);
					throw ex;
				}

				encryptedOutputStream = new ArmoredOutputStream(encryptedOutputStream);

				PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(
						new JcePGPDataEncryptorBuilder(PGPEncryptedData.CAST5).setWithIntegrityPacket(true)
								.setSecureRandom(new SecureRandom()));
				encryptedDataGenerator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(encPublicKey));

				try (OutputStream compressedOutputStream = new PGPCompressedDataGenerator(PGPCompressedData.ZIP).open(
						encryptedDataGenerator.open(encryptedOutputStream, new byte[BUFFER_SIZE]),
						new byte[BUFFER_SIZE])) {

					PGPSignatureGenerator signatureGenerator = new PGPSignatureGenerator(
							new BcPGPContentSignerBuilder(encPublicKey.getAlgorithm(), HashAlgorithmTags.SHA512));

					signatureGenerator.init(PGPSignature.BINARY_DOCUMENT, pgpPrivKey);
					signatureGenerator.generateOnePassVersion(true).encode(compressedOutputStream);

					try (OutputStream pOut = new PGPLiteralDataGenerator().open(compressedOutputStream,
							PGPLiteralData.BINARY, UUID.randomUUID().toString(), new Date(), new byte[BUFFER_SIZE])) {

						byte[] buffer = new byte[BUFFER_SIZE];
						int len;

						while ((len = dataInputStream.read(buffer)) > 0) {
							pOut.write(buffer, 0, len);
							signatureGenerator.update(buffer, 0, len);
						}
					}

					signatureGenerator.generate().encode(compressedOutputStream);

				} catch (Exception ex) {
					log.error("Exception occured during encrypting and signing the message: " + ex.getMessage(), ex);
					throw ex;
				} finally {
					try {
						if (encryptedDataGenerator != null) {
							encryptedDataGenerator.close();
						}
						if (encryptedOutputStream != null) {
							encryptedOutputStream.close();
						}
					} catch (IOException ex) {
						log.error(ex.getMessage(), ex);
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

}
