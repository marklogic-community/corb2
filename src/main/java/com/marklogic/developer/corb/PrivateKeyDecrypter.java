/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import static com.marklogic.developer.corb.Options.PRIVATE_KEY_ALGORITHM;
import static com.marklogic.developer.corb.Options.PRIVATE_KEY_FILE;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.isNotBlank;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.text.MessageFormat;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import java.util.Base64;
import java.util.logging.Logger;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * Decrypter implementation that uses asymmetric (public/private key) encryption for securing sensitive configuration values.
 * <p>
 * PrivateKeyDecrypter uses RSA or other asymmetric encryption algorithms to decrypt values that were encrypted
 * with a public key. This provides strong security for sensitive configuration like database passwords and URIs.
 * The private key is used for decryption at runtime, while the public key can be used to encrypt values offline.
 * </p>
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Asymmetric encryption - Different keys for encryption and decryption</li>
 * <li>Supports multiple algorithms (RSA, RSA/ECB/PKCS1Padding, etc.)</li>
 * <li>Compatible with OpenSSL-generated keys</li>
 * <li>Command-line utilities for key generation and encryption</li>
 * <li>Graceful handling of both encrypted and plain-text values</li>
 * <li>PKCS8 format support with or without Base64 encoding</li>
 * </ul>
 * <p>
 * <b>Configuration:</b>
 * </p>
 * <ul>
 * <li>{@link Options#DECRYPTER} - Set to {@code com.marklogic.developer.corb.PrivateKeyDecrypter}</li>
 * <li>{@link Options#PRIVATE_KEY_FILE} - Path to the private key file (required)</li>
 * <li>{@link Options#PRIVATE_KEY_ALGORITHM} - Encryption algorithm (optional, default: RSA)</li>
 * </ul>
 * <p>
 * <b>Usage Workflow:</b>
 * </p>
 * <ol>
 * <li><b>Generate Keys</b> - Use the gen-keys command or OpenSSL to create a key pair</li>
 * <li><b>Encrypt Values</b> - Use the encrypt command or OpenSSL to encrypt sensitive values with the public key</li>
 * <li><b>Configure CoRB</b> - Add the private key file and encrypted values to your CoRB configuration</li>
 * <li><b>Run CoRB</b> - Values are automatically decrypted at runtime using the private key</li>
 * </ol>
 * <p>
 * <b>Option 1: Generate Keys with Java</b>
 * </p>
 * <pre>
 * # Generate RSA 1024-bit keys
 * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
 *   gen-keys private.key public.key RSA 1024
 *
 * # Encrypt a password
 * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
 *   encrypt public.key "myPassword" RSA
 * </pre>
 * <p>
 * <b>Option 2: Generate Keys with OpenSSL</b>
 * </p>
 * <pre>
 * # Generate private key
 * openssl genrsa -out private.pem 1024
 *
 * # Convert to PKCS8 format
 * openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key
 *
 * # Extract public key
 * openssl rsa -in private.pem -pubout &gt; public.key
 *
 * # Encrypt a password
 * echo "myPassword" | openssl rsautl -encrypt -pubin -inkey public.key | base64
 * </pre>
 * <p>
 * <b>Option 3: Use SSH Keys</b>
 * </p>
 * <pre>
 * # Generate SSH key
 * ssh-keygen -f id_rsa
 *
 * # Convert to PKCS8 format
 * openssl pkcs8 -topk8 -nocrypt -in id_rsa -out id_rsa.pkcs8.key
 *
 * # Extract public key
 * openssl rsa -in id_rsa -pubout &gt; public.key
 *
 * # Encrypt a password
 * echo "myPassword" | openssl rsautl -encrypt -pubin -inkey public.key | base64
 * </pre>
 * <p>
 * <b>Example Configuration:</b>
 * </p>
 * <pre>
 * # corb.properties
 * DECRYPTER=com.marklogic.developer.corb.PrivateKeyDecrypter
 * PRIVATE-KEY-FILE=path/to/private.key
 * PRIVATE-KEY-ALGORITHM=RSA
 *
 * # Encrypted password (Base64 encoded)
 * XCC-PASSWORD=JHlWZ4fG3hS... (encrypted value)
 * </pre>
 * <p>
 * <b>Key Format Requirements:</b>
 * </p>
 * <ul>
 * <li>Private key must be in PKCS8 format</li>
 * <li>Can be raw bytes or Base64 encoded (with or without PEM headers)</li>
 * <li>Public key should be in X509 format for encryption</li>
 * <li>Encrypted values must be Base64 encoded</li>
 * </ul>
 * <p>
 * <b>Security Considerations:</b>
 * </p>
 * <ul>
 * <li>Keep the private key secure and restrict file permissions</li>
 * <li>Never commit the private key to version control</li>
 * <li>The public key can be distributed safely</li>
 * <li>Use strong key lengths (2048 bits or higher for production)</li>
 * <li>Consider using hardware security modules (HSM) for private key storage in production</li>
 * </ul>
 * <p>
 * <b>Error Handling:</b>
 * </p>
 * <p>
 * If decryption fails (wrong key, value not encrypted, or corrupted data), the decrypter
 * returns the original value unchanged. This allows mixed configurations with both encrypted
 * and plain-text values. A warning is logged but execution continues.
 * </p>
 *
 * @author MarkLogic Corporation
 * @since 2.0.0
 * @see AbstractDecrypter
 * @see Options#DECRYPTER
 * @see Options#PRIVATE_KEY_FILE
 * @see Options#PRIVATE_KEY_ALGORITHM
 */
public class PrivateKeyDecrypter extends AbstractDecrypter {

    /**
     * Default encryption algorithm used when none is specified.
     * RSA is widely supported and provides strong asymmetric encryption.
     */
    private static final String DEFAULT_ALGORITHM = "RSA";
    /**
     * Usage instructions for the gen-keys command-line utility.
     * Displayed when invalid arguments are provided or when no command is specified.
     */
    protected static final String GEN_KEYS_USAGE = "Generate Keys (Note: default algorithm: RSA, default key-length: 1024):\n java -cp marklogic-corb-"
            + AbstractManager.VERSION + ".jar " + PrivateKeyDecrypter.class.getName() + " gen-keys /path/to/private.key /path/to/public.key RSA 1024";
    /**
     * Usage instructions for the encrypt command-line utility.
     * Displayed when invalid arguments are provided or when no command is specified.
     */
    protected static final String ENCRYPT_USAGE = "Encrypt (Note: default algorithm: RSA):\n java -cp marklogic-corb-"
            + AbstractManager.VERSION + ".jar " + PrivateKeyDecrypter.class.getName() + " encrypt /path/to/public.key clearText RSA";

    /**
     * The encryption algorithm to use for decryption.
     * Configured via {@link Options#PRIVATE_KEY_ALGORITHM} or defaults to RSA.
     * Common values include "RSA", "RSA/ECB/PKCS1Padding", etc.
     */
    private String algorithm;
    // option 1 - generate keys with java
    // java -cp marklogic-corb-2.1.*.jar
    // com.marklogic.developer.corb.PrivateKeyDecrypter gen-keys
    // /path/to/private.key /path/to/public.key
    // java -cp marklogic-corb-2.1.*.jar
    // com.marklogic.developer.corb.PrivateKeyDecrypter encrypt
    // /path/to/public.key clearText
    //
    // option 2 - generate keys with openssl
    // openssl genrsa -out private.pem 1024
    // openssl pkcs8 -topk8 -nocrypt -in private.pem -out private.pkcs8.key
    // openssl rsa -in private.pem -pubout > public.key
    // echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key |
    // base64
    //
    // option 3 - ssh-keygen
    // ssh-keygen (ex: gen key as id_rsa)
    // openssl pkcs8 -topk8 -nocrypt -in id_rsa -out id_rsa.pkcs8.key
    // openssl rsa -in id_rsa -pubout > public.key
    // echo "password or uri" | openssl rsautl -encrypt -pubin -inkey public.key |
    // base64
    /**
     * The private key used for decryption.
     * Loaded from the file specified by {@link Options#PRIVATE_KEY_FILE} during initialization.
     * Expected to be in PKCS8 format, either raw bytes or Base64 encoded.
     */
    private PrivateKey privateKey;

    /**
     * Logger for this class.
     * Used to log initialization status, decryption attempts, and errors.
     */
    protected static final Logger LOG = Logger.getLogger(PrivateKeyDecrypter.class.getName());

    /**
     * Initializes the decrypter by loading the private key.
     * <p>
     * The initialization process:
     * </p>
     * <ol>
     * <li>Determines the encryption algorithm (from {@link Options#PRIVATE_KEY_ALGORITHM} or default RSA)</li>
     * <li>Locates the private key file (from {@link Options#PRIVATE_KEY_FILE})</li>
     * <li>Attempts to load the key from classpath first, then filesystem</li>
     * <li>Parses the key as PKCS8 format (raw bytes or Base64 encoded)</li>
     * <li>Initializes the PrivateKey instance for later decryption</li>
     * </ol>
     * <p>
     * The private key file can be:
     * </p>
     * <ul>
     * <li>A resource in the classpath (e.g., embedded in JAR)</li>
     * <li>A file on the filesystem (absolute or relative path)</li>
     * <li>In PKCS8 format with or without Base64 encoding</li>
     * <li>With or without PEM headers (BEGIN/END PRIVATE KEY lines)</li>
     * </ul>
     * <p>
     * If the key cannot be loaded as raw bytes, the method attempts to decode it
     * as Base64, which is common for OpenSSL-generated keys.
     * </p>
     *
     * @throws IOException if the key file cannot be read
     * @throws ClassNotFoundException if required crypto classes are not available
     * @throws IllegalStateException if the private key file cannot be found or loaded
     */
    @Override
    protected void init_decrypter() throws IOException, ClassNotFoundException {
        algorithm = getProperty(PRIVATE_KEY_ALGORITHM);
        if (isBlank(algorithm)) {
            algorithm = DEFAULT_ALGORITHM;
        }

        String filename = getProperty(PRIVATE_KEY_FILE);
        if (isNotBlank(filename)) {
            InputStream is = null;
            try {
                is = Manager.class.getResourceAsStream('/' + filename);
                if (is != null) {
                    LOG.log(INFO, () -> MessageFormat.format("Loading private key file {0} from classpath", filename));
                } else {
                    File f = new File(filename);
                    if (f.exists() && !f.isDirectory()) {
                        LOG.log(INFO, () -> MessageFormat.format("Loading private key file {0} from filesystem", filename));
                        is = new FileInputStream(f);
                    } else {
                        throw new IllegalStateException("Unable to load " + filename);
                    }
                }
                byte[] keyAsBytes = toByteArray(is);

                KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
                try {
                    privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(keyAsBytes));
                } catch (Exception exc) {
                    LOG.log(INFO, "Attempting to decode private key with base64. Ignore this message if keys are generated with openssl", exc);
                    String keyAsString = new String(keyAsBytes);
                    // remove the begin and end key lines if present.
                    keyAsString = keyAsString.replaceAll("[-]+(BEGIN|END)[A-Z ]*KEY[-]+", "");

                    privateKey = keyFactory.generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(keyAsString)));
                }
                LOG.log(INFO, "Initialized PrivateKeyDecrypter");
            } catch (Exception exc) {
                LOG.log(SEVERE, "Problem initializing PrivateKeyDecrypter");
            } finally {
                closeQuietly(is);
            }
        } else {
            LOG.log(SEVERE, () -> MessageFormat.format("{0} property must be defined", PRIVATE_KEY_FILE));
        }
    }

    /**
     * Copies data from an InputStream to an OutputStream.
     * <p>
     * This is a utility method for reading key files.
     * </p>
     *
     * @param input the source stream
     * @param output the destination stream
     * @throws IOException if an I/O error occurs
     */
    private static void copy(InputStream input, OutputStream output) throws IOException {
        byte[] buffer = new byte[1024];
        int n;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
    }

    /**
     * Reads an entire InputStream into a byte array.
     * <p>
     * This is used to read the private key file contents.
     * </p>
     *
     * @param input the input stream to read
     * @return byte array containing all data from the stream
     * @throws IOException if an I/O error occurs
     */
    private static byte[] toByteArray(final InputStream input) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        copy(input, output);
        return output.toByteArray();
    }

    /**
     * Decrypts an encrypted value using the private key.
     * <p>
     * The decryption process:
     * </p>
     * <ol>
     * <li>Expects the encrypted value to be Base64 encoded</li>
     * <li>Decodes the Base64 string to bytes</li>
     * <li>Decrypts the bytes using the private key and configured algorithm</li>
     * <li>Returns the decrypted string (trimmed)</li>
     * </ol>
     * <p>
     * If decryption fails (e.g., value is not encrypted, wrong key, or corrupted),
     * the original value is returned unchanged. This allows the decrypter to
     * gracefully handle both encrypted and plain-text values.
     * </p>
     *
     * @param property the name of the property being decrypted (for logging)
     * @param value the encrypted value (Base64 encoded)
     * @return the decrypted value (trimmed), or the original value if decryption fails
     */
    @Override
    protected String doDecrypt(String property, String value) {
        String dValue = null;
        if (privateKey != null) {
            try {
                final Cipher cipher = Cipher.getInstance(algorithm);
                cipher.init(Cipher.DECRYPT_MODE, privateKey);
                dValue = new String(cipher.doFinal(Base64.getDecoder().decode(value)));
            } catch (NoSuchAlgorithmException | NoSuchPaddingException | InvalidKeyException | IllegalArgumentException | IllegalBlockSizeException | BadPaddingException exc) {
                LOG.log(INFO, MessageFormat.format("Cannot decrypt {0}. Ignore if clear text.", property), exc);
            }
        }
        return dValue == null ? value : dValue.trim();
    }

    /**
     * Generates a new RSA key pair and saves to files.
     * <p>
     * Command-line arguments:
     * </p>
     * <ol>
     * <li>args[0] - "gen-keys" command</li>
     * <li>args[1] - Path for private key file</li>
     * <li>args[2] - Path for public key file</li>
     * <li>args[3] - Algorithm (optional, default: RSA)</li>
     * <li>args[4] - Key length in bits (optional, default: 1024)</li>
     * </ol>
     * <p>
     * Generated keys are in raw binary format (PKCS8 for private, X509 for public).
     * The private key can be used directly with this decrypter. The public key
     * can be used with the {@link #encrypt(String...)} method or OpenSSL to
     * encrypt values.
     * </p>
     * <p>
     * Example:
     * </p>
     * <pre>
     * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
     *   gen-keys private.key public.key RSA 2048
     * </pre>
     *
     * @param args command-line arguments
     * @throws Exception if key generation fails
     */
    private static void generateKeys(String... args) throws Exception {
        String algorithm = DEFAULT_ALGORITHM;
        int length = 1024;
        String privateKeyPathName = null;
        String publicKeyPathName = null;

        if (args.length > 1 && isNotBlank(args[1])) {
            privateKeyPathName = args[1].trim();
        }
        if (args.length > 2 && isNotBlank(args[2])) {
            publicKeyPathName = args[2].trim();
        }
        if (args.length > 3 && isNotBlank(args[3])) {
            algorithm = args[3].trim();
        }
        if (args.length > 4 && isNotBlank(args[4])) {
            length = Integer.parseInt(args[4].trim());
        }
        if (privateKeyPathName == null || publicKeyPathName == null) {
            System.err.println(GEN_KEYS_USAGE); // NOPMD
            return;
        }

        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(algorithm);
        keyPairGenerator.initialize(length);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        PrivateKey privateKey = keyPair.getPrivate();
        PublicKey publicKey = keyPair.getPublic();

        try (FileOutputStream privateFos = new FileOutputStream(privateKeyPathName);
             FileOutputStream publicFos = new FileOutputStream(publicKeyPathName)) {

            privateFos.write(privateKey.getEncoded());
            System.out.println("Generated private key: " + privateKeyPathName); // NOPMD

            publicFos.write(publicKey.getEncoded());
            System.out.println("Generated public key: " + publicKeyPathName); // NOPMD
        }
    }

    /**
     * Encrypts a plain-text value using a public key.
     * <p>
     * Command-line arguments:
     * </p>
     * <ol>
     * <li>args[0] - "encrypt" command</li>
     * <li>args[1] - Path to public key file</li>
     * <li>args[2] - Plain-text value to encrypt</li>
     * <li>args[3] - Algorithm (optional, default: RSA)</li>
     * </ol>
     * <p>
     * The encrypted output is Base64 encoded and can be used directly in
     * CoRB configuration files. The public key should be in X509 format
     * (generated by {@link #generateKeys(String...)} or OpenSSL).
     * </p>
     * <p>
     * Example:
     * </p>
     * <pre>
     * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
     *   encrypt public.key "myPassword" RSA
     * </pre>
     *
     * @param args command-line arguments
     * @throws Exception if encryption fails
     */
    private static void encrypt(String... args) throws Exception {
        String algorithm = DEFAULT_ALGORITHM;
        String publicKeyPathName = null;
        String clearText = null;
        if (args.length > 1 && isNotBlank(args[1])) {
            publicKeyPathName = args[1].trim();
        }
        if (args.length > 2 && isNotBlank(args[2])) {
            clearText = args[2].trim();
        }
        if (args.length > 3 && isNotBlank(args[3])) {
            algorithm = args[3].trim();
        }
        if (publicKeyPathName == null || clearText == null) {
            System.err.println(ENCRYPT_USAGE); // NOPMD
            return;
        }

        try (FileInputStream fis = new FileInputStream(publicKeyPathName)) {
            X509EncodedKeySpec x509EncodedKeySpec = new X509EncodedKeySpec(toByteArray(fis));
            Cipher cipher = Cipher.getInstance(algorithm);
            cipher.init(Cipher.ENCRYPT_MODE, KeyFactory.getInstance(algorithm).generatePublic(x509EncodedKeySpec));
            String encryptedText = Base64.getEncoder().encodeToString(cipher.doFinal(clearText.getBytes("UTF-8")));
            System.out.println("Input: " + clearText + "\nOutput: " + encryptedText); // NOPMD
        }
    }

    /**
     * Main entry point for command-line key generation and encryption utilities.
     * <p>
     * Supported commands:
     * </p>
     * <ul>
     * <li><b>gen-keys</b> - Generate a new private/public key pair</li>
     * <li><b>encrypt</b> - Encrypt a value using a public key</li>
     * </ul>
     * <p>
     * If no command or an invalid command is provided, usage information is printed.
     * </p>
     * <p>
     * Examples:
     * </p>
     * <pre>
     * # Generate keys
     * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
     *   gen-keys private.key public.key
     *
     * # Encrypt a password
     * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter \
     *   encrypt public.key "mySecretPassword"
     *
     * # Show usage
     * java -cp marklogic-corb.jar com.marklogic.developer.corb.PrivateKeyDecrypter
     * </pre>
     *
     * @param args command-line arguments
     * @throws Exception if an error occurs during processing
     */
    public static void main(String... args) throws Exception {
        String method = (args != null && args.length > 0) ? args[0].trim() : "";
        if ("gen-keys".equals(method)) {
            generateKeys(args);
        } else if ("encrypt".equals(method)) {
            encrypt(args);
        } else {
            System.out.println(GEN_KEYS_USAGE + '\n' + ENCRYPT_USAGE); // NOPMD
        }
    }
}
