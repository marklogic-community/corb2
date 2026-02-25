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

import com.marklogic.developer.corb.util.StringUtils;

import static com.marklogic.developer.corb.util.StringUtils.byteArrayToHexString;
import static com.marklogic.developer.corb.util.StringUtils.hexStringToByteArray;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.file.ProviderNotFoundException;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.Optional;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

/**
 * A host-specific decrypter that derives encryption keys from hardware and network characteristics.
 * <p>
 * This class extends {@link AbstractDecrypter} and provides a security mechanism that binds
 * encrypted values to a specific host machine. The encryption key is derived from multiple
 * host-specific identifiers:
 * </p>
 * <ul>
 *   <li>Hardware BIOS serial number</li>
 *   <li>Network MAC address</li>
 *   <li>Hostname</li>
 *   <li>Hard-coded salt bytes</li>
 * </ul>
 * <p>
 * These values are combined using XOR operations and hashed with SHA-256 to create a unique
 * 256-bit AES encryption key for each host.
 * </p>
 * <b>Security Model:</b>
 * Encrypted values are bound to the specific machine where they were encrypted. This means:
 * <ul>
 *   <li>Values encrypted on Machine A cannot be decrypted on Machine B</li>
 *   <li>Values remain encrypted in configuration files</li>
 *   <li>Keys are derived at runtime and never stored</li>
 *   <li>Changing hardware (MAC address, BIOS) invalidates encrypted values</li>
 * </ul>
 * <b>Use Cases:</b>
 * This decrypter is suitable for:
 * <ul>
 *   <li>Encrypting passwords in configuration files</li>
 *   <li>Preventing plaintext credentials in source control</li>
 *   <li>Single-machine deployments where credentials don't need portability</li>
 * </ul>
 * <b>Limitations:</b>
 * <ul>
 *   <li>Encrypted values are <b>not portable</b> between machines</li>
 *   <li>Hardware changes may invalidate encrypted values</li>
 *   <li>Not suitable for distributed deployments with shared configuration</li>
 *   <li>Requires OS-specific commands to retrieve BIOS serial numbers</li>
 * </ul>
 * <p>
 * <b>Encryption Algorithm:</b>
 * Uses AES-256 with {@link SecureRandom} for initialization vector generation.
 * Encrypted values are returned as hexadecimal strings for safe storage in text files.
 * </p>
 * <p>
 * <b>Platform Support:</b>
 * </p>
 * <ul>
 *   <li><b>Windows:</b> Uses {@code wmic bios get serialnumber} command</li>
 *   <li><b>macOS:</b> Uses {@code system_profiler SPHardwareDataType} command</li>
 *   <li><b>Linux:</b> Uses {@code lshal} command (deprecated), falls back to default if unavailable</li>
 *   <li><b>Other:</b> Uses default serial number bytes</li>
 * </ul>
 * <p>
 * <b>Command Line Usage:</b><br>
 * Can be run standalone to encrypt passwords:
 * </p>
 * <pre>
 * java -cp marklogic-corb-VERSION.jar com.marklogic.developer.corb.HostKeyDecrypter encrypt myPassword
 * </pre>
 * Or to test encryption/decryption:
 * <pre>
 * java -cp marklogic-corb-VERSION.jar com.marklogic.developer.corb.HostKeyDecrypter test
 * </pre>
 *
 * @author Richard Kennedy
 * @since 2.2.0
 * @see AbstractDecrypter
 * @see JasyptDecrypter
 */
public class HostKeyDecrypter extends AbstractDecrypter {

    /**
     * The host-specific private key derived from hardware and network identifiers.
     * <p>
     * This 256-bit AES key is derived at runtime from:
     * <ul>
     *   <li>Hardware MAC address</li>
     *   <li>Hostname</li>
     *   <li>BIOS serial number</li>
     *   <li>Hard-coded salt bytes ({@link #HARD_CODED_BYTES})</li>
     * </ul>
     * These values are combined using XOR operations and hashed with SHA-256 to produce
     * a unique key for each host machine. The key is initialized once during
     * {@link #init_decrypter()} and reused for all encryption/decryption operations.
     * </p>
     * <p>
     * <b>Important:</b> This key is <b>never stored</b> on disk and is regenerated each
     * time the application starts, ensuring that encrypted values remain bound to the
     * specific host machine.
     * </p>
     */
    private static byte[] privateKey;

    /**
     * Default fallback bytes used when BIOS serial number cannot be retrieved.
     * <p>
     * These bytes are used by {@link OSType#OTHER} when the operating system is unsupported
     * or when platform-specific commands for retrieving the BIOS serial number fail.
     * This ensures that encryption/decryption can still function, though with reduced
     * host-specificity.
     * </p>
     * <p>
     * Contains 15 hardcoded byte values that serve as a substitute for the BIOS serial number
     * in the key derivation process.
     * </p>
     */
    private static final byte[] DEFAULT_BYTES = {45, 32, 67, 34, 67, 23, 21, 45, 7, 89, 3, 27, 39, 62, 15};

    /**
     * Hard-coded salt bytes mixed into the key derivation process.
     * <p>
     * These 32 bytes are XORed with the MAC address, hostname, and BIOS serial number
     * during key generation to add an additional layer of entropy and prevent rainbow
     * table attacks. The salt is constant across all hosts and is part of the application's
     * security design.
     * </p>
     * <p>
     * While this salt is hard-coded and visible in the source code, the security model
     * relies primarily on the host-specific identifiers (MAC, hostname, BIOS serial) which
     * vary between machines and are not stored.
     * </p>
     */
    private static final byte[] HARD_CODED_BYTES = {120, 26, 58, 29, 43, 77, 95, 103, 29, 86, 97, 105, 52, 16, 42, 63, 37, 100, 45, 109, 108, 79, 75, 71, 11, 46, 36, 62, 124, 12, 7, 127};

    /**
     * Encryption algorithm identifier for AES (Advanced Encryption Standard).
     * <p>
     * Used when creating {@link SecretKeySpec} instances and initializing {@link Cipher} objects.
     * The actual key size (256 bits) is determined by the length of {@link #privateKey}.
     * </p>
     */
    private static final String AES = "AES";

    /**
     * Format string template for command-line usage messages.
     * <p>
     * This format string is used with {@link MessageFormat} to generate usage instructions
     * for the command-line interface. It includes the CoRB version number and class name,
     * with a placeholder {0} for the specific command and arguments.
     * </p>
     * <p>
     * Example output: {@code java -cp marklogic-corb-2.5.0.jar com.marklogic.developer.corb.HostKeyDecrypter encrypt clearText}
     * </p>
     */
    private static final String USAGE_FORMAT = "java -cp marklogic-corb-" + AbstractManager.VERSION + ".jar " + HostKeyDecrypter.class.getName() + "{0} ";

    /**
     * Exception message template for when a BIOS serial number cannot be retrieved.
     * <p>
     * Used with {@link MessageFormat} to provide context-specific error messages.
     * The placeholder {0} is replaced with the operating system type (e.g., "WINDOWS", "MAC").
     * </p>
     */
    private static final String EXCEPTION_MGS_SERIAL_NOT_FOUND = "Unable to find serial number on {0}";

    /**
     * Command-line method identifier for the test mode.
     * <p>
     * When this value is passed as the first argument to {@link #main(String...)}, the
     * program will encrypt and decrypt a sample password to verify functionality.
     * </p>
     */
    private static final String METHOD_TEST = "test";

    /**
     * Command-line method identifier for the encrypt mode.
     * <p>
     * When this value is passed as the first argument to {@link #main(String...)}, the
     * program will encrypt the provided password and output the encrypted hexadecimal string.
     * </p>
     */
    private static final String METHOD_ENCRYPT = "encrypt";

    /**
     * Usage instructions for the command-line interface.
     * <p>
     * This string is displayed when the program is run without valid arguments or with
     * incorrect arguments. It provides examples of how to use both the encrypt and test modes.
     * </p>
     * <p>
     * Generated dynamically using {@link #USAGE_FORMAT} to include the current CoRB version
     * and class name.
     * </p>
     */
    // currently only usage is encrypt
    protected static final String USAGE = "Encrypt:\n "
            + MessageFormat.format(USAGE_FORMAT, METHOD_ENCRYPT + " clearText")
            + "\nTest:\n "
            + MessageFormat.format(USAGE_FORMAT, METHOD_TEST);

    /**
     * Logger instance for this class.
     */
    protected static final Logger LOG = Logger.getLogger(HostKeyDecrypter.class.getName());

    /**
     * Enumeration of supported operating system types with platform-specific serial number retrieval.
     * <p>
     * Each OS type implements the {@link #getSN()} method to retrieve the BIOS serial number
     * using platform-specific commands:
     * </p>
     * <ul>
     *   <li><b>WINDOWS:</b> Uses {@code wmic bios get serialnumber}</li>
     *   <li><b>MAC:</b> Uses {@code system_profiler SPHardwareDataType}</li>
     *   <li><b>LINUX:</b> Uses {@code lshal} (with fallback to default)</li>
     *   <li><b>OTHER:</b> Returns default bytes</li>
     * </ul>
     */
    protected enum OSType {

        /**
         * Windows operating system type.
         * Retrieves BIOS serial number using the Windows Management Instrumentation Command-line (WMIC).
         */
        WINDOWS {
            /**
             * Retrieves the BIOS serial number on a Windows machine using WMIC.
             * <p>
             * Executes the command {@code wmic bios get serialnumber} and parses the output
             * to extract the serial number value.
             * </p>
             *
             * @return the BIOS serial number as a byte array
             * @throws IllegalStateException if the serial number cannot be found
             */
            @Override
            public byte[] getSN() {
                StringBuilder sb = new StringBuilder();
                BufferedReader br = null;
                boolean isSN = false;
                try {
                    br = read("wmic bios get serialnumber");
                    try (Scanner sc = new Scanner(br)) {
                        while (sc.hasNext()) {
                            String next = sc.next();
                            if ("SerialNumber".equals(next) || isSN) {
                                isSN = true;
                                next = sc.next();
                                sb.append(next);
                            }
                        }
                        String sn = sb.toString();
                        if (!sn.isEmpty()) {
                            return sn.getBytes();
                        } else {
                            throw new IllegalStateException(MessageFormat.format(EXCEPTION_MGS_SERIAL_NOT_FOUND, this.toString()));
                        }
                    }
                } finally {
                    closeOrThrowRuntime(br);
                }
            }
        },
        /**
         * macOS (Mac/Darwin) operating system type.
         * Retrieves hardware serial number using the system_profiler command.
         */
        MAC {
            /**
             * Retrieves the hardware serial number on a macOS machine using system_profiler.
             * <p>
             * Executes {@code /usr/sbin/system_profiler SPHardwareDataType} and extracts
             * the serial number from the output.
             * </p>
             *
             * @return the hardware serial number as a byte array
             * @throws IllegalStateException if the serial number cannot be found
             */
            @Override
            public byte[] getSN() {
                return OSType.getSN("/usr/sbin/system_profiler SPHardwareDataType", "Serial Number", this);
            }
        },
        /**
         * Linux operating system type.
         * Retrieves hardware serial number using the lshal command (part of the deprecated hal package).
         */
        LINUX {
            /**
             * Retrieves the hardware serial number on a Linux machine using lshal.
             * <p>
             * Executes the {@code lshal} command to query hardware information.
             * <b>Note:</b> lshal is deprecated in modern Linux distributions and may not be available.
             * If lshal is not found, falls back to using the default serial number bytes.
             * </p>
             *
             * @return the hardware serial number as a byte array, or default bytes if lshal is unavailable
             */
            @Override
            public byte[] getSN() {
                try {
                    return OSType.getSN("lshal", "system.hardware.serial", this);
                } catch (ProviderNotFoundException ex) {
                    //Linux distros have deprecated lshal and may not be available on modern versions
                    LOG.warning("lshal is not available on this machine. Using default Serial Number value.");
                    return OTHER.getSN();
                }
            }
        },
        /**
         * Fallback operating system type for unsupported or unknown platforms.
         * Returns default hardcoded bytes instead of attempting to retrieve a real serial number.
         */
        OTHER {
            /**
             * Returns default hardcoded bytes as a fallback serial number.
             *
             * @return the default byte array
             */
            @Override
            public byte[] getSN() {
                return DEFAULT_BYTES;
            }
        };

        /**
         * Abstract method to retrieve the serial number for the specific operating system.
         *
         * @return the BIOS or hardware serial number as a byte array
         */
        public abstract byte[] getSN();

        /**
         * Executes a system command and searches the output for a marker string to extract the serial number.
         * <p>
         * This helper method:
         * <ol>
         *   <li>Executes the specified command</li>
         *   <li>Reads the output line by line</li>
         *   <li>Finds the first line containing the marker string</li>
         *   <li>Extracts the value after the marker</li>
         * </ol>
         * </p>
         *
         * @param command the OS command to execute
         * @param marker the string marker to search for in the command output
         * @param os the OS type for error reporting
         * @return the extracted serial number as a byte array
         * @throws ProviderNotFoundException if the required command is not installed
         * @throws IllegalStateException if the serial number cannot be found in the output
         */
        private static byte[] getSN(String command, String marker, OSType os) {
            try (BufferedReader br = read(command)) {
                Optional<String> hasMatch = br.lines().filter(s -> s.contains(marker)).findFirst();
                if (hasMatch.isPresent()) {
                    String sn = hasMatch.get().split(marker)[1].trim();
                    return sn.getBytes();
                }
            } catch (RuntimeException | IOException ex) {
                throw new ProviderNotFoundException("Required to have " + command + " command installed on machine");
            }
            throw new IllegalStateException(MessageFormat.format(EXCEPTION_MGS_SERIAL_NOT_FOUND, os));
        }

        /**
         * Closes a Closeable object quietly, wrapping any IOException in a RuntimeException.
         *
         * @param obj the Closeable object to close; may be null
         * @throws RuntimeException if an IOException occurs during closing
         */
        private static void closeOrThrowRuntime(Closeable obj) {
            if (obj != null) {
                try {
                    obj.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        /**
         * Executes a system command and returns a BufferedReader for reading the output.
         * <p>
         * This method:
         * <ol>
         *   <li>Splits the command string on spaces</li>
         *   <li>Executes the command via {@link Runtime#exec(String[])}</li>
         *   <li>Closes the process output stream</li>
         *   <li>Returns a BufferedReader for the process input stream</li>
         * </ol>
         * </p>
         *
         * @param command the command to execute (will be split on spaces)
         * @return a BufferedReader for reading the command's output
         * @throws RuntimeException if an IOException occurs during command execution
         */
        private static BufferedReader read(String command) {
            Runtime runtime = Runtime.getRuntime();
            Process process;
            try {
                process = runtime.exec(command.split(" "));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            OutputStream os = process.getOutputStream();
            closeOrThrowRuntime(os);

            InputStream is = process.getInputStream();
            return new BufferedReader(new InputStreamReader(is));
        }
    }

    /**
     * Initializes the decrypter by deriving the private key from host-specific identifiers.
     * <p>
     * This method is called once during initialization to generate the AES key from:
     * </p>
     * <ul>
     *   <li>MAC address</li>
     *   <li>Hostname</li>
     *   <li>BIOS serial number</li>
     *   <li>Hard-coded salt bytes</li>
     * </ul>
     * These values are XORed together and hashed with SHA-256 to produce a 256-bit key.
     *
     * @throws IOException if an I/O error occurs
     * @throws ClassNotFoundException if a required class cannot be found
     * @throws RuntimeException if key generation fails due to NoSuchAlgorithmException
     */
    @Override
    protected synchronized void init_decrypter() throws IOException, ClassNotFoundException {
        try {
            privateKey = getPrivateKey();
            LOG.log(INFO, "Initialized HostKeyDecrypter");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error constructing private key", e);
        }
    }

    /**
     * Decrypts an encrypted property value using the host-specific key.
     * <p>
     * This method attempts to decrypt the value. If decryption fails (e.g., value was encrypted
     * on a different host), logs a warning and returns the original encrypted value.
     * </p>
     *
     * @param property the property name (for logging purposes)
     * @param value the encrypted value to decrypt
     * @return the decrypted plaintext value, or the original value if decryption fails
     */
    @Override
    protected String doDecrypt(String property, String value) {
        String decryptedText = null;
        try {
            decryptedText = decrypt(value);
        } catch (Exception e) {
            LOG.log(WARNING,"Unabled to decrypt property:" + property, e);
        }
        return decryptedText == null ? value : decryptedText;
    }

    /**
     * Performs a bitwise XOR operation on two byte arrays.
     * <p>
     * This method XORs corresponding bytes from both arrays. If arrays are of different lengths,
     * the result is the length of the longer array, with missing bytes treated as 0.
     * </p>
     * <p>
     * <b>Example:</b><br>
     * {@code xor([1, 2, 3], [4, 5]) = [5, 7, 3]}<br>
     * Where 3 is XORed with 0 (implicit).
     * </p>
     *
     * @param byteOne the first byte array
     * @param byteTwo the second byte array
     * @return a new byte array containing the XOR result
     */
    public static byte[] xor(byte[] byteOne, byte[] byteTwo) {
        int max;
        if (byteOne.length > byteTwo.length) {
            max = byteOne.length;
        } else {
            max = byteTwo.length;
        }
        byte[] result = new byte[max];
        for (int i = 0; i < max; i++) {
            byte one;
            byte two;
            if (i < byteOne.length) {
                one = byteOne[i];
            } else {
                one = (byte) 0;
            }
            if (i < byteTwo.length) {
                two = byteTwo[i];
            } else {
                two = (byte) 0;
            }
            result[i] = (byte) (one ^ two);
        }
        return result;
    }

    /**
     * Computes the SHA-256 hash of the input bytes.
     * <p>
     * This method uses the SHA-256 message digest algorithm to produce a 256-bit (32-byte) hash.
     * </p>
     *
     * @param input the input bytes to hash
     * @return the 256-bit SHA-256 hash as a byte array
     * @throws NoSuchAlgorithmException if the SHA-256 algorithm is not available
     */
    public static byte[] getSHA256Hash(byte[] input) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(input);
        return md.digest();
    }

    /**
     * Derives a private key from host-specific identifiers for AES encryption.
     * <p>
     * The key derivation process:
     * </p>
     * <ol>
     *   <li>Retrieves the local host's IP address</li>
     *   <li>Gets the network interface for that IP</li>
     *   <li>Extracts the hardware MAC address</li>
     *   <li>Gets the hostname as bytes</li>
     *   <li>Retrieves the BIOS serial number</li>
     *   <li>XORs these values with hard-coded salt bytes</li>
     *   <li>Hashes the result with SHA-256 to produce a 256-bit key</li>
     * </ol>
     * This ensures the key is unique to this specific machine.
     *
     * @return a 256-bit AES key derived from host identifiers
     * @throws UnknownHostException if the local host cannot be resolved
     * @throws SocketException if an I/O error occurs accessing the network interface
     * @throws NoSuchAlgorithmException if SHA-256 is not available
     */
    private static byte[] getPrivateKey() throws UnknownHostException, SocketException, NoSuchAlgorithmException {
        InetAddress ip = InetAddress.getLocalHost();
        NetworkInterface network = NetworkInterface.getByInetAddress(ip);
        byte[] mac = network.getHardwareAddress();
        byte[] hostname = ip.getHostName().getBytes();
        byte[] biosSN = getSerialNumber();
        // doing an xor on mac address and hostname
        byte[] xorResult = xor(HARD_CODED_BYTES, xor(biosSN, xor(mac, hostname)));
        return getSHA256Hash(xorResult);
    }

    /**
     * Retrieves the BIOS serial number for the current operating system.
     *
     * @return the BIOS serial number as a byte array
     */
    private static byte[] getSerialNumber() {
        String os = System.getProperty("os.name");
        return getOperatingSystemType(os).getSN();
    }

    /**
     * Determines the operating system type from the OS name string.
     * <p>
     * Detection logic:
     * </p>
     * <ul>
     *   <li>Contains "mac" or "darwin" → {@link OSType#MAC}</li>
     *   <li>Contains "win" → {@link OSType#WINDOWS}</li>
     *   <li>Contains "nix", "nux", or "aix" → {@link OSType#LINUX}</li>
     *   <li>Otherwise → {@link OSType#OTHER}</li>
     * </ul>
     *
     * @param osName the operating system name from {@code System.getProperty("os.name")}
     * @return the detected {@link OSType}
     */
    protected static OSType getOperatingSystemType(String osName) {
        OSType type = OSType.OTHER;
        if (osName != null) {
            String os = osName.toLowerCase(Locale.ENGLISH);
            if (os.contains("mac") || os.contains("darwin")) {
                type = OSType.MAC;
            } else if (os.contains("win")) {
                type = OSType.WINDOWS;
            } else if (os.contains("nix") || os.contains("nux") || os.contains("aix")) {
                type = OSType.LINUX;
            }
        }
        return type;
    }

    /**
     * Encrypts plaintext using the host-specific private key and AES-256 algorithm.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Derives the private key from host identifiers</li>
     *   <li>Creates an AES {@link SecretKeySpec} from the key</li>
     *   <li>Initializes a {@link Cipher} in ENCRYPT mode with {@link SecureRandom}</li>
     *   <li>Encrypts the plaintext bytes</li>
     *   <li>Returns the encrypted bytes as a hexadecimal string</li>
     * </ol>
     * <p>
     * <b>Important:</b> The resulting encrypted string can only be decrypted on the same host
     * where it was encrypted.
     * </p>
     *
     * @param plaintext the plaintext string to encrypt
     * @return the encrypted value as a hexadecimal string
     * @throws NoSuchAlgorithmException if AES or SHA-256 algorithm is not available
     * @throws NoSuchPaddingException if the padding scheme is not available
     * @throws InvalidKeyException if the generated key is invalid
     * @throws IllegalBlockSizeException if the plaintext length is invalid
     * @throws BadPaddingException if padding is incorrect
     * @throws UnknownHostException if the local host cannot be resolved
     * @throws SocketException if an I/O error occurs accessing the network interface
     */
    public static String encrypt(String plaintext)
            throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException,
            IllegalBlockSizeException, BadPaddingException, UnknownHostException, SocketException {
        // secret key based off of internal private key
        byte[] encryptionPrivateKey = getPrivateKey();
        Key key = new SecretKeySpec(encryptionPrivateKey, AES);
        Cipher cipher = Cipher.getInstance(AES);
        // encrypting with private key and random for initialization vector
        cipher.init(Cipher.ENCRYPT_MODE, key, new SecureRandom());
        byte[] encryptedVal = cipher.doFinal(plaintext.getBytes());
        return byteArrayToHexString(encryptedVal);
    }

    /**
     * Decrypts an encrypted value using the host-specific private key and AES-256 algorithm.
     * <p>
     * This method:
     * </p>
     * <ol>
     *   <li>Converts the hexadecimal encrypted string to bytes</li>
     *   <li>Creates an AES {@link SecretKeySpec} from the private key</li>
     *   <li>Initializes a {@link Cipher} in DECRYPT mode</li>
     *   <li>Decrypts the bytes to plaintext</li>
     * </ol>
     * <p>
     * <b>Important:</b> Decryption will fail if the value was encrypted on a different host,
     * or if the host's hardware identifiers have changed since encryption.
     * </p>
     *
     * @param encryptedText the encrypted hexadecimal string to decrypt
     * @return the decrypted plaintext string
     * @throws NoSuchAlgorithmException if the AES algorithm is not available
     * @throws NoSuchPaddingException if the padding scheme is not available
     * @throws InvalidKeyException if the private key is invalid
     */
    protected static String decrypt(String encryptedText) throws NoSuchAlgorithmException, NoSuchPaddingException, InvalidKeyException {
        byte[] encryptedTextBytes = hexStringToByteArray(encryptedText);
        SecretKeySpec secretSpec = new SecretKeySpec(privateKey, AES);
        Cipher cipher = Cipher.getInstance(AES);
        cipher.init(Cipher.DECRYPT_MODE, secretSpec, new SecureRandom());
        byte[] decryptedTextBytes = null;
        try {
            decryptedTextBytes = cipher.doFinal(encryptedTextBytes);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "decryption failed", e);
        }
        return new String(decryptedTextBytes);
    }

    /**
     * Command-line utility for encrypting passwords and testing encryption/decryption.
     * <p>
     * <b>Usage:</b>
     * </p>
     * <ul>
     *   <li><b>Encrypt mode:</b> {@code java -cp corb.jar HostKeyDecrypter encrypt myPassword}<br>
     *       Outputs the encrypted hexadecimal string</li>
     *   <li><b>Test mode:</b> {@code java -cp corb.jar HostKeyDecrypter test}<br>
     *       Encrypts and decrypts a sample password to verify functionality</li>
     *   <li><b>No arguments:</b> Displays usage information</li>
     * </ul>
     *
     * @param args command-line arguments: [method] [password]
     * @throws Exception if an error occurs during encryption/decryption
     */
    public static void main(String... args) throws Exception {
        String[] arguments = args == null ? new String[]{} : args;

        String method = arguments.length > 0 ? StringUtils.trim(arguments[0]) : "";

        if (METHOD_ENCRYPT.equals(method) && arguments.length == 2) {
            System.out.println(encrypt(arguments[1].trim())); // NOPMD
        } else if (METHOD_TEST.equals(method)) {
            HostKeyDecrypter decrypter = new HostKeyDecrypter();
            decrypter.init(System.getProperties());
            String original = "234Helloworld!!!";
            System.out.println("Password is :" + original); // NOPMD
            String password = encrypt(original);
            System.out.println("Encrypted Password is :" + password); // NOPMD
            System.out.println("Decrypted password:" + decrypter.doDecrypt("Property", password)); // NOPMD
        } else {
            System.out.println(USAGE); // NOPMD
        }
    }
}
