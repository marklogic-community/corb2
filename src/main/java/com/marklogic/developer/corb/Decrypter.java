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

import java.io.IOException;
import java.util.Properties;

/**
 * Interface for decrypting sensitive configuration values in CoRB.
 * Implementations of this interface provide custom decryption strategies for
 * encrypted passwords, connection strings, API keys, and other sensitive configuration
 * values that should not be stored in plain text.
 *
 * <p>The decryption process supports two formats:</p>
 * <ul>
 *   <li><strong>Wrapped format:</strong> {@code ENC(encrypted_value)} - The encrypted value is wrapped in "ENC(...)"</li>
 *   <li><strong>Unwrapped format:</strong> {@code encrypted_value} - The value is already in encrypted form</li>
 * </ul>
 *
 * <p>Common use cases:</p>
 * <ul>
 *   <li>Decrypting database passwords in XCC connection URIs</li>
 *   <li>Decrypting SSL keystore and truststore passwords</li>
 *   <li>Decrypting API keys and OAuth tokens</li>
 *   <li>Decrypting any custom property values marked as sensitive</li>
 * </ul>
 *
 * <p>To use a custom decrypter, specify the implementation class using the
 * {@link Options#DECRYPTER} option. The decrypter is initialized once during
 * CoRB startup and reused throughout the job.</p>
 *
 * @see AbstractDecrypter
 * @see PrivateKeyDecrypter
 * @see JasyptDecrypter
  * @since 2.1.3
 */
public interface Decrypter {
	/**
	 * Initializes the decrypter with configuration properties.
	 * This method is called once during CoRB initialization before any decryption operations.
	 * Implementations should load encryption keys, initialize cipher algorithms, and perform
	 * any necessary setup using the provided properties.
	 *
	 * @param properties configuration properties that may contain decryption keys,
	 *                   algorithm settings, or other initialization parameters
	 * @throws IOException if an I/O error occurs during initialization (e.g., reading key files)
	 * @throws ClassNotFoundException if a required class cannot be found during initialization
	 */
	void init(Properties properties) throws IOException, ClassNotFoundException;
	/**
	 * Decrypts the given encrypted value.
	 * The property name parameter can be used by implementations to apply different
	 * decryption strategies based on the property being decrypted, or for logging purposes.
	 *
	 * <p>Implementations should handle both wrapped format {@code ENC(encrypted_value)}
	 * and unwrapped format gracefully. The {@link AbstractDecrypter} base class provides
	 * this handling automatically.</p>
	 *
	 * @param property the name of the property being decrypted (e.g., "XCC-PASSWORD"),
	 *                 may be used for context-specific decryption or logging
	 * @param value the encrypted value to decrypt (may include "ENC(...)" wrapper)
	 * @return the decrypted plain text value
	 * @throws RuntimeException if decryption fails (implementation-specific)
	 */
	String decrypt(String property, String value);
}
