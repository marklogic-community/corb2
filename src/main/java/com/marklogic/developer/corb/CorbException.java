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

/**
 * Exception class for CoRB (Content Reprocessing in Bulk) operations.
 * This is the primary exception type thrown by CoRB components when errors occur
 * during job initialization, URI loading, task processing, or connection management.
 *
 * <p>CorbException is a checked exception that extends {@link Exception}, requiring
 * explicit handling or declaration in method signatures. It can wrap underlying causes
 * to preserve the full exception chain for debugging.</p>
 *
 * <p>Common scenarios where CorbException is thrown:</p>
 * <ul>
 *   <li>Connection failures to MarkLogic server</li>
 *   <li>Configuration errors (invalid options, missing properties)</li>
 *   <li>Module loading failures (adhoc query or installed module not found)</li>
 *   <li>Task execution errors (when failOnError is true)</li>
 *   <li>URI loader initialization or processing failures</li>
 *   <li>Decryption failures for encrypted configuration values</li>
 * </ul>
 */
public class CorbException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new CorbException with the specified detail message.
	 *
	 * @param msg the detail message describing the error condition
	 */
	public CorbException(String msg) {
		super(msg);
	}

	/**
	 * Constructs a new CorbException with the specified detail message and cause.
	 * This constructor is useful for wrapping lower-level exceptions while adding
	 * context-specific information.
	 *
	 * @param msg the detail message describing the error condition
	 * @param th the underlying cause of this exception (may be null)
	 */
	public CorbException(String msg, Throwable th) {
		super(msg, th);
	}
}
