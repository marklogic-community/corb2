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

import java.util.Properties;

import java.io.Closeable;

/**
 * Interface for loading URIs (Universal Resource Identifiers) to be processed by CoRB.
 * <p>
 * A UrisLoader is responsible for providing a stream of URIs that will be processed
 * in batches by CoRB's processing modules. Implementations may load URIs from various sources
 * such as:
 * </p>
 * <ul>
 *   <li>XQuery or JavaScript queries executed against MarkLogic</li>
 *   <li>Files containing lists of URIs</li>
 *   <li>Collections in MarkLogic</li>
 *   <li>Other custom sources</li>
 * </ul>

 * <p>
 * The UrisLoader follows an iterator pattern with {@link #hasNext()} and {@link #next()}
 * methods to traverse the URIs. It extends {@link Closeable} to ensure proper resource
 * cleanup after use.
 * </p>
 * <p>
 * Typical usage pattern:
 * </p>
 * <pre>
 * UrisLoader loader = new SomeUrisLoaderImplementation();
 * loader.setOptions(options);
 * loader.setContentSourcePool(pool);
 * loader.setProperties(properties);
 * loader.open();
 * try {
 *     while (loader.hasNext()) {
 *         String uri = loader.next();
 *         // process uri
 *     }
 * } finally {
 *     loader.close();
 * }
 * </pre>
 *
 * @author MarkLogic Corporation
 * @since 1.0
 */
public interface UrisLoader extends Closeable {

	/**
	 * Sets the transformation options for this UrisLoader.
	 * <p>
	 * Transform options contain configuration settings that control how URIs are loaded
	 * and processed, including batch size, number of threads, and other execution parameters.
	 * </p>
	 *
	 * @param options the transform options to use, or {@code null} to clear options
	 */
	void setOptions(TransformOptions options);

	/**
	 * Sets the ContentSourcePool for accessing MarkLogic databases.
	 * <p>
	 * The ContentSourcePool manages connections to MarkLogic Server and is used
	 * by the UrisLoader to execute queries or retrieve data needed for URI loading.
	 * </p>
	 *
	 * @param csm the ContentSourcePool to use for database connections, or {@code null} to clear
	 */
	void setContentSourcePool(ContentSourcePool csm);

	/**
	 * Sets the collection name to use when loading URIs.
	 * <p>
	 * If specified, the UrisLoader may use this collection to filter or identify
	 * documents to process. The exact behavior depends on the implementation.
	 * </p>
	 *
	 * @param collection the collection name to use, or {@code null} to clear
	 */
	void setCollection(String collection);

	/**
	 * Sets additional properties for configuring the UrisLoader.
	 * <p>
	 * Properties may include implementation-specific configuration options such as
	 * query parameters, file paths, or other settings required by the loader.
	 * </p>
	 *
	 * @param properties the configuration properties to use, or {@code null} to clear
	 */
	void setProperties(Properties properties);

	/**
	 * Opens and initializes the UrisLoader, preparing it to provide URIs.
	 * <p>
	 * This method must be called after all configuration methods (setOptions, setContentSourcePool,
	 * etc.) and before calling {@link #hasNext()} or {@link #next()}. It typically performs
	 * initialization tasks such as:
     * </p>
	 * <ul>
	 *   <li>Establishing database connections</li>
	 *   <li>Executing initial queries to retrieve URIs</li>
	 *   <li>Opening files or other data sources</li>
	 *   <li>Validating configuration</li>
	 * </ul>
	 *
	 * @throws CorbException if there is an error opening or initializing the loader
	 */
	void open() throws CorbException;

	/**
	 * Returns a unique reference identifier for the current batch of URIs.
	 * <p>
	 * The batch reference can be used for logging, tracking, or correlating
	 * processing activities. The format and content of the reference is
	 * implementation-specific.
	 * </p>
	 *
	 * @return the batch reference identifier, or {@code null} if not available
	 */
	String getBatchRef();

	/**
	 * Returns the total count of URIs that will be provided by this loader.
	 * <p>
	 * This count may be an estimate or exact value depending on the implementation.
	 * If the total count cannot be determined in advance, implementations may return 0
	 * or an approximate value.
	 * </p>
	 *
	 * @return the total number of URIs to be loaded, or 0 if unknown
	 */
	long getTotalCount();

	/**
	 * Checks whether there are more URIs available to load.
	 * <p>
	 * This method should be called before each call to {@link #next()} to determine
	 * if additional URIs are available for processing.
	 * </p>
	 *
	 * @return {@code true} if there are more URIs available, {@code false} otherwise
	 * @throws CorbException if there is an error checking for the next URI
	 */
	boolean hasNext() throws CorbException;

	/**
	 * Retrieves the next URI from the loader.
	 * <p>
	 * This method should only be called after {@link #hasNext()} returns {@code true}.
	 * The behavior when calling this method without checking hasNext() first is
	 * implementation-specific.
	 * </p>
	 *
	 * @return the next URI as a String
	 * @throws CorbException if there is an error retrieving the next URI or if no more URIs are available
	 */
	String next() throws CorbException;

}
