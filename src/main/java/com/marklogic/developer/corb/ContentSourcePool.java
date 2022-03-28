/*
 * * Copyright (c) 2004-2022 MarkLogic Corporation
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 * *
 * * The use of the Apache License does not indicate that this project is
 * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import java.io.Closeable;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;
/**
 * @since 2.4.0
 */
public interface ContentSourcePool extends Closeable{
    /**
     * Initializes the ContentSourcePool. This method should only be called by AbstractManager class during initialization of corb.
     * @param properties
     * @param sslConfig
     * @param connectionStrings
     */
    void init(Properties properties, SSLConfig sslConfig, String... connectionStrings);

    /**
     * Returns SSLConfig used by the content source manager
     * @return sslConfig
     */
    SSLConfig sslConfig();

    /**
     * Returns the next ContentSource from the list of available pool.
     * DefaultConnectionManager uses either round-robin (default) or random policy to determine next content source.
     * DefaultConnectionManager implementation waits for #Options.XCC_CONNECTION_RETRY_INTERVAL if the next available connection has failed earlier.
     * @return ContentSource
     * @throws CorbException when content source is not available
     */
    ContentSource get() throws CorbException;

    /**
     * Removes contentSource from the list of available ContentSource instances.
     * @param contentSource
     */
    void remove(ContentSource contentSource);

    /**
     * Checks if there is at least one ContentSource available
     * @return true if available
     */
    boolean available();

    /**
     * Returns all the content sources managed by this ContentSourcePool
     */
    ContentSource[] getAllContentSources();
}
