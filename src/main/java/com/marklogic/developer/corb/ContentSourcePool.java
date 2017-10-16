package com.marklogic.developer.corb;

import java.io.Closeable;
import java.util.Properties;

import com.marklogic.xcc.ContentSource;

public interface ContentSourcePool extends Closeable{
    /**
     * Initializes the ContentSourcePool. This method should only be called by AbstractManager class during initialization of corb.
     * @param properties
     * @param sslConfig
     * @param connectionStrings 
     */
    void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings);
    
    /**
     * Returns SSLConfig used by the content source manager
     * @return sslConfig
     */
    SSLConfig getSSLConfig();
    
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
