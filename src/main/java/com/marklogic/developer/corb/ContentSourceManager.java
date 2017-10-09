package com.marklogic.developer.corb;

import java.util.Properties;

import com.marklogic.xcc.ContentSource;

public interface ContentSourceManager {
    /**
     * Initializes the connection manager. This method should only be called by AbstractManager class during initialization of corb.
     * @param properties
     * @param sslConfig
     * @param connectionStrings 
     */
    void init(Properties properties, SSLConfig sslConfig, String[] connectionStrings);
    
    /**
     * Returns the next content source from the list of available connections.  
     * DefaultConnectionManager uses either round-robin (default) or random policy to determine next content source. 
     * DefaultConnectionManager implementation waits for #Options.XCC_CONNECTION_RETRY_INTERVAL if the next available connection has failed earlier.
     * @return contentSource
     * @throws NullPointerException when content source is not available
     */
    ContentSource get();
    
    /**
     * Removes contentSource from the list of available content sources. 
     * @param contentSource
     */
    void remove(ContentSource contentSource);
        
    /**
     * Checks if there is at least one content source available
     * @return true if available
     */
    boolean available();
    
    /**
     * Returns all the content sources managed by this manager pool
     */
    ContentSource[] getAllContentSources();   
}
