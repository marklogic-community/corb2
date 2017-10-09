package com.marklogic.developer.corb;

public class DefaultContentSourceManagerTest {
	/*
    @Test(expected = NullPointerException.class)
    public void testPrepareContentSourceNull() {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.prepareContentSource();
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testPrepareContentSourceSecureXCC() {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.connectionUri = new URI("xccs://user:pass@localhost:8001");
            instance.sslConfig = mock(SSLConfig.class);
            instance.prepareContentSource();
        } catch (CorbException | URISyntaxException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }

        assertNotNull(instance.contentSource);
    }

    @Test(expected = CorbException.class)
    public void testPrepareContentSourceNoScheme() throws CorbException {
        AbstractManager instance = new AbstractManagerImpl();
        try {
            instance.connectionUri = new URI("//user:pass@localhost:8001");
            instance.sslConfig = mock(SSLConfig.class);
            instance.prepareContentSource();
        } catch (URISyntaxException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }
*/
/*    
    @Test
    public void testGetSecurityOptions() {
        try {
            AbstractManager instance = new AbstractManagerImpl();
            TrustAnyoneSSLConfig sslConfig = new TrustAnyoneSSLConfig();
            instance.sslConfig = new TrustAnyoneSSLConfig();
            SecurityOptions result = instance.getSecurityOptions();
            SecurityOptions securityOptions = instance.getSecurityOptions();

            assertNotNull(securityOptions);
            assertArrayEquals(sslConfig.getSecurityOptions().getEnabledProtocols(), result.getEnabledProtocols());
        } catch (NoSuchAlgorithmException | KeyManagementException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = NullPointerException.class)
    public void testGetSecurityOptionsNullPointer() {
        try {
            AbstractManager instance = new AbstractManagerImpl();
            instance.getSecurityOptions();
        } catch (KeyManagementException | NoSuchAlgorithmException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }
*/
}
