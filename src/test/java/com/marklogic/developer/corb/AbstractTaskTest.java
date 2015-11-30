/*
 * * Copyright 2005-2015 MarkLogic Corporation
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

import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ModuleInvoke;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.ServerConnectionException;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmItem;
import java.util.Properties;
import javax.sound.midi.Sequence;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.mockito.exceptions.base.MockitoException;


/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class AbstractTaskTest {
    
    public AbstractTaskTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
        clearSystemProperties();
    }
    
    @After
    public void tearDown() {
        clearSystemProperties();
    }

    /**
     * Test of setContentSource method, of class AbstractTask.
     */
    @Test
    public void testSetContentSource() {
        System.out.println("setContentSource");
        ContentSource cs = mock(ContentSource.class);
        AbstractTask instance = new AbstractTaskImpl();
        instance.setContentSource(cs);
        assertEquals(cs, instance.cs);
    }

    /**
     * Test of setModuleType method, of class AbstractTask.
     */
    @Test
    public void testSetModuleType() {
        System.out.println("setModuleType");
        String moduleType = "foo";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleType(moduleType);
        assertEquals(moduleType, instance.moduleType);
    }

    /**
     * Test of setModuleURI method, of class AbstractTask.
     */
    @Test
    public void testSetModuleURI() {
        System.out.println("setModuleURI");
        String moduleUri = "test.xqy";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setModuleURI(moduleUri);
        assertEquals(moduleUri, instance.moduleUri);
    }

    /**
     * Test of setAdhocQuery method, of class AbstractTask.
     */
    @Test
    public void testSetAdhocQuery() {
        System.out.println("setAdhocQuery");
        String adhocQuery = "adhoc.xqy";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setAdhocQuery(adhocQuery);
        assertEquals(adhocQuery, instance.adhocQuery);    
    }

    /**
     * Test of setQueryLanguage method, of class AbstractTask.
     */
    @Test
    public void testSetQueryLanguage() {
        System.out.println("setQueryLanguage");
        String language = "XQuery";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setQueryLanguage(language);
        assertEquals(language, instance.language);
    }

    /**
     * Test of setProperties method, of class AbstractTask.
     */
    @Test
    public void testSetProperties() {
        System.out.println("setProperties");
        Properties props = new Properties();
        Properties properties = props;
        AbstractTask instance = new AbstractTaskImpl();
        instance.setProperties(properties);
        assertEquals(properties, instance.properties);
    }

    /**
     * Test of setInputURI method, of class AbstractTask.
     */
    @Test
    public void testSetInputURI() {
        System.out.println("setInputURI");
        String[] inputUri = {"foo", "bar", "baz"};
        AbstractTask instance = new AbstractTaskImpl();
        instance.setInputURI(inputUri);
        Assert.assertArrayEquals(inputUri, instance.inputUris);
    }

    /**
     * Test of setFailOnError method, of class AbstractTask.
     */
    @Test
    public void testSetFailOnError() {
        System.out.println("setFailOnError");
        AbstractTask instance = new AbstractTaskImpl();
        instance.setFailOnError(false);
        assertFalse(instance.failOnError);
        instance.setFailOnError(true);
        assertTrue(instance.failOnError);
    }

    /**
     * Test of setExportDir method, of class AbstractTask.
     */
    @Test
    public void testSetExportDir() {
        System.out.println("setExportDir");
        String exportFileDir = "/tmp";
        AbstractTask instance = new AbstractTaskImpl();
        instance.setExportDir(exportFileDir);
        assertEquals(exportFileDir, instance.exportDir);
    }

    /**
     * Test of getExportDir method, of class AbstractTask.
     */
    @Test
    public void testGetExportDir() {
        System.out.println("getExportDir");
        AbstractTask instance = new AbstractTaskImpl();
        String expResult = "/tmp";
        instance.exportDir = expResult;
        String result = instance.getExportDir();
        assertEquals(expResult, result);
    }

    /**
     * Test of newSession method, of class AbstractTask.
     */
    @Test
    public void testNewSession() {
        System.out.println("newSession");
        AbstractTask instance = new AbstractTaskImpl();
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        when(cs.newSession()).thenReturn(session);
        Session expResult = session;
        instance.cs = cs;
        Session result = instance.newSession();
        assertEquals(expResult, result);
    }

    /**
     * Test of invokeModule method, of class AbstractTask.
     */
    @Test 
    public void testInvokeModule() throws Exception {
        System.out.println("invokeModule");
        AbstractTask instance = new AbstractTaskImpl();
        instance.moduleUri = "module.xqy";
        instance.adhocQuery = "adhoc.xqy";
        instance.inputUris = new String[] {"foo", "bar", "baz"};
        ContentSource cs = mock(ContentSource.class);
        Session session = mock(Session.class);
        ModuleInvoke request = mock(ModuleInvoke.class);
        ResultSequence seq = mock(ResultSequence.class);
        
        when(cs.newSession()).thenReturn(session);
        when(session.newModuleInvoke(anyString())).thenReturn(request);
        when(request.setNewStringVariable(anyString(),anyString())).thenReturn(null);
        when(session.submitRequest(request)).thenReturn(seq);
        
        instance.cs = cs;
        String[] result = instance.invokeModule();
    }

    /**
     * Test of asString method, of class AbstractTask.
     */
    @Test
    public void testAsString() {
        System.out.println("asString");
        String[] uris = new String[] {"foo", "bar", "baz"};
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("foo,bar,baz", result);
    }

    @Test
    public void testAsString_emptyArray() {
        System.out.println("asString");
        String[] uris = new String[] {};
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("", result);
    }
    
    @Test
    public void testAsString_null() {
        System.out.println("asString");
        String[] uris = null;
        AbstractTask instance = new AbstractTaskImpl();
        String result = instance.asString(uris);
        assertEquals("", result);
    }

    /**
     * Test of cleanup method, of class AbstractTask.
     */
    @Test
    public void testCleanup() {
        System.out.println("cleanup");
        AbstractTask instance = new AbstractTaskImpl();
        instance.cs = mock(ContentSource.class);
        instance.moduleType = "moduleType";
        instance.moduleUri = "moduleUri";
        instance.properties = new Properties();
        instance.inputUris = new String[] {};
        instance.adhocQuery = "adhocQuery";
        instance.cleanup();
        assertNull(instance.cs);
        assertNull(instance.moduleType);
        assertNull(instance.moduleUri);
        assertNull(instance.properties);
        assertNull(instance.inputUris);
        assertNull(instance.adhocQuery);
    }

    /**
     * Test of getProperty method, of class AbstractTask.
     */
    @Test
    public void testGetProperty() {
        System.out.println("getProperty");
        String key = "INIT-TASK";
        String val = "foo";
        Properties props = new Properties();
        props.setProperty(key, val);
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
        assertEquals(val, result);
    }
    
    @Test
    public void testGetProperty_systemPropertyTakesPrecedence() {
        System.out.println("getProperty");
        String key = "INIT-TASK";
        String val = "foo";
        System.setProperty(key, val);
        Properties props = new Properties();
        props.setProperty(key, "bar");
        AbstractTask instance = new AbstractTaskImpl();
        instance.properties = props;
        String result = instance.getProperty(key);
        assertEquals(val, result);
        clearSystemProperties();
    }
    /**
     * Test of getValueAsBytes method, of class AbstractTask.
     */
    @Test
    public void testGetValueAsBytes_xdmBinary() {
        System.out.println("getValueAsBytes");
        XdmItem item = mock(XdmBinary.class);
   
        AbstractTask instance = new AbstractTaskImpl();
        byte[] result = instance.getValueAsBytes(item);
        assertNull(result);
    }

        @Test
    public void testGetValueAsBytes_xdmItem() {
        System.out.println("getValueAsBytes");
        XdmItem item = mock(XdmItem.class);
        String value = "foo";
        when(item.asString()).thenReturn(value);
        AbstractTask instance = new AbstractTaskImpl();
        byte[] result = instance.getValueAsBytes(item);
        Assert.assertArrayEquals(value.getBytes(), result);
    }
    
    public class AbstractTaskImpl extends AbstractTask {

        @Override
        public String processResult(ResultSequence seq) throws CorbException {
            return "";
        }

        @Override
        public String[] call() throws Exception {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }
        
    }
    
}
