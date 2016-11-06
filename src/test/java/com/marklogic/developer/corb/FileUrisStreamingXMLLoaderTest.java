/*
  * * Copyright (c) 2004-2016 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.XML_FILE;
import static com.marklogic.developer.corb.Options.XML_SCHEMA;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class FileUrisStreamingXMLLoaderTest {
    
    public static final String BUU_DIR = "src/test/resources/streamingXMLUrisLoader/";
    public static final String BUU_FILENAME = "EDI.ICF15T.D150217.T113100716.T";
    public static final String BUU_SCHEMA = "BenefitEnrollment.xsd";
    public static final String NOT_BUU_SCHEMA = "Not" + BUU_SCHEMA;
    private static final Logger LOG = Logger.getLogger(FileUrisStreamingXMLLoaderTest.class.getName());

    public FileUrisStreamingXMLLoaderTest() {
    }

    @Test
    public void testOpenWithDefaultXPath() {
        testOpen(null, 6);
    }

    @Test
    public void testOpenWithDescendantXpath() {
        testOpen("//FileInformation", 1);
    }

    @Test
    public void testOpenSingleFileWithElementNameOnly() {
        testOpen("FileInformation", 1);
    }

    @Test
    public void testOpenWithElementWildcard() {
        testOpen("/*/FileInformation", 1);
    }

    @Test
    public void testOpenWithMultipleWildcardSteps() {
        testOpen("/*/*/*/MemberInformation", 15);
    }

    @Test
    public void testOpenWithNamespace() {
        testOpen("/*/buu:FileInformation", 1);
    }
    
    @Test
    public void testOpenWithUnknownNamespace() {
        testOpen("/*/xyz:FileInformation", 1);
    }

    @Test(expected = CorbException.class)
    public void testOpenWithSchemaInvalidContent() throws CorbException {
        FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();
        loader.properties.setProperty(XML_SCHEMA, BUU_DIR + NOT_BUU_SCHEMA);
        loader.open();
    }

    public void testOpen(String XPath, int expectedItems) {
        FileUrisStreamingXMLLoader loader = getDefaultLargeFileUrisXMLLoader();
        if (XPath != null) {
            loader.properties.setProperty(Options.XML_NODE, XPath);
        }
        try {
            loader.open();
            assertEquals(expectedItems, loader.getTotalCount());
            assertTrue(loader.hasNext());
            for (int i = 0; i < expectedItems; i++) {
                assertNotNull(loader.next());
            }
        } catch (CorbException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    public FileUrisStreamingXMLLoader getDefaultLargeFileUrisXMLLoader() {
        FileUrisStreamingXMLLoader lfuxl = new FileUrisStreamingXMLLoader();
        lfuxl.properties = new Properties();
        lfuxl.properties.setProperty(XML_FILE, BUU_DIR + BUU_FILENAME);
        lfuxl.properties.setProperty(XML_SCHEMA, BUU_DIR + BUU_SCHEMA);
        return lfuxl;
    }
}
