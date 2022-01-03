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
package com.marklogic.developer.corb.util;

import com.marklogic.developer.corb.CorbException;
import org.junit.Test;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

public class XmlUtilsTest {
    private String dir = "src/test/resources/streamingXMLUrisLoader/";
    private File xmlDoc = new File(dir , "EDI.ICF15T.D150217.T113100716.T");
    private File schema = new File(dir + "BenefitEnrollment.xsd");
    private Properties options = new Properties();
    @Test
    public void schemaValidate() {
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertTrue(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    public void schemaValidateWithError()  {
        File schema = new File(dir, "NotBenefitEnrollment.xsd");
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertFalse(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test (expected = CorbException.class)
    public void schemaValidateMissingFile() throws CorbException {
        File missingFile = new File("does-not-exist.xml");
        XmlUtils.schemaValidate(missingFile, schema, options);
    }

}
