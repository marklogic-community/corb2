/*
 * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
import org.xml.sax.SAXParseException;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class XmlUtilsTest {
    private static final String dir = "src/test/resources/streamingXMLUrisLoader/";
    private static final File xmlDoc = new File(dir , "EDI.ICF15T.D150217.T113100716.T");
    private static final File schema = new File(dir + "BenefitEnrollment.xsd");
    private static final Properties options = new Properties();

    @Test
    void schemaValidate() {
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertTrue(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void schemaValidateWithError()  {
        File schema = new File(dir, "NotBenefitEnrollment.xsd");
        try {
            List<SAXParseException> exceptionList = XmlUtils.schemaValidate(xmlDoc, schema, options);
            assertFalse(exceptionList.isEmpty());
        } catch (CorbException ex) {
            fail();
        }
    }

    @Test
    void schemaValidateMissingFile() {
        File missingFile = new File("does-not-exist.xml");
        assertThrows(CorbException.class, () -> XmlUtils.schemaValidate(missingFile, schema, options));
    }

}
