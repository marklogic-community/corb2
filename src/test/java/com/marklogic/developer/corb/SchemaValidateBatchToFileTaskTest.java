/*
 * Copyright (c) 2004-2018 MarkLogic Corporation
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

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;
import org.junit.Test;
import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaValidateBatchToFileTaskTest {

    @Test
    public void processResultValid() {
        try {
            String xmlFile = "src/test/resources/streamingXMLUrisLoader/EDI.ICF15T.D150217.T113100716.T";
            String schemaFile = "src/test/resources/streamingXMLUrisLoader/BenefitEnrollment.xsd";
            File outputFile = File.createTempFile("rpt", "xml");
            outputFile.deleteOnExit();

            SchemaValidateBatchToFileTask validateTask = createSchemaValidateTask(schemaFile, outputFile);
            validateTask.processResult(singleFileSequence(xmlFile));

            assertEquals(0, FileUtils.getLineCount(outputFile));

        } catch (IOException | CorbException ex) {
            fail();
        }
    }

    @Test
    public void processResultInvalid() {
        try {
            String xmlFile = "src/test/resources/streamingXMLUrisLoader/EDI.ICF15T.D150217.T113100716.T";
            String schemaFile = "src/test/resources/streamingXMLUrisLoader/NotBenefitEnrollment.xsd";
            File outputFile = File.createTempFile("rpt", "xml");
            outputFile.deleteOnExit();

            SchemaValidateBatchToFileTask validateTask = createSchemaValidateTask(schemaFile, outputFile);
            validateTask.processResult(singleFileSequence(xmlFile));

            assertNotNull(TestUtils.readFile(outputFile));

        } catch (IOException | CorbException ex) {
            fail();
        }
    }

    @Test (expected = CorbException.class)
    public void processResultWithException() throws CorbException {

        ResultSequence sequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = null;
        when(sequence.hasNext()).thenReturn(true);
        when(sequence.next()).thenReturn(resultItem);
        when(resultItem.getItem()).thenReturn(xdmItem);

        String schemaFile = "src/test/resources/streamingXMLUrisLoader/NotBenefitEnrollment.xsd";
        File outputFile = mock(File.class);
        when(outputFile.getAbsolutePath()).thenReturn("/tmp/foo.xml");
        SchemaValidateBatchToFileTask validate = createSchemaValidateTask(schemaFile, outputFile);

        validate.processResult(sequence);
    }

    @Test
    public void getSchemaFile() {
        Properties properties = new Properties();
        properties.setProperty(Options.XML_SCHEMA, "src/test/resources/streamingXMLUrisLoader/BenefitEnrollment.xsd");
        SchemaValidateBatchToFileTask validate = new SchemaValidateBatchToFileTask();
        validate.setProperties(properties);
        File schema = validate.getSchemaFile();
        assertTrue(schema.exists());
    }

    @Test(expected = NullPointerException.class)
    public void getSchemaFileMissing() {
        SchemaValidateBatchToFileTask validate = new SchemaValidateBatchToFileTask();
        validate.getSchemaFile();
    }

    @Test
    public void writeSchemaValidationReportWithNoExceptions() {
        StringWriter writer = new StringWriter();
        SchemaValidateBatchToFileTask validate = new SchemaValidateBatchToFileTask();
        validate.inputUris = new String[]{"foo"};
        List<SAXParseException> exceptions = new ArrayList<>();
        try {
            validate.writeSchemaValidationReport(exceptions, writer);
            assertTrue(writer.toString().isEmpty());
        } catch (XMLStreamException ex) {
            fail();
        }
    }

    @Test
    public void writeParseException() {
        Writer stringWriter = new StringWriter();
        XMLOutputFactory output = XMLOutputFactory.newInstance();
        SchemaValidateBatchToFileTask validate = new SchemaValidateBatchToFileTask();
        try {
            XMLStreamWriter xmlWriter = output.createXMLStreamWriter(stringWriter);
            validate.writeParseException(new SAXParseException("Invalid content", "public-id", "system-id", 11, 4), xmlWriter);
            assertEquals("<error publicId=\"public-id\" systemId=\"system-id\" lineNumber=\"11\" columnNumber=\"4\">Invalid content</error>",
                stringWriter.toString());
        } catch (XMLStreamException ex) {
            fail();
        }
    }

    public static ResultSequence singleFileSequence(String testFile) throws FileNotFoundException {
        File file = new File(testFile);
        return singleFileSequence(file);
    }

    public static ResultSequence singleFileSequence(File testFile) throws FileNotFoundException {
        ResultSequence sequence = mock(ResultSequence.class);
        ResultItem resultItem = mock(ResultItem.class);
        XdmItem xdmItem = mock(XdmItem.class);

        when(sequence.hasNext()).thenReturn(true).thenReturn(false);
        when(resultItem.getItem()).thenReturn(xdmItem);
        when(xdmItem.asString()).thenReturn(TestUtils.readFile(testFile));
        when(xdmItem.asInputStream()).thenReturn(new FileInputStream(testFile));
        when(sequence.next()).thenReturn(resultItem);

        return sequence;
    }

    public static SchemaValidateBatchToFileTask createSchemaValidateTask(String schemaFileName, File outputFile ) {

        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, outputFile.getAbsolutePath());
        properties.setProperty(Options.XML_SCHEMA, schemaFileName);

        SchemaValidateBatchToFileTask validate = new SchemaValidateBatchToFileTask();
        validate.inputUris = new String[]{"foo"};
        validate.setProperties(properties);

        return validate;
    }
}
