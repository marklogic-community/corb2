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

import org.junit.Test;
import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLStreamException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaValidateToFileTaskTest {

    @Test
    public void testGetFileName() {

        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "/tmp/bar.xml");

        SchemaValidateToFileTask validate = new SchemaValidateToFileTask();
        validate.setProperties(properties);
        validate.inputUris = new String[]{"/tmp/foo.xml"};
        String fileName = validate.getFileName();
        assertEquals("tmp/foo.xml", fileName);
    }

    @Test
    public void testGetExportFileDoesNotUseExportFilePartExtension() {
        String exportFilePartExtension = ".zzz";
        Properties properties = new Properties();
        properties.setProperty(Options.EXPORT_FILE_NAME, "/tmp/bar.xml");
        properties.setProperty(Options.EXPORT_FILE_PART_EXT, exportFilePartExtension);

        SchemaValidateToFileTask validate = new SchemaValidateToFileTask();
        validate.setProperties(properties);
        validate.inputUris = new String[]{"/tmp/foo.xml"};

        File exportFile = validate.getExportFile();
        assertFalse(exportFile.getName().endsWith(exportFilePartExtension));
        assertEquals("foo.xml", exportFile.getName());
    }

    @Test (expected = IOException.class)
    public void testWriteSchemaValidationReportException() throws IOException, XMLStreamException {
        List<SAXParseException> exceptions = new ArrayList<>();
        File outputFile = mock(File.class);
        when(outputFile.getPath()).thenThrow(IOException.class);
        SchemaValidateToFileTask validate = new SchemaValidateToFileTask();
        validate.writeSchemaValidationReport(exceptions, outputFile);
    }

}
