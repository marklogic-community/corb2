/*
 * Copyright (c) 2004-2022 MarkLogic Corporation
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

import org.xml.sax.SAXParseException;

import javax.xml.stream.XMLStreamException;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.List;

/**
 * Validate items in a ResultSequence and write any validation errors to an individual file per item
 */
public class SchemaValidateToFileTask extends SchemaValidateBatchToFileTask {

    @Override
    protected String getFileName() {
        //this extends SchemaValidateBatchToFileTask, which produces the Batch filename instead of individual files per URI,
        //so, need to change to a single Export file per document
        return getExportFileName();
    }

    @Override
    protected File getExportFile() {
        // this extends SchemaValidateBatchToFileTask, which extends ExportBatchToFileTask and would get a File that could
        // have the EXPORT_FILE_PART extension. We just want the regular filename.
        return getExportFile(getFileName());
    }

    @Override
    protected void writeSchemaValidationReport(List<SAXParseException> exceptions, File outputFile) throws IOException, XMLStreamException {
        //Since these validation reports are per doc, the FileWriter will not append, and no need for synchnoized writes
        try (Writer writer = Files.newBufferedWriter(outputFile.toPath())) {
            writeSchemaValidationReport(exceptions, writer);
        }
    }
}
