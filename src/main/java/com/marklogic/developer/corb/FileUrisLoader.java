/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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

import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import static java.util.logging.Level.SEVERE;
import java.util.logging.Logger;

public class FileUrisLoader extends AbstractUrisLoader {

    protected BufferedReader br;
    protected String nextLine;
    protected static final Logger LOG = Logger.getLogger(FileUrisLoader.class.getName());
    private static final String EXCEPTION_MSG_PROBLEM_READING_URIS_FILE = "Problem while reading the uris file";

    @Override
    public void open() throws CorbException {

        parseUriReplacePatterns();

        String fileName = getOptions().getUrisFile();

        try (LineNumberReader lnr = new LineNumberReader(new FileReader(fileName))) {
            lnr.skip(Long.MAX_VALUE);
            this.setTotalCount(lnr.getLineNumber() + 1);

            FileReader fr = new FileReader(fileName);
            br = new BufferedReader(fr);
            
        } catch (Exception exc) {
            throw new CorbException("Problem loading data from uris file " + getOptions().getUrisFile(), exc);
        }
    }

    private String readNextLine() throws IOException {
        String line = trim(br.readLine());
        if (line != null && isBlank(line)) {
            line = readNextLine();
        }
        return line;
    }

    @Override
    public boolean hasNext() throws CorbException {
        if (nextLine == null) {
            try {
                nextLine = readNextLine();
            } catch (Exception exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_URIS_FILE, exc);
            }
        }
        return nextLine != null;
    }

    @Override
    public String next() throws CorbException {
        String line;
        if (nextLine != null) {
            line = nextLine;
            nextLine = null;
        } else {
            try {
                line = readNextLine();
            } catch (Exception exc) {
                throw new CorbException(EXCEPTION_MSG_PROBLEM_READING_URIS_FILE, exc);
            }
        }
        for (int i = 0; line != null && i < replacements.length - 1; i += 2) {
            line = line.replaceAll(replacements[i], replacements[i + 1]);
        }
        return line;
    }

    @Override
    public void close() {
        if (br != null) {
            LOG.info("closing uris file reader");
            try {
                br.close();
                br = null;
            } catch (Exception exc) {
                LOG.log(SEVERE, "while closing uris file reader", exc);
            }
        }
        cleanup();
    }

    @Override
    protected void cleanup() {
        super.cleanup();
        nextLine = null;
        br = null;
    }
}
