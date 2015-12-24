/*
 * Copyright 2005-2015 MarkLogic Corporation
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

import static com.marklogic.developer.corb.Options.URIS_REPLACE_PATTERN;
import static com.marklogic.developer.corb.util.IOUtils.closeQuietly;
import static com.marklogic.developer.corb.util.StringUtils.isBlank;
import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ContentSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileUrisLoader implements UrisLoader {

	TransformOptions options;
	ContentSource cs;
	String collection;
	Properties properties;

	BufferedReader br = null;
	int total = 0;

	String[] replacements = new String[0];

	protected static final Logger LOG = Logger.getLogger(FileUrisLoader.class.getSimpleName());

	@Override
	public void setOptions(TransformOptions options) {
		this.options = options;
	}

	@Override
	public void setContentSource(ContentSource cs) {
		this.cs = cs;
	}

	@Override
	public void setCollection(String collection) {
		this.collection = collection;
	}

	@Override
	public void setProperties(Properties properties) {
		this.properties = properties;
	}

	@Override
	public void open() throws CorbException {
        
		if (properties != null && properties.containsKey(URIS_REPLACE_PATTERN)) {
			String pattern = properties.getProperty(URIS_REPLACE_PATTERN).trim();
			replacements = pattern.split(",", -1);
			if (replacements.length % 2 != 0) {
				throw new IllegalArgumentException("Invalid replacement pattern " + pattern);
			}
		}
		
		try {
			String fileName = options.getUrisFile();
			LineNumberReader lnr = null;
			try {
				lnr = new LineNumberReader(new FileReader(fileName));
				lnr.skip(Long.MAX_VALUE);
				total = lnr.getLineNumber() + 1;
			} finally {
                closeQuietly(lnr);
			}

			FileReader fr = new FileReader(fileName);
			br = new BufferedReader(fr);

		} catch (Exception exc) {
			throw new CorbException("Problem loading data from uris file " + options.getUrisFile(), exc);
		}
	}

	@Override
	public String getBatchRef() {
		return null;
	}

	@Override
	public int getTotalCount() {
		return this.total;
	}

	private String readNextLine() throws IOException {
		String line = trim(br.readLine());
		if (line != null && isBlank(line)) {
			line = readNextLine();
		}
		return line;
	}

	String nextLine = null;

	@Override
	public boolean hasNext() throws CorbException {
		if (nextLine == null) {
			try {
				nextLine = readNextLine();
			} catch (Exception exc) {
				throw new CorbException("Problem while reading the uris file");
			}
		}
		return nextLine != null;
	}

	@Override
	public String next() throws CorbException {
		String line = null;
		if (nextLine != null) {
			line = nextLine;
			nextLine = null;
		} else {
			try {
				line = readNextLine();
			} catch (Exception exc) {
				throw new CorbException("Problem while reading the uris file");
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
				LOG.log(Level.SEVERE, "while closing uris file reader", exc);
			}
		}
		cleanup();
	}

	protected void cleanup() {
		// release
		br = null;
		options = null;
		cs = null;
		collection = null;
		properties = null;
		replacements = null; 
	}
}
