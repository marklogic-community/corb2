package com.marklogic.developer.corb;

import java.util.Properties;

import com.marklogic.xcc.ContentSource;

public interface UrisLoader {

	public void setOptions(TransformOptions options);

	public void setContentSource(ContentSource cs);

	public void setCollection(String collection);

	public void setProperties(Properties properties);

	public void open() throws CorbException;

	public String getBatchRef();

	public int getTotalCount();

	public boolean hasNext() throws CorbException;

	public String next() throws CorbException;

	public void close();
}
