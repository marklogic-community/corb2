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

/**
 *
 * @author Praveen Venkata
 */

import static com.marklogic.developer.corb.util.StringUtils.trim;
import com.marklogic.xcc.ContentSource;
import java.util.Properties;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.marklogic.developer.corb.util.StringUtils.isBlank;

public class FileUrisXMLLoader implements UrisLoader {

	protected static final Logger LOG = Logger.getLogger(FileUrisXMLLoader.class.getName());
	TransformOptions options;
	ContentSource cs;
	String collection;
	Properties properties;
	int total = 0;
	String batchRef = null;
	String nextNode = null;
	Iterator<Node> nodeIterator = null;
	Document doc = null;
	Map<Integer,Node> nodeMap = null;

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
	public String getBatchRef() {
		return batchRef;
	}

	@Override
	public int getTotalCount() {
		return this.total;
	}

	@Override
	public void open() throws CorbException {

		try {
			String fileName = getProperty("XML-FILE");
			String xpathRootNode = getProperty("XML-ROOT-NODE");


			File fXmlFile = new File(fileName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			doc = dBuilder.parse(fXmlFile);

			//Get Child nodes for parent node which is a wrapper node
			NodeList nodeList = null;

			if(xpathRootNode == null){
				nodeList = doc.getChildNodes().item(0).getChildNodes();
			} else {
				XPathFactory factory = XPathFactory.newInstance();
				//using this factory to create an XPath object:
				XPath xpath = factory.newXPath();

				// XPath Query for showing all nodes value
				XPathExpression expr = xpath.compile(xpathRootNode);
				Object result = expr.evaluate(doc, XPathConstants.NODESET);
				nodeList = (NodeList) result;
			}

			nodeMap = new ConcurrentHashMap<>();

			for (int i = 0; i < nodeList.getLength(); i++) {
				Node node = nodeList.item(i);
				if (node.getNodeType() == Node.ELEMENT_NODE) {

					nodeMap.put(i,node);
				}
			}

			total = nodeMap.size();

			nodeIterator = nodeMap.values().iterator();

		} catch (Exception exc) {
			throw new CorbException("Problem loading data from xml file " , exc);
		}
	}

	private String nodeToString(Node node) throws CorbException {
		StringWriter sw = new StringWriter();
		try {
			Transformer t = TransformerFactory.newInstance().newTransformer();
			t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
			t.setOutputProperty(OutputKeys.INDENT, "yes");
			t.transform(new DOMSource(node), new StreamResult(sw));
		} catch (TransformerException te) {
			throw new CorbException("nodeToString Transformer Exception", te);
		}
		return sw.toString();
	}

	public String getProperty(String key) {
		String val = System.getProperty(key);
		if (val == null && properties != null) {
			val = properties.getProperty(key);
		}
		return trim(val);
	}

	private String readNextNode() throws IOException, CorbException {
		if(nodeIterator.hasNext()) {

			Node nextNode = nodeIterator.next();
			String line = trim(nodeToString(nextNode));
			if (line != null && isBlank(line)) {
				line = readNextNode();
			}
			return line;
		}
		return null;

	}

	@Override
	public boolean hasNext() throws CorbException {
		if (nextNode == null) {
			try {
				nextNode = readNextNode();
			} catch (Exception exc) {
				throw new CorbException("Problem while reading the xml file");
			}
		}
		return nextNode != null;
	}

	@Override
	public String next() throws CorbException {
		String node;
		if (nextNode != null) {
			node = nextNode;
			nextNode = null;
		} else {
			try {
				node = readNextNode();
			} catch (Exception exc) {
				throw new CorbException("Problem while reading the xml file");
			}
		}

		return node;
	}

	@Override
	public void close() {
		if (nodeMap != null) {
			LOG.info("closing xml file reader");
			try {
				doc = null;
				nodeMap.clear();

			} catch (Exception exc) {
				LOG.log(Level.SEVERE, "while closing xml file reader", exc);
			}
		}
	}


}
