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
package com.marklogic.developer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;

/**
 * @author mike.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * 
 */
public final class Utilities {

	private static final DateFormat m_ISO8601Local = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	private static final int BUFFER_SIZE = 32 * 1024;

	private Utilities() {
	}

	/**
	 * 
	 * @param date
	 * @return
	 * @throws ParseException
	 */
	public static Date parseDateTime(String date) throws ParseException {
		synchronized (m_ISO8601Local) {
			return m_ISO8601Local.parse(date.replaceFirst(":(\\d\\d)$", "$1"));
		}
	}

	/**
	 * 
	 * @return
	 */
	public static String formatDateTime() {
		return formatDateTime(new Date());
	}

	/**
	 * 
	 * @param date
	 * @return
	 */
	public static String formatDateTime(Date date) {
		if (date == null) {
			return formatDateTime(new Date());
		}
		// format in (almost) ISO8601 format
		String dateStr = null;
		synchronized (m_ISO8601Local) {
			dateStr = m_ISO8601Local.format(date);
		}

		// remap the timezone from 0000 to 00:00 (starts at char 22)
		return dateStr.substring(0, 22) + ":" + dateStr.substring(22);
	}

	/**
	 * @param path
	 * @return
	 */
	public static String getPathExtension(String path) {
		return path.replaceFirst(".*\\.([^\\.]+)$", "$1");
	}

	/**
	 * Joins items of the provided collection into a single String using the
	 * delimiter specified.
	 * 
	 * @param items
	 * @param delimiter
	 * @return
	 */
	public static String join(Collection<?> items, String delimiter) {
		if (items == null) {
			return null;
		}
		StringBuilder joinedValues = new StringBuilder();
		Iterator<?> iterator = items.iterator();
		if (iterator.hasNext()) {
			joinedValues.append(iterator.next().toString());
		}
		while (iterator.hasNext()) {
			joinedValues.append(delimiter);
			joinedValues.append(iterator.next().toString());
		}
		return joinedValues.toString();
	}

	/**
	 * Joins items of the provided Array of Objects into a single String using the
	 * delimiter specified.
	 * 
	 * @param items
	 * @param delimiter
	 * @return
	 */
	public static String join(Object[] items, String delimiter) {
		return join(Arrays.asList(items), delimiter);
	}

	/**
	 * Replace all occurrences of the following characters [&, <, >] with their
	 * corresponding entities.
	 * 
	 * @param str
	 * @return
	 */
	public static String escapeXml(String str) {
		if (str == null) {
			return "";
		}
		return str.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");
	}

	/**
	 * 
	 * @param inputStream
	 * @param outputStream
	 * @return
	 * @throws IOException
	 */
	public static long copy(InputStream inputStream, OutputStream outputStream) throws IOException {
		if (inputStream == null) {
			throw new IOException("null InputStream");
		}
		if (outputStream == null) {
			throw new IOException("null OutputStream");
		}
		long totalBytes = 0;
		int len = 0;
		byte[] buf = new byte[BUFFER_SIZE];
		int available = inputStream.available();
		// System.err.println("DEBUG: " + _in + ": available " + available);
		while ((len = inputStream.read(buf, 0, BUFFER_SIZE)) > -1) {
			outputStream.write(buf, 0, len);
			totalBytes += len;
			// System.err.println("DEBUG: " + _out + ": wrote " + len);
		}
		// System.err.println("DEBUG: " + _in + ": last read " + len);

		// caller MUST close the stream for us
		outputStream.flush();

		// check to see if we copied enough data
		if (available > totalBytes) {
			throw new IOException("expected at least " + available + " Bytes, copied only " + totalBytes);
		}
		return totalBytes;
	}

	/**
	 * @param source
	 * @param destination
	 * @throws IOException
	 */
	public static void copy(File source, File destination) throws IOException {
		InputStream inputStream = new FileInputStream(source);
		OutputStream outputStream = new FileOutputStream(destination);
		copy(inputStream, outputStream);
	}

	/**
	 * 
	 * @param source
	 * @param destination
	 * @return
	 * @throws IOException
	 */
	public static long copy(Reader source, OutputStream destination) throws IOException {
		if (source == null) {
			throw new IOException("null InputStream");
		}
		if (destination == null) {
			throw new IOException("null OutputStream");
		}
		long totalBytes = 0;
		int len = 0;
		char[] buf = new char[BUFFER_SIZE];
		byte[] bite = null;
		while ((len = source.read(buf)) > -1) {
			bite = new String(buf).getBytes();
			// len? different for char vs byte?
			// code is broken if I use bite.length, though
			destination.write(bite, 0, len);
			totalBytes += len;
		}

		// caller MUST close the stream for us
		destination.flush();

		// check to see if we copied enough data
		if (1 > totalBytes) {
			throw new IOException("expected at least " + 1 + " Bytes, copied only " + totalBytes);
		}
		return totalBytes;
	}

	/**
	 * @param sourceFilePath
	 * @param destinationFilePath
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void copy(String sourceFilePath, String destinationFilePath) throws FileNotFoundException, IOException {
		copy(new FileInputStream(sourceFilePath), new FileOutputStream(destinationFilePath));
	}

	/**
	 * 
	 * @param file
	 * @throws IOException
	 */
	public static void deleteFile(File file) throws IOException {
		if (!file.exists()) {
			return;
		}
		boolean success;

		if (!file.isDirectory()) {
			success = file.delete();
			if (!success) {
				throw new IOException("error deleting " + file.getCanonicalPath());
			}
			return;
		}

		// directory, so recurse
		File[] children = file.listFiles();
		if (children != null) {
			for (File children1 : children) {
				// recurse
				deleteFile(children1);
			}
		}

		// now this directory should be empty
		if (file.exists()) {
			file.delete();
		}
	}

	/**
	 * 
	 * @param str
	 * @return
	 */
	public static final boolean stringToBoolean(String str) {
		// let the caller decide: should an unset string be true or false?
		return stringToBoolean(str, false);
	}

	/**
	 * 
	 * @param str
	 * @param defaultValue
	 * @return
	 */
	public static final boolean stringToBoolean(String str, boolean defaultValue) {
		if (str == null) {
			return defaultValue;
		}
		String lcStr = str.toLowerCase();
		return !(str.equals("") || str.equals("0") || lcStr.equals("f") || lcStr.equals("false") || lcStr.equals("n") || lcStr
				.equals("no"));
	}

	/**
	 * @param path
	 * @throws IOException
	 */
	public static void deleteFile(String path) throws IOException {
		deleteFile(new File(path));
	}

	/**
	 * 
	 * @param clazz
	 * @return
	 */
	public static String buildModulePath(Class<?> clazz) {
		return "/" + clazz.getName().replace('.', '/') + ".xqy";
	}

	/**
	 * 
	 * @param modulePackage
	 * @param name
	 * @return
	 */
	public static String buildModulePath(Package modulePackage, String name) {
		return "/" + modulePackage.getName().replace('.', '/') + "/" + name + (name.endsWith(".xqy") ? "" : ".xqy");
	}

	/**
	 * @param r
	 * @return
	 * @throws IOException
	 */
	public static String cat(Reader r) throws IOException {
		StringBuilder rv = new StringBuilder();

		int size;
		char[] buf = new char[BUFFER_SIZE];
		while ((size = r.read(buf)) > 0) {
			rv.append(buf, 0, size);
		}
		return rv.toString();
	}

	/**
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static byte[] cat(InputStream is) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
		copy(is, bos);
		return bos.toByteArray();
	}

	/**
	 * 
	 * @param is
	 * @return
	 * @throws IOException
	 */
	public static long getSize(InputStream is) throws IOException {
		long size = 0;
		int b = 0;
		byte[] buf = new byte[BUFFER_SIZE];
		while ((b = is.read(buf)) > 0) {
			size += b;
		}
		return size;
	}

	/**
	 * 
	 * @param r
	 * @return
	 * @throws IOException
	 */
	public static long getSize(Reader r) throws IOException {
		long size = 0;
		int b = 0;
		char[] buf = new char[BUFFER_SIZE];
		while ((b = r.read(buf)) > 0) {
			size += b;
		}
		return size;
	}

	/**
	 * @param contentFile
	 * @return
	 * @throws IOException
	 */
	public static byte[] getBytes(File contentFile) throws IOException {
		InputStream is = null;
		ByteArrayOutputStream os = null;
		byte[] buf = new byte[BUFFER_SIZE];
		int read;
		try{
			is = new FileInputStream(contentFile);
			os = new ByteArrayOutputStream();
			while ((read = is.read(buf)) > 0) {
				os.write(buf, 0, read);
			}
			return os.toByteArray();
		}finally{
			if(os != null) os.close();
			if(is != null) is.close();
		}
	}

	/**
	 * @param value
	 * @param encoding
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String dumpHex(String value, String encoding) throws UnsupportedEncodingException {
		StringBuilder sb = new StringBuilder();
		byte[] bytes = value.getBytes(encoding);
		for (int i = 0; i < bytes.length; i++) {
			// bytes are signed: we want unsigned values
			sb.append(Integer.toHexString(bytes[i] & 0xff));
			if (i < bytes.length - 1) {
				sb.append(' ');
			}
		}
		return sb.toString();
	}

}
