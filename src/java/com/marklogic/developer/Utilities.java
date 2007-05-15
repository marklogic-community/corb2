/*
 * Copyright (c)2005-2007 Mark Logic Corporation
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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author mike.blakeley@marklogic.com
 * 
 */
public class Utilities {

    private static DateFormat m_ISO8601Local = new SimpleDateFormat(
            "yyyy-MM-dd'T'HH:mm:ssZ");

    // private static DateFormat m_ISO8601plusRFC822 = new SimpleDateFormat(
    // "yyyy-MM-dd'T'HH:mm:ssz");

    private static final int BUFFER_SIZE = 32 * 1024;

    public static Date parseDateTime(String _date) throws ParseException {
        synchronized (m_ISO8601Local) {
            return m_ISO8601Local.parse(_date.replaceFirst(":(\\d\\d)$", "$1"));
        }
    }

    public static String formatDateTime() {
        return formatDateTime(new Date());
    }

    public static String formatDateTime(Date date) {
        if (date == null)
            return formatDateTime(new Date());

        // format in (almost) ISO8601 format
        String dateStr = null;
        synchronized (m_ISO8601Local) {
            dateStr = m_ISO8601Local.format(date);
        }
        
        // remap the timezone from 0000 to 00:00 (starts at char 22)
        return dateStr.substring(0, 22) + ":" + dateStr.substring(22);
    }

    /**
     * @param _path
     * @return
     */
    public static String getPathExtension(String _path) {
        return _path.replaceFirst(".*\\.([^\\.]+)$", "$1");
    }

    public static String listToXml(List _list, String _root, String _element) {
        return listToXml(_list, _root, _element, null, false);
    }

    public static String listToXml(List _list, String _root, String _namespace,
            String _element) {
        return listToXml(_list, _root, _element, _namespace, false);
    }

    public static String listToXml(List _list, String _root, String _element,
            String _namespace, boolean _preserveSpace) {
        // preserve xml whitespace everywhere,
        // or strings will be whitespace-normalized
        String rootName = escapeXml(_root);
        String rv = "<" + rootName;
        if (_namespace != null)
            rv = rv + " xmlns=\"" + _namespace + "\"";
        rv = rv + ">";

        String elementName = escapeXml(_element);
        Iterator i = _list.iterator();
        Object v = null;
        String attrs = "";
        if (_preserveSpace)
            attrs = " xml:space=\"preserve\"";

        while (i.hasNext()) {
            v = i.next();
            if (v instanceof Map) {
                // assume it's a Map and try to descend
                rv += mapToXml((Map) v, elementName, null, _preserveSpace);
            } else {
                // escape illegal characters in values
                rv += "<" + elementName + attrs + ">" + escapeXml(v.toString())
                        + "</" + elementName + ">";
            }
        }

        rv += "</" + rootName + ">";
        return rv;
    }

    public static String arrayToXml(Object[] _list, String _root,
            String _element) {
        return arrayToXml(_list, _root, _element, null, null, false);
    }

    public static String arrayToXml(Object[] _list, String _root,
            String _namespace, String _element) {
        return arrayToXml(_list, _root, _element, _namespace, null, false);
    }

    public static String arrayToXml(Object[] _list, String _root,
            String _element, String _namespace, String _rootAttributes) {
        return arrayToXml(_list, _root, _element, _namespace, _rootAttributes,
                false);
    }

    public static String arrayToXml(Object[] _list, String _root,
            String _element, String _namespace, String _rootAttributes,
            boolean _preserveSpace) {

        // preserve xml whitespace everywhere,
        // or strings will be whitespace-normalized
        String rootName = _root;
        if (_root != null)
            rootName = escapeXml(_root);
        StringBuffer rv = new StringBuffer();
        if (rootName != null) {
            rv.append("<");
            rv.append(rootName);
            if (_namespace != null) {
                rv.append(" xmlns=\"");
                rv.append(_namespace);
                rv.append("\"");
            }
            if (_rootAttributes != null) {
                rv.append(" ");
                rv.append(_rootAttributes);
            }
            rv.append(">");
        }

        if (_list != null) {
            String elementName = escapeXml(_element);
            String attrs = "";
            if (_preserveSpace)
                attrs = " xml:space=\"preserve\"";

            for (int i = 0; i < _list.length; i++) {
                if (_list[i] instanceof Map) {
                    // assume it's a Map and try to descend
                    rv.append(mapToXml((Map) _list[i], elementName, null,
                            _preserveSpace));
                } else {
                    // escape illegal characters in values
                    rv.append("<");
                    rv.append(elementName);
                    rv.append(attrs);
                    rv.append(">");
                    rv.append(escapeXml(_list[i].toString()));
                    rv.append("</");
                    rv.append(elementName);
                    rv.append(">");
                }
            }
        }

        if (rootName != null) {
            rv.append("</");
            rv.append(rootName);
            rv.append(">");
        }
        return rv.toString();
    }

    /**
     * turn a hashmap into xml
     * 
     * @param _h
     * @return
     */
    public static String mapToXml(Map _h) {
        return mapToXml(_h, "params", false);
    }

    public static String mapToXml(Map _h, String _root) {
        return mapToXml(_h, _root, false);
    }

    public static String mapToXml(Map _h, boolean _preserveSpace) {
        return mapToXml(_h, "params", _preserveSpace);
    }

    /**
     * @param
     * @param
     * @param
     * @return
     */
    public static String mapToXml(Map _h, String _root, String _nameSpace) {
        return mapToXml(_h, _root, _nameSpace, false);
    }

    public static String mapToXml(Map _h, String _root, boolean _preserveSpace) {
        return mapToXml(_h, _root, null, _preserveSpace);
    }

    /**
     * @param
     * @param
     * @param
     * @param
     * @return
     */
    public static String mapToXml(Map _h, String _root, String _nameSpace,
            boolean _preserveSpace) {
        // preserve xml whitespace everywhere,
        // or strings will be whitespace-normalized
        String topElementName = escapeXml(_root);
        String rv = "<" + topElementName;
        if (_nameSpace != null)
            rv = rv + " xmlns=\"" + _nameSpace + "\"";
        rv = rv + ">";
        Set keys = _h.keySet();
        Iterator i = keys.iterator();
        String k = null;
        Object v = null;
        String attrs = "";
        if (_preserveSpace)
            attrs = " xml:space=\"preserve\"";

        while (i.hasNext()) {
            k = (String) i.next();
            v = _h.get(k);
            if (v instanceof Map) {
                // assume it's a Map and try to recurse
                rv += mapToXml((Map) v, k, null, _preserveSpace);
            } else if (v instanceof List) {
                // try to handle the list
                String elementName = null;
                if (k.endsWith("s"))
                    elementName = k.substring(0, k.length() - 1);
                else
                    elementName = k + "-item";
                listToXml((List) v, k, null, elementName, _preserveSpace);
            } else {
                // escape illegal characters in values
                rv += "<" + k + attrs + ">" + escapeXml(v.toString()) + "</"
                        + k + ">";
            }
        }

        rv += "</" + topElementName + ">";
        return rv;
    }

    public static String join(List _items, String _delim) {
        if (null == _items) {
            return null;
        }
        return join(_items.toArray(), _delim);
    }

    public static String join(Object[] _items, String _delim) {
        String rval = "";
        for (int i = 0; i < _items.length; i++)
            if (i == 0)
                rval = "" + _items[0];
            else
                rval += _delim + _items[i];
        return rval;
    }

    /**
     * @param _items
     * @param _delim
     * @return
     */
    public static String join(String[] _items, String _delim) {
        String rval = "";
        for (int i = 0; i < _items.length; i++)
            if (i == 0)
                rval = _items[0];
            else
                rval += _delim + _items[i];
        return rval;
    }

    public static String escapeXml(String _in) {
        if (_in == null)
            return "";
        return _in.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(
                ">", "&gt;");
    }

    public static void main(String[] args) throws Exception {
        String dateString = "1997-07-11T01:00:00-04:00";
        Date theDate = parseDateTime(dateString);
        System.out.println(theDate);
    }

    public static long copy(InputStream _in, OutputStream _out)
            throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        int available = _in.available();
        // System.err.println("DEBUG: " + _in + ": available " + available);
        while ((len = _in.read(buf, 0, BUFFER_SIZE)) > -1) {
            _out.write(buf, 0, len);
            totalBytes += len;
            // System.err.println("DEBUG: " + _out + ": wrote " + len);
        }
        // System.err.println("DEBUG: " + _in + ": last read " + len);

        // caller MUST close the stream for us
        _out.flush();

        // check to see if we copied enough data
        if (available > totalBytes)
            throw new IOException("expected at least " + available
                    + " Bytes, copied only " + totalBytes);

        return totalBytes;
    }

    /**
     * @param _in
     * @param _out
     * @throws IOException
     */
    public static void copy(File _in, File _out) throws IOException {
        InputStream in = new FileInputStream(_in);
        OutputStream out = new FileOutputStream(_out);
        copy(in, out);
    }

    public static long copy(Reader _in, OutputStream _out) throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        char[] buf = new char[BUFFER_SIZE];
        byte[] bite = null;
        while ((len = _in.read(buf)) > -1) {
            bite = new String(buf).getBytes();
            // len? different for char vs byte?
            // code is broken if I use bite.length, though
            _out.write(bite, 0, len);
            totalBytes += len;
        }

        // caller MUST close the stream for us
        _out.flush();

        // check to see if we copied enough data
        if (1 > totalBytes)
            throw new IOException("expected at least " + 1
                    + " Bytes, copied only " + totalBytes);

        return totalBytes;
    }

    /**
     * @param inFilePath
     * @param outFilePath
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void copy(String inFilePath, String outFilePath)
            throws FileNotFoundException, IOException {
        copy(new FileInputStream(inFilePath), new FileOutputStream(outFilePath));
    }

    public static void deleteFile(File _file) throws IOException {
        if (!_file.exists())
            return;

        boolean success;

        if (!_file.isDirectory()) {
            success = _file.delete();
            if (!success) {
                throw new IOException("error deleting "
                        + _file.getCanonicalPath());
            }
            return;
        }

        // directory, so recurse
        File[] children = _file.listFiles();
        if (children != null) {
            for (int i = 0; i < children.length; i++) {
                // recurse
                deleteFile(children[i]);
            }
        }

        // now this directory should be empty
        if (_file.exists()) {
            _file.delete();
        }
    }

    public static final boolean stringToBoolean(String str) {
        // let the caller decide: should an unset string be true or false?
        return stringToBoolean(str, false);
    }

    public static final boolean stringToBoolean(String str, boolean defaultValue) {
        if (str == null)
            return defaultValue;

        String lcStr = str.toLowerCase();
        if (str == "" || str.equals("0") || lcStr.equals("f")
                || lcStr.equals("false") || lcStr.equals("n")
                || lcStr.equals("no"))
            return false;

        return true;
    }

    /**
     * @param outHtmlFileName
     * @throws IOException
     */
    public static void deleteFile(String _path) throws IOException {
        deleteFile(new File(_path));
    }

    public static String buildModulePath(Class _class) {
        return "/" + _class.getName().replace('.', '/') + ".xqy";
    }

    public static String buildModulePath(Package _package, String _name) {
        return "/" + _package.getName().replace('.', '/') + "/" + _name
                + (_name.endsWith(".xqy") ? "" : ".xqy");
    }

    /**
     * @param r
     * @return
     * @throws IOException
     */
    public static String cat(Reader r) throws IOException {
        StringBuffer rv = new StringBuffer();

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

    public static long getSize(InputStream is) throws IOException {
        long size = 0;
        int b = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        while ((b = is.read(buf)) > 0) {
            size += b;
        }
        return size;
    }

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
        InputStream is = new FileInputStream(contentFile);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buf = new byte[BUFFER_SIZE];
        int read;

        while ((read = is.read(buf)) > 0) {
            os.write(buf, 0, read);
        }

        return os.toByteArray();
    }

    /**
     * @param _string
     * @param _encoding
     * @return
     * @throws UnsupportedEncodingException
     */
    public static String dumpHex(String _string, String _encoding)
            throws UnsupportedEncodingException {
        StringBuffer sb = new StringBuffer();
        byte[] bytes = _string.getBytes(_encoding);
        for (int i = 0; i < bytes.length; i++) {
            // bytes are signed: we want unsigned values
            sb.append(Integer.toHexString(bytes[i] & 0xff));
            if (i < bytes.length - 1) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

}