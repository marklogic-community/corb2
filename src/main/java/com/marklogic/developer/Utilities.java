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
     * Joins items of the provided Array of Objects into a single String using
     * the delimiter specified.
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
