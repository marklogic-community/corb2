/*
  * * Copyright (c) 2004-2016 MarkLogic Corporation
  * *
  * * Licensed under the Apache License, Version 2.0 (the "License");
  * * you may not use this file except in compliance with the License.
  * * You may obtain a copy of the License at
  * *
  * * http://www.apache.org/licenses/LICENSE-2.0
  * *
  * * Unless required by applicable law or agreed to in writing, software
  * * distributed under the License is distributed on an "AS IS" BASIS,
  * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * * See the License for the specific language governing permissions and
  * * limitations under the License.
  * *
  * * The use of the Apache License does not indicate that this project is
  * * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb.util;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mike.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @author Mads Hansen, MarkLogic Corporation
 */
public class StringUtils {

    public static final String EMPTY = "";
    private static final String ADHOC_PATTERN = "(?i).*\\|ADHOC";
    private static final String JAVASCRIPT_MODULE_FILENAME_PATTERN = "(?i).*\\.s?js(\\|ADHOC)?$";
    private static final String INLINE_MODULE_PATTERN = "(?i)INLINE-(JAVASCRIPT|XQUERY)\\|(.*?)(\\|ADHOC)?$";

    private static final Pattern COMPILED_INLINE_MODULE_PATTERN = Pattern.compile(INLINE_MODULE_PATTERN);

    private StringUtils() {
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
        return !(str.equals("") || str.equals("0") || lcStr.equals("f") || lcStr.equals("false") || lcStr.equals("n") || lcStr.equals("no"));
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
     * @param path
     * @return
     */
    public static String getPathExtension(String path) {
        return path.replaceFirst(".*\\.([^\\.]+)$", "$1");
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

    public static String buildModulePath(String root, String module) {
        if (!root.endsWith("/")) {
            root += "/";
        }

        if (module.startsWith("/") && module.length() > 1) {
            module = module.substring(1);
        }

        return root + module;
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
            sb.append(Integer.toHexString(bytes[i] & 255));
            if (i < bytes.length - 1) {
                sb.append(' ');
            }
        }
        return sb.toString();
    }

    /**
     * Checks if a CharSequence is null or empty ("")
     *
     * @param value
     * @return true if the value is null or empty
     */
    public static boolean isEmpty(final CharSequence value) {
        return value == null || value.length() == 0;
    }

    /**
     * Checks if a CharSequence is not null or empty ("")
     *
     * @param value
     * @return
     */
    public static boolean isNotEmpty(final CharSequence value) {
        return !isEmpty(value);
    }

    /**
     * Checks if a CharSequence is null or whitespace-only characters
     *
     * @param value
     * @return {@code true} if the value is null, empty, or whitespace-only 
     * characters; {@code false} otherwise.
     */
    public static boolean isBlank(final CharSequence value) {
        int length;
        if (value == null || (length = value.length()) == 0) {
            return true;
        }
        for (int i = 0; i < length; i++) {
            if (!Character.isWhitespace(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Checks if a CharSequence is not null and not whitespace-only characters
     *
     * @param value
     * @return
     */
    public static boolean isNotBlank(final CharSequence value) {
        return !isBlank(value);
    }

    /**
     * Removes control characters (char <= 32) from both ends of the string. If
     * null, returns null. @param value @return @param value @return the trimmed
     * string, or {@code null} if null String
     *
     * @param value
     * @return
     */
    public static String trim(final String value) {
        return value == null ? null : value.trim();
    }

    /**
     * Removes control characters (char <= 32) from both ends of the string. If
     * null, returns null. @param value @return @param value @return the trimmed
     * String or an empty String if {@code nul
     *
     * @param value
     * @return 
     */
    public static String trimToEmpty(final String value) {
        return value == null ? EMPTY : value.trim();
    }

    public static boolean isAdhoc(final String value) {
        return (value != null && value.matches(ADHOC_PATTERN));
    }

    public static boolean isJavaScriptModule(final String value) {
        return (value != null
                && (value.matches(JAVASCRIPT_MODULE_FILENAME_PATTERN)
                    || inlineModuleLanguage(value).equalsIgnoreCase("javascript")));
    }

    public static boolean isInlineModule(final String value) {
        return (value != null && value.matches(INLINE_MODULE_PATTERN));
    }

    public static boolean isInlineOrAdhoc(final String value) {
        return StringUtils.isInlineModule(value) || isAdhoc(value);
    }
    
    public static String inlineModuleLanguage(final String value) {
        String language = "";
        if (isInlineModule(value)) {
            Matcher m = COMPILED_INLINE_MODULE_PATTERN.matcher(value);
            if (m.find()) {
                language = m.group(1);
            }
        }
        return language;
    }

    public static String getInlineModuleCode(final String value) {
        String code = "";
        if (isInlineModule(value)) {
            Matcher m = COMPILED_INLINE_MODULE_PATTERN.matcher(value);
            if (m.find()) {
                code = m.group(2);
            }
        }
        return code;
    }
}
