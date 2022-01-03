/*
  * * Copyright (c) 2004-2022 MarkLogic Corporation
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
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author mike.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class StringUtils {

    public static final String EMPTY = "";
    public static final String SLASH = "/";
    public static final String COMMA = ",";
    public static final String XQUERY_EXTENSION = ".xqy";
    private static final Pattern ADHOC_PATTERN = Pattern.compile("(?i).*\\|ADHOC");
    private static final Pattern JAVASCRIPT_MODULE_FILENAME_PATTERN = Pattern.compile("(?i).*\\.s?js(\\|ADHOC)?$");
    private static final Pattern INLINE_MODULE_PATTERN = Pattern.compile("(?i)INLINE-(JAVASCRIPT|XQUERY)\\|(.*?)(\\|ADHOC)?$");
    private static final String UTF_8 = "UTF-8";
    private static final String UTF_8_NOT_SUPPORTED = UTF_8 + " not supported";

    private StringUtils() {
    }

    /**
     *
     * @param str
     * @return
     */
    public static boolean stringToBoolean(String str) {
        // let the caller decide: should an unset string be true or false?
        return stringToBoolean(str, false);
    }

    /**
     *
     * @param str
     * @param defaultValue
     * @return
     */
    public static boolean stringToBoolean(String str, boolean defaultValue) {
        if (str == null) {
            return defaultValue;
        }
        String lcStr = str.trim().toLowerCase();
        return !(lcStr.isEmpty() || "0".equals(lcStr) || "f".equals(lcStr) || "false".contains(lcStr) ||
                "n".equals(lcStr) || "no".equals(lcStr));
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
        return SLASH + clazz.getName().replace('.', '/') + XQUERY_EXTENSION;
    }

    /**
     *
     * @param modulePackage
     * @param name
     * @return
     */
    public static String buildModulePath(Package modulePackage, String name) {
        return SLASH + modulePackage.getName().replace('.', '/') + SLASH + name + (name.endsWith(XQUERY_EXTENSION) ? "" : XQUERY_EXTENSION);
    }

    public static String buildModulePath(String root, String module) {
        String moduleRoot = root;
        String modulePath = module;
        if (!root.endsWith(SLASH)) {
            moduleRoot += SLASH;
        }

        if (module.startsWith(SLASH) && module.length() > 1) {
            modulePath = module.substring(1);
        }

        return moduleRoot + modulePath;
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
     * Splits the provided value into an Array using the specified regex.
     * @param value
     * @param regex
     * @return An Array of String, an empty Array if the value is null.
     */
    public static String[] split(final String value, String regex) {
        return value == null ? new String[0] : value.split(regex);
    }

    /**
     * Split a CSV and return List of values
     * @param value
     * @return
     */
    public static List<String> commaSeparatedValuesToList(String value) {
        List<String> values = new ArrayList<>();
        for (String item : split(value, COMMA)) {
            values.add(item.trim());
        }
        return values;
    }

    /**
     * Removes control characters (char &lt;= 32) from both ends of the string. If
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
     * Removes control characters (char &lt;= 32) from both ends of the string. If
     * null, returns null. @param value @return @param value @return the trimmed
     * String or an empty String if {@code null}
     *
     * @param value
     * @return
     */
    public static String trimToEmpty(final String value) {
        return value == null ? EMPTY : value.trim();
    }

    public static boolean isAdhoc(final String value) {
        return value != null && ADHOC_PATTERN.matcher(value).matches();
    }

    public static boolean isJavaScriptModule(final String value) {
        return value != null
                && (JAVASCRIPT_MODULE_FILENAME_PATTERN.matcher(value).matches()
                    || "javascript".equalsIgnoreCase(inlineModuleLanguage(value)));
    }

    public static boolean isInlineModule(final String value) {
        return value != null && INLINE_MODULE_PATTERN.matcher(value).matches();
    }

    public static boolean isInlineOrAdhoc(final String value) {
        return StringUtils.isInlineModule(value) || isAdhoc(value);
    }

    public static String inlineModuleLanguage(final String value) {
        String language = "";
        if (isInlineModule(value)) {
            Matcher m = INLINE_MODULE_PATTERN.matcher(value);
            if (m.find()) {
                language = m.group(1);
            }
        }
        return language;
    }

    public static String getInlineModuleCode(final String value) {
        String code = "";
        if (isInlineModule(value)) {
            Matcher m = INLINE_MODULE_PATTERN.matcher(value);
            if (m.find()) {
                code = m.group(2);
            }
        }
        return code;
    }


    /**
     * Build an XCC URI from the values provided. Values will be URLEncoded, if it does not appear that they have already been URLEncoded.
     * @param protocol
     * @param username
     * @param password
     * @param host
     * @param port
     * @param dbname
     * @param urlEncode
     * @return
     */
    public static String getXccUri(String protocol, String username, String password, String host, String port, String dbname, String urlEncode) {
        if (isBlank(protocol)) {
            protocol = "xcc";
        }
        if (isBlank(dbname)){
            dbname = EMPTY;
        }
        //If URL-ENCODE
        if (!"never".equalsIgnoreCase(urlEncode)) {
            if ("always".equalsIgnoreCase(urlEncode)) {
                username = urlEncode(username);
                password = urlEncode(password);
                dbname = urlEncode(dbname);
            } else {
                username = urlEncodeIfNecessary(username);
                password = urlEncodeIfNecessary(password);
                dbname = urlEncodeIfNecessary(dbname);
            }
        }
        return protocol + "://" + username + ':' + password + '@' + host + ':' + port + (isBlank(dbname) ? EMPTY : SLASH + dbname);
    }

    /**
     * Indicate whether any items in the array of String objects are null.
     * @param args
     * @return
     */
    public static boolean anyIsNull(String... args) {
        return Arrays.asList(args).contains(null);
    }

    /**
     * If the given string is not URLEncoded, encode it. Otherwise, return the original value.
     * @param arg
     * @return
     */
    protected static String urlEncodeIfNecessary(String arg) {
        return isUrlEncoded(arg) ? arg : urlEncode(arg);
    }

    /**
     * Determines whether the value is URLEncoded by evaluating whether the
     * length of the URLDecoded value is the same.
     * @param arg
     * @return
     */
    public static boolean isUrlEncoded(String arg) {
        try {
            return arg.length() != urlDecode(arg).length();
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    /**
     * URLEncode the given value
     * @param arg
     * @return
     */
    protected static String urlEncode(String arg) {
        try {
            return URLEncoder.encode(arg, UTF_8);
        } catch (UnsupportedEncodingException ex) {
            throw new AssertionError(UTF_8_NOT_SUPPORTED, ex);
        }
    }

    /**
     * URLDecode the given value
     * @param arg
     * @return
     */
    protected static String urlDecode(String arg) {
        try {
            return URLDecoder.decode(arg, UTF_8);
        } catch (UnsupportedEncodingException ex) {
            throw new AssertionError(UTF_8_NOT_SUPPORTED, ex);
        }
    }

	public static Set<Integer> parsePortRanges(String jobServerPort) {
		Set<Integer> jobServerPorts = new LinkedHashSet<>();
		if (isNotBlank(jobServerPort)) {
            for (String aSplitByComma : commaSeparatedValuesToList(jobServerPort)) {
                if (aSplitByComma.contains("-")) {
                    String[] splitByDash = aSplitByComma.split("\\s*-\\s*");
                    if (splitByDash.length == 2) {
                        Integer start = Integer.parseInt(splitByDash[0]);
                        Integer end = Integer.parseInt(splitByDash[1]);
                        if (start > end) {
                            int tmp = start;
                            start = end;
                            end = tmp;
                        }
                        for (int j = start; j <= end; j++) {
                            jobServerPorts.add(j);
                        }
                    }
                } else {
                    jobServerPorts.add(Integer.parseInt(aSplitByComma));
                }
            }
        }
		return jobServerPorts;
	}

}
