/*
  * * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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

import static com.marklogic.developer.corb.Options.*;


/**
 * Utility class for string manipulation operations.
 * Provides methods for string conversion, validation, encoding, URL building,
 * and pattern matching. This class handles common string operations safely
 * with null-safe implementations.
 *
 * @author mike.blakeley@marklogic.com
 * @author Bhagat Bandlamudi, MarkLogic Corporation
 * @author Mads Hansen, MarkLogic Corporation
  * @since 2.0.0
 */
public final class StringUtils {

    /** Constant representing an empty string */
    public static final String EMPTY = "";
    /** Constant representing a forward slash character */
    public static final String SLASH = "/";
    /** Constant representing a comma character */
    public static final String COMMA = ",";
    /** File extension for XQuery modules */
    public static final String XQUERY_EXTENSION = ".xqy";
    /** Pattern to match adhoc module specifications ending with "|ADHOC" (case-insensitive) */
    private static final Pattern ADHOC_PATTERN = Pattern.compile("(?i).*\\|ADHOC");
    /** Pattern to match JavaScript module filenames */
    private static final Pattern JAVASCRIPT_MODULE_FILENAME_PATTERN = Pattern.compile("(?i).*\\.s?js(\\|ADHOC)?$");
    /** Pattern to match inline module declarations */
    private static final Pattern INLINE_MODULE_PATTERN = Pattern.compile("(?i)INLINE-(JAVASCRIPT|XQUERY)\\|(.*?)(\\|ADHOC)?$");
    /** UTF-8 character encoding constant */
    private static final String UTF_8 = "UTF-8";
    /** Error message for unsupported UTF-8 encoding */
    private static final String UTF_8_NOT_SUPPORTED = UTF_8 + " not supported";

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private StringUtils() {
    }

    /**
     * Converts a string to a boolean value, returning false if the conversion fails.
     * This method delegates to {@link #stringToBoolean(String, boolean)} with a default value of false.
     *
     * @param str the string to convert, may be null
     * @return {@code false} if the string is null or represents a false value; {@code true} otherwise
     */
    public static boolean stringToBoolean(String str) {
        // let the caller decide: should an unset string be true or false?
        return stringToBoolean(str, false);
    }

    /**
     * Converts a string to a boolean value with a specified default.
     * Treats the following values as false: null, empty string, "0", "f", "false", "n", "no" (case-insensitive).
     * All other non-null, non-empty values are treated as true.
     *
     * @param str the string to convert, may be null
     * @param defaultValue the value to return if the string is null
     * @return the boolean representation of the string, or the default value if null
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
     * Converts a byte array into a string of hex-encoded values.
     * Each byte is represented as a two-character hexadecimal string.
     *
     * @param bytes the byte array to convert
     * @return a string containing the hexadecimal representation of the byte array
     */
    public static String byteArrayToHexString(byte[] bytes) {
        StringBuilder hexStringBuilder = new StringBuilder(bytes.length * 2);
        for (byte b: bytes) {
            hexStringBuilder.append(String.format("%02x", b));
        }
        return hexStringBuilder.toString();
    }

    /**
     * Converts a hexadecimal string into an array of bytes.
     * Each pair of characters in the hex string represents one byte.
     *
     * @param hexString the hexadecimal string to convert (must have even length)
     * @return a byte array representing the decoded hexadecimal string
     */
    public static byte[] hexStringToByteArray(String hexString) {
        int len = hexString.length();
        byte[] result = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            result[i / 2] = (byte) ((Character.digit(hexString.charAt(i), 16) << 4)
                + Character.digit(hexString.charAt(i+1), 16));
        }
        return result;
    }

    /**
     * Joins items of the provided collection into a single String using the
     * delimiter specified.
     *
     * @param items the collection of items to join, may be null
     * @param delimiter the delimiter to use between items
     * @return a string containing all items joined by the delimiter, or null if the collection is null
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
     * Joins items of the provided array of objects into a single String using
     * the delimiter specified.
     *
     * @param items the array of items to join
     * @param delimiter the delimiter to use between items
     * @return a string containing all items joined by the delimiter
     */
    public static String join(Object[] items, String delimiter) {
        return join(Arrays.asList(items), delimiter);
    }

    /**
     * Extracts the file extension from a given path.
     *
     * @param path the file path from which to extract the extension
     * @return the file extension without the dot, or the original string if no extension found
     */
    public static String getPathExtension(String path) {
        return path.replaceFirst(".*\\.([^\\.]+)$", "$1");
    }

    /**
     * Builds a module path from a class by converting the package structure to a path
     * and appending the XQuery extension.
     *
     * @param clazz the class to build the module path from
     * @return the module path with XQuery extension (e.g., "/com/example/MyClass.xqy")
     */
    public static String buildModulePath(Class<?> clazz) {
        return SLASH + clazz.getName().replace('.', '/') + XQUERY_EXTENSION;
    }

    /**
     * Builds a module path from a package and module name.
     * Appends the XQuery extension if not already present.
     *
     * @param modulePackage the package containing the module
     * @param name the module name
     * @return the complete module path with XQuery extension
     */
    public static String buildModulePath(Package modulePackage, String name) {
        return SLASH + modulePackage.getName().replace('.', '/') + SLASH + name + (name.endsWith(XQUERY_EXTENSION) ? "" : XQUERY_EXTENSION);
    }

    /**
     * Builds a module path by combining a root path and module name.
     * Handles leading/trailing slashes appropriately.
     *
     * @param root the root path
     * @param module the module name or path
     * @return the complete module path
     */
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
     * Converts a string to its hexadecimal representation using the specified encoding.
     * Each byte is represented as a space-separated hexadecimal value.
     *
     * @param value the string to convert to hexadecimal
     * @param encoding the character encoding to use (e.g., "UTF-8")
     * @return a string containing space-separated hexadecimal values
     * @throws UnsupportedEncodingException if the encoding is not supported
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
     * Encodes special characters in a string for safe HTML display.
     *
     * @param input the string to encode
     * @return the encoded string with HTML entities, or null if the input is null
     */
    public static String encodeForHtml(String input) {
        if (input == null) { return null; }

        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '<': sb.append("&lt;"); break;
                case '>': sb.append("&gt;"); break;
                case '&': sb.append("&amp;"); break;
                case '"': sb.append("&quot;"); break;
                case '\'': sb.append("&#39;"); break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Checks if a CharSequence is null or empty ("").
     *
     * @param value the CharSequence to check, may be null
     * @return {@code true} if the value is null or has zero length; {@code false} otherwise
     */
    public static boolean isEmpty(final CharSequence value) {
        return value == null || value.length() == 0;
    }

    /**
     * Checks if a CharSequence is not null and not empty ("").
     *
     * @param value the CharSequence to check, may be null
     * @return {@code true} if the value is not null and has length greater than zero; {@code false} otherwise
     */
    public static boolean isNotEmpty(final CharSequence value) {
        return !isEmpty(value);
    }

    /**
     * Checks if a CharSequence is null, empty, or contains only whitespace characters.
     *
     * @param value the CharSequence to check, may be null
     * @return {@code true} if the value is null, empty, or whitespace-only; {@code false} otherwise
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
     * Checks if a CharSequence is not null, not empty, and contains at least one non-whitespace character.
     *
     * @param value the CharSequence to check, may be null
     * @return {@code true} if the value contains at least one non-whitespace character; {@code false} otherwise
     */
    public static boolean isNotBlank(final CharSequence value) {
        return !isBlank(value);
    }

    /**
     * Splits the provided value into an array using the specified regex pattern.
     *
     * @param value the string to split, may be null
     * @param regex the regular expression to use as delimiter
     * @return an array of strings, or an empty array if the value is null
     */
    public static String[] split(final String value, String regex) {
        return value == null ? new String[0] : value.split(regex);
    }

    /**
     * Splits a comma-separated string and returns a list of trimmed values.
     *
     * @param value the comma-separated string to split
     * @return a list of trimmed string values
     */
    public static List<String> commaSeparatedValuesToList(String value) {
        List<String> values = new ArrayList<>();
        for (String item : split(value, COMMA)) {
            values.add(item.trim());
        }
        return values;
    }

    /**
     * Removes control characters (char &lt;= 32) from both ends of the string.
     *
     * @param value the string to trim, may be null
     * @return the trimmed string, or {@code null} if the input is null
     */
    public static String trim(final String value) {
        return value == null ? null : value.trim();
    }

    /**
     * Removes control characters (char &lt;= 32) from both ends of the string.
     * Returns an empty string if the input is null.
     *
     * @param value the string to trim, may be null
     * @return the trimmed string, or an empty string if the input is null
     */
    public static String trimToEmpty(final String value) {
        return value == null ? EMPTY : value.trim();
    }

    /**
     * Checks if the given value represents an adhoc module.
     * An adhoc module is identified by the "|ADHOC" suffix (case-insensitive).
     *
     * @param value the string to check, may be null
     * @return {@code true} if the value is an adhoc module; {@code false} otherwise
     */
    public static boolean isAdhoc(final String value) {
        return value != null && ADHOC_PATTERN.matcher(value).matches();
    }

    /**
     * Checks if the given value represents a JavaScript module.
     * Identifies JavaScript modules by file extension (.js, .sjs) or inline module language.
     *
     * @param value the string to check, may be null
     * @return {@code true} if the value represents a JavaScript module; {@code false} otherwise
     */
    public static boolean isJavaScriptModule(final String value) {
        return value != null
                && (JAVASCRIPT_MODULE_FILENAME_PATTERN.matcher(value).matches()
                    || "javascript".equalsIgnoreCase(inlineModuleLanguage(value)));
    }

    /**
     * Checks if the given value represents an inline module.
     * Inline modules have the format "INLINE-{LANGUAGE}|{code}" (case-insensitive).
     *
     * @param value the string to check, may be null
     * @return {@code true} if the value is an inline module; {@code false} otherwise
     */
    public static boolean isInlineModule(final String value) {
        return value != null && INLINE_MODULE_PATTERN.matcher(value).matches();
    }

    /**
     * Checks if the given value represents either an inline or adhoc module.
     *
     * @param value the string to check, may be null
     * @return {@code true} if the value is an inline or adhoc module; {@code false} otherwise
     */
    public static boolean isInlineOrAdhoc(final String value) {
        return StringUtils.isInlineModule(value) || isAdhoc(value);
    }

    /**
     * Extracts the language type from an inline module declaration.
     * Returns the language identifier (e.g., "JAVASCRIPT" or "XQUERY").
     *
     * @param value the inline module string
     * @return the language identifier, or an empty string if not an inline module
     */
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

    /**
     * Extracts the code content from an inline module declaration.
     *
     * @param value the inline module string
     * @return the module code, or an empty string if not an inline module
     */
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
     * Builds an XCC URI from the provided connection parameters.
     * Values will be URL-encoded if they do not appear to be already encoded.
     *
     * @param protocol the connection protocol (e.g., "xcc")
     * @param username the database username
     * @param password the database password
     * @param host the database host
     * @param port the database port
     * @param dbname the database name
     * @param urlEncode encoding strategy: "always", "never", or auto-detect
     * @return the complete XCC URI string
     * @deprecated Use {@link #getXccUri(Map, String)} instead
     */
    @Deprecated
    public static String getXccUri(String protocol, String username, String password, String host, String port, String dbname, String urlEncode) {
        Map<String, String> parameters = new HashMap<>();
        parameters.put(XCC_PROTOCOL, protocol);
        parameters.put(XCC_USERNAME, username);
        parameters.put(XCC_PASSWORD, password);
        parameters.put(XCC_HOSTNAME, host);
        parameters.put(XCC_PORT, port);
        parameters.put(XCC_DBNAME, dbname);
        return getXccUri(parameters, urlEncode);
    }

    /**
     * Builds an XCC URI from a map of connection parameters.
     * Values will be URL-encoded if they do not appear to be already encoded.
     * Encoding can be controlled with the urlEncode parameter:
     * <ul>
     *   <li>"always" - always URL-encode values</li>
     *   <li>"never" - never URL-encode values</li>
     *   <li>any other value - auto-detect and encode if necessary</li>
     * </ul>
     *
     * @param xccConnectionParameters map containing connection parameters (protocol, host, port, username, password, etc.)
     * @param urlEncode encoding strategy: "always", "never", or auto-detect
     * @return the complete XCC URI string
     */
    public static String getXccUri(Map<String, String> xccConnectionParameters, String urlEncode) {
        String protocol = xccConnectionParameters.getOrDefault(XCC_PROTOCOL, "xcc");
        String host = xccConnectionParameters.getOrDefault(XCC_HOSTNAME, EMPTY);
        String port = xccConnectionParameters.getOrDefault(XCC_PORT, EMPTY);
        String dbname = xccConnectionParameters.getOrDefault(XCC_DBNAME, EMPTY);
        String basePath = xccConnectionParameters.getOrDefault(XCC_BASE_PATH, EMPTY);
        String apiKey = xccConnectionParameters.getOrDefault(XCC_API_KEY, EMPTY);
        String username = xccConnectionParameters.getOrDefault(XCC_USERNAME, EMPTY);
        String password = xccConnectionParameters.getOrDefault(XCC_PASSWORD, EMPTY);
        String tokenDuration = xccConnectionParameters.getOrDefault(XCC_TOKEN_DURATION, EMPTY);
        String grantType = xccConnectionParameters.getOrDefault(XCC_GRANT_TYPE, EMPTY);
        String tokenEndpoint = xccConnectionParameters.getOrDefault(XCC_TOKEN_ENDPOINT, EMPTY);
        String oauthToken = xccConnectionParameters.getOrDefault(XCC_OAUTH_TOKEN, EMPTY);
        if (isBlank(protocol)) {
            protocol = "xcc";
        }
        dbname = isBlank(dbname) ? EMPTY : dbname;

        //If URL-ENCODE
        if (!"never".equalsIgnoreCase(urlEncode)) {
            if ("always".equalsIgnoreCase(urlEncode)) {
                username = urlEncode(username);
                password = urlEncode(password);
                dbname = urlEncode(dbname);
                apiKey = urlEncode(apiKey);
                basePath = urlEncode(basePath);
                oauthToken = urlEncode(oauthToken);
                tokenEndpoint = urlEncode(tokenEndpoint);
            } else {
                username = urlEncodeIfNecessary(username);
                password = urlEncodeIfNecessary(password);
                dbname = urlEncodeIfNecessary(dbname);
                apiKey = urlEncodeIfNecessary(apiKey);
                basePath = urlEncodeIfNecessary(basePath);
                oauthToken = urlEncodeIfNecessary(oauthToken);
                tokenEndpoint = urlEncodeIfNecessary(tokenEndpoint);
            }
        }
        String query = EMPTY;
        query = appendParameter(query, "apikey", apiKey);
        query = appendParameter(query, "basepath", basePath);
        query = appendParameter(query, "granttype", grantType);
        query = appendParameter(query, "oauthtoken", oauthToken);
        query = appendParameter(query, "tokenduration", tokenDuration);
        query = appendParameter(query, "tokenendpoint", tokenEndpoint);

        String auth = EMPTY;
        if (isNotBlank(username) && isNotBlank(password)) {
            auth = username + ':' + password + '@';
        }
        return protocol + "://" + auth + host + ':' + port + (isBlank(dbname) ? EMPTY : SLASH + dbname) + (isBlank(query) ? EMPTY : "?" + query);
    }

    /**
     * Appends a query parameter to an existing query string.
     * Only appends if the value is not blank.
     *
     * @param query the existing query string
     * @param parameterName the parameter name to append
     * @param value the parameter value to append
     * @return the updated query string with the appended parameter
     */
    private static String appendParameter(String query, String parameterName, String value) {
        if (isNotBlank(value)) {
            query += isBlank(query) ? EMPTY : '&';
            query += parameterName + "=" + value;
        }
        return query;
    }

    /**
     * Checks whether any items in the array of strings are null.
     *
     * @param args variable number of string arguments to check
     * @return {@code true} if any argument is null; {@code false} otherwise
     */
    public static boolean anyIsNull(String... args) {
        return Arrays.asList(args).contains(null);
    }

    /**
     * URL-encodes the given string if it is not already encoded.
     * Uses heuristic detection to determine if encoding is necessary.
     *
     * @param arg the string to potentially encode
     * @return the URL-encoded string, or the original value if already encoded
     */
    protected static String urlEncodeIfNecessary(String arg) {
        return isUrlEncoded(arg) ? arg : urlEncode(arg);
    }

    /**
     * Determines whether a string is URL-encoded.
     * Uses length comparison between the original and decoded values as a heuristic.
     *
     * @param arg the string to check
     * @return {@code true} if the string appears to be URL-encoded; {@code false} otherwise
     */
    public static boolean isUrlEncoded(String arg) {
        try {
            return arg.length() != urlDecode(arg).length();
        } catch (IllegalArgumentException ex) {
            return false;
        }
    }

    /**
     * URL-encodes the given value using UTF-8 encoding.
     *
     * @param arg the string to encode
     * @return the URL-encoded string
     * @throws AssertionError if UTF-8 encoding is not supported
     */
    protected static String urlEncode(String arg) {
        try {
            return URLEncoder.encode(arg, UTF_8);
        } catch (UnsupportedEncodingException ex) {
            throw new AssertionError(UTF_8_NOT_SUPPORTED, ex);
        }
    }

    /**
     * URL-decodes the given value using UTF-8 encoding.
     *
     * @param arg the string to decode
     * @return the URL-decoded string
     * @throws AssertionError if UTF-8 encoding is not supported
     */
    protected static String urlDecode(String arg) {
        try {
            return URLDecoder.decode(arg, UTF_8);
        } catch (UnsupportedEncodingException ex) {
            throw new AssertionError(UTF_8_NOT_SUPPORTED, ex);
        }
    }

	/**
	 * Parses a string containing port numbers and port ranges into a set of integers.
	 * Supports individual ports (e.g., "8080") and ranges (e.g., "8080-8090").
	 * Multiple values can be comma-separated. Ranges are automatically sorted.
	 *
	 * @param jobServerPort comma-separated string of port numbers and/or ranges (e.g., "8080,8090-8095")
	 * @return a set of all port numbers represented by the input string
	 */
	public static Set<Integer> parsePortRanges(String jobServerPort) {
		Set<Integer> jobServerPorts = new LinkedHashSet<>();
		if (isNotBlank(jobServerPort)) {
            for (String aSplitByComma : commaSeparatedValuesToList(jobServerPort)) {
                if (aSplitByComma.contains("-")) {
                    String[] splitByDash = aSplitByComma.split("\\s*-\\s*");
                    if (splitByDash.length == 2) {
                        int start = Integer.parseInt(splitByDash[0]);
                        int end = Integer.parseInt(splitByDash[1]);
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
