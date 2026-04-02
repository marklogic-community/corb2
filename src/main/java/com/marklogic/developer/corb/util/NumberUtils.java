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

import java.math.BigDecimal;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for number conversion operations.
 * Provides safe conversion methods that handle null values and parsing failures gracefully.
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class NumberUtils {

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private NumberUtils() {
    }

    /**
     * <p>Convert a {@code String} to an {@code int}, returning
     * {@code zero} if the conversion fails.</p>
     *
     * <p>If the string is {@code null}, {@code zero} is returned.</p>
     *
     * @param val the string to convert, may be null
     * @return the int represented by the string, or {@code zero} if the
     * conversion fails
     */
    public static int toInt(String val) {
        return toInt(val, 0);
    }

    /**
     * <p>Convert a {@code String} to an {@code int}, returning a default
     * value if the conversion fails.</p>
     *
     * <p>If the string is {@code null}, the default value is returned.</p>
     *
     * @param val the string to convert, may be null
     * @param defaultValue the default value to return if conversion fails
     * @return the int represented by the string, or the default if conversion
     * fails or the input is null
     */
    public static int toInt(String val, int defaultValue) {
        if (val == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(val);
        } catch (final NumberFormatException nfe) {
            return defaultValue;
        }
    }

    /**
     * <p>Parses a size string and converts it to the equivalent number of bytes.</p>
     *
     * <p>The size string should consist of a numeric value followed by an optional unit.
     * Whitespace between the number and unit is allowed. The parsing is case-insensitive.</p>
     *
     * <p>Supported units (using binary/base-2 multipliers):</p>
     * <ul>
     *   <li>B - Bytes (multiplier: 1)</li>
     *   <li>K, KB, KiB - Kilobytes (multiplier: 1,024)</li>
     *   <li>M, MB, MiB - Megabytes (multiplier: 1,048,576)</li>
     *   <li>G, GB, GiB - Gigabytes (multiplier: 1,073,741,824)</li>
     *   <li>T, TB, TiB - Terabytes (multiplier: 1,099,511,627,776)</li>
     * </ul>
     *
     * <p>If no unit is specified, the value is assumed to be in bytes.</p>
     *
     * <p>Examples:</p>
     * <ul>
     *   <li>{@code "1024"} returns 1024</li>
     *   <li>{@code "10 KB"} returns 10240</li>
     *   <li>{@code "2.5 MB"} returns 2621440</li>
     *   <li>{@code "5GB"} returns 5368709120</li>
     * </ul>
     *
     * @param size the size string to parse, must not be null
     * @return the size in bytes as a {@code long}
     * @throws NumberFormatException if the size string cannot be parsed, contains an invalid
     *         numeric value, or uses an unsupported unit
     */
    public static long parseSize(String size) throws NumberFormatException {
        if (size == null) {
            return 0L;
        }
        // Use a regex to capture the number part and the unit part (case-insensitive)
        Pattern pattern = Pattern.compile("([\\d.]+)\\s*([a-zA-Z]+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(size.trim());

        if (matcher.find()) {
            String numberPart = matcher.group(1);
            String unitPart = matcher.group(2).toUpperCase();

            BigDecimal bytes = new BigDecimal(numberPart);
            long multiplier;

            switch (unitPart) {
                case "B":
                    multiplier = 1L;
                    break;
                case "K":
                case "KB":
                case "KIB": // KiB is 1024, KB can be 1000 or 1024. Using 1024 (binary) here.
                    multiplier = 1024L;
                    break;
                case "M":
                case "MB":
                case "MIB":
                    multiplier = 1024L * 1024L;
                    break;
                case "G":
                case "GB":
                case "GIB":
                    multiplier = 1024L * 1024L * 1024L;
                    break;
                case "T":
                case "TB":
                case "TIB":
                    multiplier = 1024L * 1024L * 1024L * 1024L;
                    break;
                default:
                    throw new NumberFormatException("Unknown size unit: " + unitPart);
            }

            // Use BigDecimal for precise multiplication, then convert to a long (bytes)
            return bytes.multiply(BigDecimal.valueOf(multiplier)).longValue();
        } else {
            // Handle cases with no unit, assuming bytes
            try {
                return Long.parseLong(size.trim());
            } catch (NumberFormatException e) {
                throw new NumberFormatException("Cannot parse size: " + size);
            }
        }
    }
}
