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

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public final class NumberUtils {

    private NumberUtils() {
    }

    /**
     * <p>Convert a {@code String} to an <code>int</code>, returning
     * {@code zero} if the conversion fails.</p>
     *
     * <p>If the string is {@code null}, <code>zero</code> is returned.</p>
     *
     * @param val
     * @return the int represented by the string,or {@code zero} if the
     * conversion fails
     */
    public static int toInt(String val) {
        return toInt(val, 0);
    }

    /**
     * <p>Convert a {@code String} to an <code>int</code>, returning a default
     * value if the conversion fails.</p>
     *
     * <p>If the string is {@code null}, the default value is returned.</p>
     *
     * @param val the string to convert, may be null
     * @param defaultValue the default value
     * @return the int represented by the string, or the default if conversion
     * fails
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
}
