/*
  * * Copyright (c) 2004-2015 MarkLogic Corporation
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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class DateUtils {

    public static final DateFormat ISO_8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

    private DateUtils() {
    }

    /**
     *
     * @param date
     * @return
     * @throws ParseException
     */
    public static Date parseDateTime(String date) throws ParseException {
        synchronized (ISO_8601) {
            return ISO_8601.parse(date.replaceFirst(":(\\d\\d)$", "$1"));
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
        synchronized (ISO_8601) {
            dateStr = ISO_8601.format(date);
        }
        // remap the timezone from 0000 to 00:00 (starts at char 22)
        return dateStr.substring(0, 22) + ":" + dateStr.substring(22);
    }

}
