/*
  * * Copyright 2005-2015 MarkLogic Corporation
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

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class DateUtilsTest {

    final Date exampleDate;

    public DateUtilsTest() {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(2012, 3, 8, 11, 22, 34);
        cal.set(Calendar.MILLISECOND, 0);
        exampleDate = cal.getTime();
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of parseDateTime method, of class Utilities.
     *
     * @throws java.lang.Exception
     */
    @org.junit.Test
    public void testParseDateTime() throws Exception {
        System.out.println("parseDateTime");
        Date result = DateUtils.parseDateTime("2012-04-08T11:22:34+00:00");
        assertEquals(exampleDate, result);
    }

    @org.junit.Test
    public void testParseDateTime_00() throws Exception {
        System.out.println("parseDateTime");
        Date result = DateUtils.parseDateTime("2012-04-08T11:22:34+0000");
        assertEquals(exampleDate, result);
    }

    @org.junit.Test(expected = ParseException.class)
    public void testParseDateTime_BadDate() throws Exception {
        System.out.println("parseDateTime");
        DateUtils.parseDateTime("10/27/2010");
    }

    /**
     * Test of formatDateTime method, of class Utilities.
     *
     * @throws java.text.ParseException
     */
    @org.junit.Test
    public void testFormatDateTime_0args() throws ParseException {
        System.out.println("formatDateTime");
        String formattedDateNow = DateUtils.formatDateTime();
        String roundTrip = DateUtils.formatDateTime(DateUtils.parseDateTime(formattedDateNow));
        assertEquals(roundTrip, formattedDateNow);
    }

    /**
     * Test of formatDateTime method, of class Utilities.
     *
     * @throws java.text.ParseException
     */
    @org.junit.Test
    public void testFormatDateTime_Date() throws ParseException {
        System.out.println("formatDateTime");
        String formattedExampleDate = DateUtils.formatDateTime(exampleDate);
        String roundTrip = DateUtils.formatDateTime(DateUtils.parseDateTime(formattedExampleDate));
        assertEquals(roundTrip, formattedExampleDate);
    }

    @org.junit.Test
    public void testFormatDateTime_DateIsNull() throws ParseException {
        System.out.println("formatDateTime");
        String formattedExampleDate1 = DateUtils.formatDateTime(null);
        String formattedExampleDate2 = DateUtils.formatDateTime(null);
        assertTrue(formattedExampleDate1.compareTo(formattedExampleDate2) >= 0);
    }
}
