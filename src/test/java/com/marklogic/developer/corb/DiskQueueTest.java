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
package com.marklogic.developer.corb;

import com.marklogic.developer.TestHandler;
import java.io.File;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class DiskQueueTest {

    private final TestHandler testLogger = new TestHandler();
    private static final Logger logger = Logger.getLogger(DiskQueue.class.getName());

    public DiskQueueTest() {
    }

    @BeforeClass
    public static void setUpClass() {
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Before
    public void setUp() {
        logger.addHandler(testLogger);
    }

    @After
    public void tearDown() {
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueue_sizeTooSmall() {
        Queue<String> instance = new DiskQueue<String>(0);
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueue_tempDirNotDir() throws IOException {
        File tmpFile = File.createTempFile("tmp", "txt");
        Queue<String> instance = new DiskQueue<String>(0, tmpFile);
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueue_tempFileDoesNotExist() throws IOException {
        File tmpFile = new File("/does/not/exist");
        Queue<String> instance = new DiskQueue<String>(0, tmpFile);
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueue_tempDirIsNull() throws IOException {
        File tmpFile = null;
        Queue<String> instance = new DiskQueue<String>(0, tmpFile);
    }
    
     @Test(expected = InvalidParameterException.class)
    public void testDiskQueue_tempDirDoesNotExist() throws IOException {
        File tmpFile = TestUtils.createTempDirectory();
        tmpFile.delete();
        Queue<String> instance = new DiskQueue<String>(0, tmpFile);
    }


    @Test
    public void testDiskQueue_customComparator() throws IOException {
        Queue<String> instance = new DiskQueue<String>(1, String.CASE_INSENSITIVE_ORDER);
    }

    @Test
    public void testDiskQueue_finalizeWhileOpen() throws IOException, Throwable {
        DiskQueue<String> instance = new DiskQueue<String>(1);
        instance.add("one");
        instance.add("two");
        instance.add("three");
        assertEquals(3, instance.size());
        instance.finalize();
        List<LogRecord> records = testLogger.getLogRecords();
        assertTrue(TestUtils.containsLogRecord(records,
                new LogRecord(Level.WARNING,
                        MessageFormat.format("{0} still had open file in finalize", DiskQueue.class.getSimpleName()))));
    }

    @Test
    public void testDiskQueue_loadFromFile() throws IOException, Throwable {
        Queue<String> instance = new DiskQueue<String>(1);
        assertEquals(0, instance.size());
        instance.add("one");
        assertEquals(1, instance.size());
        instance.add("two");
        assertEquals(2, instance.size());
        instance.add("three");
        assertEquals(3, instance.size());
        assertEquals("one", instance.remove());
        assertEquals("two", instance.remove());
        assertEquals("three", instance.remove());
        assertEquals(0, instance.size());
    }

    /**
     * Test of finalize method, of class DiskQueue.
     */
    @Test
    public void testFinalize() throws Exception {
        System.out.println("finalize");
        DiskQueue<String> instance = new DiskQueue<String>(1);
        try {
            instance.finalize();
        } catch (Throwable ex) {
            fail();
        }
    }

    /**
     * Test of iterator method, of class DiskQueue.
     */
    @Test(expected = RuntimeException.class)
    public void testIterator() {
        System.out.println("iterator");
        Queue<String> instance = new DiskQueue<String>(1);
        instance.iterator();
    }

    /**
     * Test of size method, of class DiskQueue.
     */
    @Test
    public void testSize() {
        System.out.println("size");
        Queue<String> instance = new DiskQueue<String>(1);
        assertEquals(0, instance.size());
        instance.add("test");
        int result = instance.size();
        assertEquals(1, result);
    }

    /**
     * Test of offer method, of class DiskQueue.
     */
    @Test
    public void testOffer() {
        System.out.println("offer");
        Object element = null;
        Queue<String> instance = new DiskQueue<String>(1);
        boolean result = instance.offer("test1");
        assertTrue(result);
    }

    @Test(expected = NullPointerException.class)
    public void testOffer_null() {
        System.out.println("offer");
        Object element = null;
        Queue<String> instance = new DiskQueue<String>(1);
        instance.offer(null);
    }

    /**
     * Test of peek method, of class DiskQueue.
     */
    @Test
    public void testPeek() {
        System.out.println("peek");
        Queue<String> instance = new DiskQueue<String>(1);
        assertNull(instance.peek());
        String item = "test";
        instance.add(item);
        assertEquals(item, instance.peek());
    }

    /**
     * Test of remove method, of class DiskQueue.
     */
    @Test
    public void testRemove() {
        System.out.println("remove");
        Queue<String> instance = new DiskQueue<String>(1);
        String element = "test";
        instance.add(element);
        String result = instance.remove();
        assertEquals(element, result);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRemove_whenEmpty() {
        System.out.println("remove");
        Queue<String> instance = new DiskQueue<String>(1);
        instance.remove();
    }

    /**
     * Test of poll method, of class DiskQueue.
     */
    @Test
    public void testPoll() {
        System.out.println("poll");
        Queue<String> instance = new DiskQueue<String>(1);
        String element = "test";
        instance.add(element);
        Object result = instance.poll();
        assertEquals(element, result);
    }

    @Test
    public void testPoll_whenEmpty() {
        System.out.println("poll");
        DiskQueue<String> instance = new DiskQueue<String>(1);
        Object result = instance.poll();
        assertNull(result);
    }

    /**
     * Test of clear method, of class DiskQueue.
     */
    @Test
    public void testClear() {
        System.out.println("clear");
        Queue<String> instance = new DiskQueue<String>(1);
        instance.add("test");
        instance.clear();
        assertEquals(0, instance.size());
    }

}
