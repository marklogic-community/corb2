/*
  * * Copyright (c) 2004-2023 MarkLogic Corporation
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
import java.lang.reflect.Field;
import java.nio.charset.Charset;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class DiskQueueTest {

    private final TestHandler testLogger = new TestHandler();
    private static final Logger LOG = Logger.getLogger(DiskQueue.class.getName());

    @Before
    public void setUp() {
        LOG.addHandler(testLogger);
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueueSizeTooSmall() {
        new DiskQueue<>(0);
        fail();
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueueTempDirNotDir() {
        try {
            File tmpFile = File.createTempFile("tmp", "txt");
            new DiskQueue<>(0, tmpFile);

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueueTempFileDoesNotExist() {
        File tmpFile = new File("/does/not/exist");
        new DiskQueue<>(0, tmpFile);
        fail();
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueueTempDirIsNull() {
        File tmpFile = null;
        new DiskQueue<>(0, tmpFile);
        fail();
    }

    @Test(expected = InvalidParameterException.class)
    public void testDiskQueueTempDirDoesNotExist() {
        try {
            File tmpFile = TestUtils.createTempDirectory();
            if (tmpFile.delete()) {
                new DiskQueue<>(0, tmpFile);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
        }
        fail();
    }

    @Test
    public void testDiskQueueFinalizeWhileOpen() {
        try {
            DiskQueue<String> instance = new DiskQueue<>(1);
            instance.add("first");
            instance.add("second");
            instance.add("third");
            assertEquals(3, instance.size());
            instance.finalize();
            List<LogRecord> records = testLogger.getLogRecords();
            assertTrue(TestUtils.containsLogRecord(records,
                    new LogRecord(Level.WARNING,
                            MessageFormat.format("{0} still had open file in finalize", DiskQueue.class.getSimpleName()))));
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testDiskQueueUTF8() {

        String val = "em‐dash";
        try {

            System.setProperty("file.encoding","cp1252");
            Field charset = Charset.class.getDeclaredField("defaultCharset");
            charset.setAccessible(true);
            charset.set(null,null);

            DiskQueue<String> instance = new DiskQueue<>(1);
            instance.add(val);
            instance.add(val);
            //this one would have used the memoryqueue
            String result = instance.remove();
            assertEquals(val, result);
            //this one would have used the diskqueue
            result = instance.remove();
            assertEquals(val, result);
            instance.finalize();

        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testDiskQueueLoadFromFile() {
        String one = "one";
        String two = "two";
        String three = "three";
        Queue<String> instance = new DiskQueue<>(1);
        assertEquals(0, instance.size());
        instance.add(one);
        assertEquals(1, instance.size());
        instance.add(two);
        assertEquals(2, instance.size());
        instance.add(three);
        assertEquals(3, instance.size());
        assertEquals(one, instance.remove());
        assertEquals(two, instance.remove());
        assertEquals(three, instance.remove());
        assertEquals(0, instance.size());
    }

    @Test
    public void testFinalize() {
        DiskQueue<String> instance = new DiskQueue<>(1);
        try {
            instance.finalize();
        } catch (Throwable ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test(expected = RuntimeException.class)
    public void testIterator() {
        Queue<String> instance = new DiskQueue<>(1);
        instance.iterator();
        fail();
    }

    @Test
    public void testSize() {
        Queue<String> instance = new DiskQueue<>(1);
        assertEquals(0, instance.size());
        instance.add("testSize");
        int result = instance.size();
        assertEquals(1, result);
    }

    @Test
    public void testOffer() {
        Queue<String> instance = new DiskQueue<>(1);
        boolean result = instance.offer("test1");
        assertTrue(result);
    }

    @Test(expected = NullPointerException.class)
    public void testOfferNull() {
        Queue<String> instance = new DiskQueue<>(1);
        instance.offer(null);
        fail();
    }

    @Test
    public void testPeek() {
        Queue<String> instance = new DiskQueue<>(1);
        assertNull(instance.peek());
        String item = "testPeek";
        instance.add(item);
        assertEquals(item, instance.peek());
    }

    @Test
    public void testRemove() {
        Queue<String> instance = new DiskQueue<>(1);
        String element = "testRemove";
        instance.add(element);
        String result = instance.remove();
        assertEquals(element, result);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testRemoveWhenEmpty() {
        Queue<String> instance = new DiskQueue<>(1);
        instance.remove();
        fail();
    }

    @Test
    public void testPoll() {
        Queue<String> instance = new DiskQueue<>(1);
        String element = "testPoll";
        instance.add(element);
        Object result = instance.poll();
        assertEquals(element, result);
    }

    @Test
    public void testPollWhenEmpty() {
        Queue<String> instance = new DiskQueue<>(1);
        Object result = instance.poll();
        assertNull(result);
    }

    @Test
    public void testClear() {
        Queue<String> instance = new DiskQueue<>(1);
        instance.add("testClear");
        instance.clear();
        assertEquals(0, instance.size());
    }

}
