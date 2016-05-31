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

import java.util.Iterator;
import java.util.NoSuchElementException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class ArrayQueueTest {

    /**
     * Test of size method, of class ArrayQueue.
     */
    @Test
    public void testSize() {
        ArrayQueue<String> instance = new ArrayQueue<String>(10);
        assertEquals(0, instance.size());

        instance.add("foo");
        assertEquals(1, instance.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSizeNegativeInit() {
        new ArrayQueue<String>(-1);
        fail();
    }

    /**
     * Test of peek method, of class ArrayQueue.
     */
    @Test
    public void testPeek() {
        ArrayQueue<String> instance = new ArrayQueue<String>(10);

        String result = instance.peek();
        assertNull(result);

        instance.add("foo");
        assertEquals("foo", instance.peek());
    }

    /**
     * Test of poll method, of class ArrayQueue.
     */
    @Test
    public void testPoll() {
        ArrayQueue<String> instance = new ArrayQueue<String>(1);

        String result = instance.poll();
        assertNull(result);
        instance.add("foo");
        assertEquals("foo", instance.poll());
        assertTrue(instance.isEmpty());
    }

    /**
     * Test of offer method, of class ArrayQueue.
     */
    @Test
    public void testOffer() {
        ArrayQueue<String> instance = new ArrayQueue<String>(1);

        assertTrue(instance.offer("foo"));
        assertFalse(instance.offer("foo"));
    }

    /**
     * Test of increment method, of class ArrayQueue.
     */
    @Test
    public void testIncrement() {
        int i = 0;
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        assertEquals(0, instance.size());
        assertEquals(1, instance.increment(i));
        assertEquals(1, instance.increment(i));
        instance.add("foo");
        assertEquals(1, instance.size());
        assertEquals(1, instance.increment(i));
    }

    /**
     * Test of iterator method, of class ArrayQueue.
     */
    @Test
    public void testIterator() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.add("foo");
        instance.add("bar");
        Iterator<String> iterator = instance.iterator();
        while (iterator.hasNext()) {
            assertNotNull(iterator.next());
        }
    }

    @Test
    public void testIteratorEmpty() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        Iterator<String> iterator = instance.iterator();
        assertFalse(iterator.hasNext());
    }

    /**
     * Test of removeAt method, of class ArrayQueue.
     */
    @Test
    public void testRemoveAt() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.add("foo");
        instance.add("bar");
        instance.removeAt(0);
        assertFalse(instance.isEmpty());
        assertEquals("bar", instance.peek());
        instance.removeAt(0);
        assertTrue(instance.isEmpty());
    }

    /**
     * Test of removeAt method, of class ArrayQueue.
     */
    @Test
    public void testRemove() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.add("foo");
        instance.add("bar");
        Iterator<String> iterator = instance.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        assertTrue(instance.isEmpty());
    }

    /**
     * Test of removeAt method, of class ArrayQueue.
     */
    @Test(expected = IllegalStateException.class)
    public void testRemoveTwice() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.add("foo");
        instance.add("bar");
        Iterator<String> iterator = instance.iterator();
        iterator.next();
        iterator.remove();
        iterator.remove();
        fail();
    }

    /**
     * Test of next() method, of class ArrayQueue
     */
    @Test(expected = NoSuchElementException.class)
    public void testNextWhenEmpty() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.iterator().next();
        fail();
    }
    
    /**
     * Test of offer() method, of class ArrayQueue
     */
    @Test (expected = NullPointerException.class)
    public void testOfferNull() {
        ArrayQueue<String> instance = new ArrayQueue<String>(2);
        instance.offer(null);
        fail();
    }
}
