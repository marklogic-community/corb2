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
package com.marklogic.developer.corb;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class ArrayQueueTest {

    private static final String FOO = "foo";
    private static final String BAR = "bar";

    @Test
    void testSize() {
        Queue<String> instance = new ArrayQueue<>(10);
        assertEquals(0, instance.size());

        instance.add(FOO);
        assertEquals(1, instance.size());
    }

    @Test
    void testSizeNegativeInit() {
        assertThrows(IllegalArgumentException.class, () -> new ArrayQueue<>(-1));
    }

    @Test
    void testPeek() {
        Queue<String> instance = new ArrayQueue<>(10);

        String result = instance.peek();
        assertNull(result);

        instance.add(FOO);
        assertEquals(FOO, instance.peek());
    }

    @Test
    void testPoll() {
        Queue<String> instance = new ArrayQueue<>(1);

        String result = instance.poll();
        assertNull(result);
        instance.add(FOO);
        assertEquals(FOO, instance.poll());
        assertTrue(instance.isEmpty());
    }

    @Test
    void testOffer() {
        Queue<String> instance = new ArrayQueue<>(1);

        assertTrue(instance.offer(FOO));
        assertFalse(instance.offer(FOO));
    }

    @Test
    void testIncrement() {
        int i = 0;
        ArrayQueue<String> instance = new ArrayQueue<>(2);
        assertEquals(0, instance.size());
        assertEquals(1, instance.increment(i));
        assertEquals(1, instance.increment(i));
        instance.add(FOO);
        assertEquals(1, instance.size());
        assertEquals(1, instance.increment(i));
    }

    @Test
    void testIterator() {
        ArrayQueue<String> instance = new ArrayQueue<>(2);
        instance.add(FOO);
        instance.add(BAR);
        for (String anInstance : instance) {
            assertNotNull(anInstance);
        }
    }

    @Test
    void testIteratorEmpty() {
        Queue<String> instance = new ArrayQueue<>(2);
        Iterator<String> iterator = instance.iterator();
        assertFalse(iterator.hasNext());
    }

    @Test
    void testRemoveAt() {
        ArrayQueue<String> instance = new ArrayQueue<>(2);
        instance.add(FOO);
        instance.add(BAR);
        instance.removeAt(0);
        assertFalse(instance.isEmpty());
        assertEquals(BAR, instance.peek());
        instance.removeAt(0);
        assertTrue(instance.isEmpty());
    }

    @Test
    void testRemove() {
        Queue<String> instance = new ArrayQueue<>(2);
        instance.add(FOO);
        instance.add(BAR);
        Iterator<String> iterator = instance.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
        assertTrue(instance.isEmpty());
    }

    @Test
    void testRemoveTwice() {
        Queue<String> instance = new ArrayQueue<>(2);
        instance.add(FOO);
        instance.add(BAR);
        Iterator<String> iterator = instance.iterator();
        iterator.next();
        iterator.remove();
        assertThrows(IllegalStateException.class, iterator::remove);
    }

    @Test
    void testNextWhenEmpty() {
        Queue<String> instance = new ArrayQueue<>(2);
        assertThrows(NoSuchElementException.class, () -> instance.iterator().next());
    }

    @Test
    void testOfferNull() {
        Queue<String> instance = new ArrayQueue<>(2);
        assertThrows(NullPointerException.class, () -> instance.offer(null));
    }
}
