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

import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A memory-optimized, fixed-capacity, circular queue implementation for String elements.
 * This queue uses a backing array of char arrays instead of String objects to reduce
 * memory overhead. Strings are converted to char arrays when inserted and reconstructed
 * from char arrays when retrieved.
 *
 * <p>Key characteristics:</p>
 * <ul>
 *   <li>Fixed capacity - specified at construction time</li>
 *   <li>Circular/ring buffer implementation using head and tail indices</li>
 *   <li>Memory optimized - char primitives use less memory than String objects</li>
 *   <li>Non-blocking - returns false when full, null when empty</li>
 *   <li>Does not permit null elements</li>
 *   <li>Serializable</li>
 * </ul>
 *
 * <p>This implementation is particularly useful for large queues of URI strings
 * where memory efficiency is important.</p>
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @param <E> the type parameter (note: implementation is specialized for String)
  * @since 2.3.1
 */
public class ArrayQueue<E> extends AbstractQueue<String> implements Serializable {

    /** Serialization version identifier */
    private static final long serialVersionUID = -1;
    /** Circular buffer storing elements as char arrays for memory efficiency */
    private final char[][] queue;
    /** The number of elements currently in the queue */
    private int count = 0;
    /** Index of the next element to be dequeued (head of queue) */
    private int takeIndex;
    /** Index where the next element will be enqueued (tail of queue) */
    private int putIndex;

    /**
     * Creates an ArrayQueue with the specified capacity.
     * The queue is implemented as a fixed-size circular buffer.
     *
     * @param capacity the maximum number of elements the queue can hold
     * @throws IllegalArgumentException if capacity is negative
     */
    ArrayQueue(int capacity) {
        super();
        if (capacity < 0) {
            throw new IllegalArgumentException();
        }
        takeIndex = 0;
        queue = new char[capacity][];
    }

    /**
     * Returns the number of elements currently in the queue.
     *
     * @return the number of elements in the queue
     */
    @Override
    public int size() {
        return count;
    }

    /**
     * Retrieves, but does not remove, the head of this queue.
     * Returns null if the queue is empty.
     *
     * @return the head of the queue, or null if empty
     */
    @Override
    public String peek() {
        return (count == 0) ? null : new String(queue[takeIndex]);
    }

    /**
     * Retrieves and removes the head of this queue.
     * Returns null if the queue is empty.
     *
     * @return the head of the queue, or null if empty
     */
    @Override
    public String poll() {
        if (count == 0) {
            return null;
        }
        return extract();
    }

    /**
     * Inserts the specified element into this queue if possible.
     * This implementation returns false if the queue is at capacity.
     *
     * @param element the element to add
     * @return true if the element was added, false if the queue is full
     * @throws NullPointerException if the specified element is null
     */
    @Override
    public boolean offer(String element) {
        if (element == null) {
            throw new NullPointerException();
        }
        if (count >= queue.length) {
            return false;
        } else {
            insert(element);
            return true;
        }
    }

    /**
     * Inserts an element at the current put position.
     * Converts the String to a char array for memory efficiency.
     * Advances the put index in a circular manner.
     *
     * @param x the String to insert
     */
    private void insert(String x) {
        queue[putIndex] = x.toCharArray();
        putIndex = increment(putIndex);
        ++count;
    }

    /**
     * Increments the given index in a circular fashion.
     * Wraps around to 0 when reaching the end of the queue array.
     *
     * @param index the index to increment
     * @return the next index, wrapping to 0 if at the end
     */
    protected int increment(int index) {
        int i = index;
        return (++i == queue.length) ? 0 : i;
    }

    /**
     * Returns an iterator over the elements in this queue.
     * The iterator does not guarantee any particular order beyond
     * traversing from the current take position.
     *
     * @return an iterator over the elements in this queue
     */
    @Override
    public @NotNull Iterator<String> iterator() {
        return new Itr();
    }

    /**
     * Extracts and removes the element at the current take position.
     * Converts the char array back to a String.
     * Advances the take index in a circular manner and decrements the count.
     *
     * @return the extracted String element
     */
    private String extract() {
        String x = new String(queue[takeIndex]);
        queue[takeIndex] = null;
        takeIndex = increment(takeIndex);
        --count;
        return x;
    }

    /**
     * Removes the element at the specified index.
     * If removing the front element, simply advances the take index.
     * Otherwise, slides all subsequent elements forward to fill the gap.
     * This maintains the circular buffer structure.
     *
     * @param index the index of the element to remove
     */
    protected void removeAt(int index) {
        int i = index;
        final char[][] items = this.queue;
        // if removing front item, just advance
        if (i == takeIndex) {
            items[takeIndex] = null;
            takeIndex = increment(takeIndex);
        } else {
            // slide over all others up through putIndex.
            for (;;) {
                int nextIndex = increment(i);
                if (nextIndex != putIndex) {
                    items[i] = items[nextIndex];
                    i = nextIndex;
                } else {
                    items[i] = null;
                    putIndex = i;
                    break;
                }
            }
        }
        --count;
    }

    /**
     * Iterator implementation for traversing the ArrayQueue.
     * Supports element removal during iteration.
     */
    private class Itr implements Iterator<String> {

        private int nextIndex;
        private String nextItem;
        private int lastReturnedIndex;

        /**
         * Constructs an iterator starting at the current take position.
         * Sets nextIndex to -1 if the queue is empty.
         */
        Itr() {
            lastReturnedIndex = -1;
            if (count == 0) {
                nextIndex = -1;
            } else {
                nextIndex = takeIndex;
                nextItem = new String(queue[takeIndex]);
            }
        }

        /**
         * Returns true if the iteration has more elements.
         *
         * @return true if there are more elements to iterate
         */
        @Override
        public boolean hasNext() {
            return nextIndex >= 0;
        }

        /**
         * Validates and updates the next index and item.
         * Stops iteration when reaching the put position or encountering a null element.
         * This method is called after advancing nextIndex to check if it's still valid.
         */
        private void checkNext() {
            if (nextIndex == putIndex) {
                nextIndex = -1;
                nextItem = null;
            } else {
                char[] item = queue[nextIndex];
                if (item == null) {
                    nextIndex = -1;
                } else {
                    nextItem = new String(item);
                }
            }
        }

        /**
         * Returns the next element in the iteration.
         *
         * @return the next String element
         * @throws NoSuchElementException if the iteration has no more elements
         */
        @Override
        public String next() {

            if (nextIndex < 0) {
                throw new NoSuchElementException();
            }
            lastReturnedIndex = nextIndex;
            String x = nextItem;
            nextIndex = increment(nextIndex);
            checkNext();
            return x;
        }

        /**
         * Removes the last element returned by this iterator from the queue.
         * This method can be called only once per call to {@link #next()}.
         * Adjusts the iterator position to account for the removed element.
         *
         * @throws IllegalStateException if next has not been called, or remove has already been called after the last call to next
         */
        @Override
        public void remove() {
            int i = lastReturnedIndex;
            if (i == -1) {
                throw new IllegalStateException();
            }
            lastReturnedIndex = -1;

            int ti = takeIndex;
            removeAt(i);
            // back up cursor (reset to front if was first element)
            nextIndex = (i == ti) ? takeIndex : i;
            checkNext();
        }
    }
}
