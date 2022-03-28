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
package com.marklogic.developer.corb;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 * @param <E>
 */
public class ArrayQueue<E> extends AbstractQueue<String> implements Serializable {

    private static final long serialVersionUID = -1;
    // char primitives use less memory than strings, and arrays use less memory than lists or queues
    private final char[][] queue;
    private int count = 0;
    private int takeIndex;
    private int putIndex;

    ArrayQueue(int capacity) {
        super();
        if (capacity < 0) {
            throw new IllegalArgumentException();
        }
        takeIndex = 0;
        queue = new char[capacity][];
    }

    @Override
    public int size() {
        return count;
    }

    @Override
    public String peek() {
        return (count == 0) ? null : new String(queue[takeIndex]);
    }

    @Override
    public String poll() {
        if (count == 0) {
            return null;
        }
        return extract();
    }

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

    private void insert(String x) {
        queue[putIndex] = x.toCharArray();
        putIndex = increment(putIndex);
        ++count;
    }

    protected int increment(int index) {
        int i = index;
        return (++i == queue.length) ? 0 : i;
    }

    @Override
    public Iterator<String> iterator() {
        return new Itr();
    }

    private String extract() {
        String x = new String(queue[takeIndex]);
        queue[takeIndex] = null;
        takeIndex = increment(takeIndex);
        --count;
        return x;
    }

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

    private class Itr implements Iterator<String> {

        private int nextIndex;
        private String nextItem;
        private int lastReturnedIndex;

        Itr() {
            lastReturnedIndex = -1;
            if (count == 0) {
                nextIndex = -1;
            } else {
                nextIndex = takeIndex;
                nextItem = new String(queue[takeIndex]);
            }
        }

        @Override
        public boolean hasNext() {
            return nextIndex >= 0;
        }

        /**
         * Checks whether nextIndex is valid; if so setting nextItem. Stops
         * iterator when either hits putIndex or sees null item.
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
