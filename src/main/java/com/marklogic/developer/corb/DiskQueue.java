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
  * *
  * * Code adapted from Bixio DiskQueue
  * * https://github.com/bixo/bixo/blob/master/src/main/java/bixo/utils/DiskQueue.java
  * * Original work Copyright 2009-2015 Scale Unlimited
  * * Modifications copyright (c) 2016 MarkLogic Corporation
  *
 */
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.IOUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.security.InvalidParameterException;
import java.text.MessageFormat;
import java.util.AbstractQueue;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.marklogic.developer.corb.util.StringUtils;

/**
 * A queue that writes extra elements to disk, and reads them in as needed.
 *
 * This implementation is optimized for being filled once (ie by the iterator in
 * a reducer) and then incrementally read. So it wouldn't work very well if
 * reads/writes were happening simultaneously, once anything had spilled to
 * disk.
 *
 * @param <E>
 */
public class DiskQueue<E extends Serializable> extends AbstractQueue<String> {

    private static final Logger LOG = Logger.getLogger(DiskQueue.class.getName());

    public static final float DEFAULT_REFILL_RATIO = 0.75f;

    // The memoryQueue represents the head of the queue. It can also be the tail,
    // if nothing has spilled over onto the disk.
    private MemoryQueue<E> memoryQueue;

    // Percentage of memory queue used/capacity that triggers a refill from disk.
    private float refillMemoryRatio;

    // Number of elements in the backing store file on disk.
    private int fileElementCount;

    private File tempDir;

    private BufferedWriter fileOut;
    private BufferedReader fileIn;

    // When moving elements from disk to memory, we don't know whether the memory
    // queue has space until the offer is rejected. So rather than trying to push
    // back an element into the file, just cache it in cachedElement.
    private String cachedElement;
    private File fileQueue;

    /**
     * Construct a disk-backed queue that keeps at most
     * {@code maxInMemorySize} elements in memory.
     *
     * @param maxInMemorySize Maximum number of elements to keep in memory.
     */
    public DiskQueue(int maxInMemorySize) {
        this(maxInMemorySize, null);
    }


    /**
     * Construct a disk-backed queue that keeps at most
     * {@code maxInMemorySize} elements in memory.
     *
     * @param maxInMemorySize Maximum number of elements to keep in memory.
     * @param tempDir Directory where queue temporary files will be written to.
     */
    public DiskQueue(int maxInMemorySize, File tempDir) {
        super();
        if (maxInMemorySize < 1) {
            throw new InvalidParameterException(DiskQueue.class.getSimpleName() + " max in-memory size must be at least one");
        }
        if (tempDir != null && !(tempDir.exists() && tempDir.isDirectory() && tempDir.canWrite())) {
            throw new InvalidParameterException(DiskQueue.class.getSimpleName() + " temporary directory must exist and be writable");
        }

        this.tempDir = tempDir;
        memoryQueue = new MemoryQueue<>(maxInMemorySize);
        refillMemoryRatio = DEFAULT_REFILL_RATIO;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     *
     * Close down streams, and toss the temp file.
     */
    @Override
    protected void finalize() throws Throwable {
        if (closeFile()) {
            LOG.log(Level.WARNING, () -> MessageFormat.format("{0} still had open file in finalize", DiskQueue.class.getSimpleName()));
        }
        super.finalize();
    }

    /**
     * Make sure the file streams are all closed down, the temp file is closed,
     * and the temp file has been deleted.
     *
     * @return true if we had to close down the file.
     */
    private boolean closeFile() {
        if (fileQueue == null) {
            return false;
        }

        IOUtils.closeQuietly(fileIn);
        fileIn = null;
        cachedElement = null;

        IOUtils.closeQuietly(fileOut);
        fileOut = null;

        fileElementCount = 0;

        fileQueue.delete();
        fileQueue = null;
        return true;
    }

    private void openFile() throws IOException {
        if (fileQueue == null) {
            fileQueue = File.createTempFile(DiskQueue.class.getSimpleName() + "-backingstore-", null, tempDir);
            fileQueue.deleteOnExit();
            LOG.log(Level.INFO, () -> MessageFormat.format("created backing store {0}", fileQueue.getAbsolutePath()));
            fileOut = Files.newBufferedWriter(fileQueue.toPath());

            // Flush output file, so there's something written when we open the input stream.
            fileOut.flush();

            fileIn = Files.newBufferedReader(fileQueue.toPath());
        }
    }

    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException(MessageFormat.format("Iterator is not supported for {0}", DiskQueue.class.getSimpleName()));
    }

    @Override
    public int size() {
        return memoryQueue.size() + fileElementCount + (cachedElement != null ? 1 : 0);
    }

    @Override
    public boolean offer(String element) {
        if (element == null) {
            throw new NullPointerException("Element cannot be null for AbstractQueue");
        }

        // If there's anything in the file, or the queue is full, then we have to write to the file.
        if (fileQueue != null || !memoryQueue.offer(element)) {
            try {
                openFile();
                fileOut.write(element);
                fileOut.newLine();
                fileElementCount += 1;
            } catch (IOException e) {
                LOG.log(Level.SEVERE, MessageFormat.format("Error writing to {0} backing store {1}", DiskQueue.class.getSimpleName(), fileQueue.getAbsolutePath()), e);
                return false;
            }
        }

        return true;
    }

    @Override
    public String peek() {
        loadMemoryQueue();
        return memoryQueue.peek();
    }

    @Override
    public String remove() {
        loadMemoryQueue();
        return memoryQueue.remove();
    }

    @Override
    public String poll() {
        loadMemoryQueue();
        return memoryQueue.poll();
    }

    /* (non-Javadoc)
     * @see java.util.AbstractQueue#clear()
     *
     * Implement faster clear (so AbstractQueue doesn't call poll() repeatedly)
     */
    @Override
    public void clear() {
        memoryQueue.clear();
        cachedElement = null;
        closeFile();
    }

    private void loadMemoryQueue() {
        // use the memory queue as our buffer, so only load it up when it's below capacity.
        if (memoryQueue.size() / (float) memoryQueue.getCapacity() >= refillMemoryRatio) {
            return;
        }

        // See if we have one saved element from the previous read request
        if (cachedElement != null && memoryQueue.offer(cachedElement)) {
            cachedElement = null;
        }

        // Now see if we have anything on disk
        if (fileQueue != null) {
            try {
                // Since we buffer writes, we need to make sure everything has
                // been written before we start reading.
                fileOut.flush();

                while (fileElementCount > 0) {
                    @SuppressWarnings("unchecked")
                    String nextFileElement = fileIn.readLine();
                    fileElementCount -= 1;

                    if (!StringUtils.isEmpty(nextFileElement) && !memoryQueue.offer(nextFileElement)) {
                        //memory queue is full. Cache this entry and jump out
                        cachedElement = nextFileElement;
                        return;
                    }
                }

                // Nothing left in the file, so close/delete it.
                closeFile();

                // file queue is empty, so could reset length of file, read/write offsets
                // to start from zero instead of closing file (but for current use case of fill once, drain
                // once this works just fine)
            } catch (IOException e) {
                LOG.log(Level.SEVERE, MessageFormat.format("Error reading from {0} backing store", DiskQueue.class.getSimpleName()), e);
            }
        }
    }

    private static class MemoryQueue<E> extends AbstractQueue<String> {

        private final List<String> queue;
        private final int capacity;

        public MemoryQueue(int capacity) {
            super();
            this.capacity = capacity;
            queue = new ArrayList<>(capacity);
        }

        @Override
        public Iterator<String> iterator() {
            return queue.iterator();
        }

        public int getCapacity() {
            return capacity;
        }

        @Override
        public int size() {
            return queue.size();
        }

        @Override
        public boolean offer(String o) {
            if (o == null) {
                throw new NullPointerException();
            } else if (queue.size() >= capacity) {
                return false;
            } else {
                queue.add(o);
                return true;
            }
        }

        @Override
        public String peek() {
            if (queue.isEmpty()) {
                return null;
            } else {
                return queue.get(0);
            }
        }

        @Override
        public String poll() {
            if (queue.isEmpty()) {
                return null;
            } else {
                return queue.remove(0);
            }
        }

        @Override
        public String remove() {
            return queue.remove(0);
        }
    }

}
