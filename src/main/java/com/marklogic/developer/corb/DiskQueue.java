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
 * A disk-backed queue implementation that maintains a fixed-size memory buffer and
 * automatically spills overflow elements to a temporary file on disk.
 * <p>
 * This implementation extends {@link AbstractQueue} and provides a hybrid storage approach:
 * </p>
 * <ul>
 *   <li>Keeps up to {@code maxInMemorySize} elements in memory for fast access</li>
 *   <li>Automatically writes overflow elements to a temporary file on disk</li>
 *   <li>Transparently loads elements from disk back into memory as space becomes available</li>
 * </ul>
 * <p>
 * <b>Performance Characteristics:</b><br>
 * This implementation is optimized for write-once, read-incrementally patterns (such as
 * filling the queue from an iterator, then draining it). It is not well-suited for
 * concurrent read/write operations once elements have been spilled to disk.
 * </p>
 * <p>
 * <b>Thread Safety:</b><br>
 * This class is not thread-safe. External synchronization is required if accessed
 * from multiple threads.
 * </p>
 * <p>
 * <b>Memory Management:</b><br>
 * The in-memory portion is refilled from disk when it drops below a configurable
 * threshold (default 75% of capacity). A single element cache is maintained to
 * handle cases where the memory queue is full during refill operations.
 * </p>
 *
 * @param <E> the type of elements held in this queue, must be Serializable
 * @author MarkLogic Corporation
 * @since 1.0
 */
public class DiskQueue<E extends Serializable> extends AbstractQueue<String> {

    private static final Logger LOG = Logger.getLogger(DiskQueue.class.getName());

    /**
     * Default ratio (0.75) of memory queue usage that triggers a refill from disk.
     * When the memory queue usage drops to 75% or below of its capacity, elements
     * are loaded from the disk backing store.
     */
    public static final float DEFAULT_REFILL_RATIO = 0.75f;

    /**
     * The in-memory queue representing the head of the queue. When no disk spillover
     * has occurred, this also represents the tail of the queue (i.e., the complete queue).
     */
    private MemoryQueue<E> memoryQueue;

    /**
     * Percentage of memory queue used/capacity that triggers a refill from disk.
     * Ranges from 0.0 to 1.0.
     */
    private float refillMemoryRatio;

    /**
     * Number of elements currently stored in the backing store file on disk.
     * This count is decremented as elements are read back into memory.
     */
    private int fileElementCount;

    /**
     * Directory where the temporary backing store file will be created.
     * If null, the system default temporary directory is used.
     */
    private File tempDir;

    /**
     * Writer for appending elements to the disk backing store file.
     */
    private BufferedWriter fileOut;

    /**
     * Reader for loading elements from the disk backing store file.
     */
    private BufferedReader fileIn;

    /**
     * Cache for a single element that was read from disk but couldn't fit in the memory queue.
     * When moving elements from disk to memory, we don't know whether the memory queue has
     * space until the offer is rejected. Rather than trying to push back an element into
     * the file, we cache it here for the next load operation.
     */
    private String cachedElement;

    /**
     * The temporary file used as the disk backing store for overflow elements.
     * Created lazily when the memory queue fills up.
     */
    private File fileQueue;

    /**
     * Constructs a disk-backed queue that keeps at most {@code maxInMemorySize} elements in memory.
     * The temporary backing store file will be created in the system default temporary directory.
     *
     * @param maxInMemorySize maximum number of elements to keep in memory; must be at least 1
     * @throws InvalidParameterException if {@code maxInMemorySize} is less than 1
     */
    public DiskQueue(int maxInMemorySize) {
        this(maxInMemorySize, null);
    }


    /**
     * Constructs a disk-backed queue that keeps at most {@code maxInMemorySize} elements in memory,
     * with the temporary backing store file created in the specified directory.
     *
     * @param maxInMemorySize maximum number of elements to keep in memory; must be at least 1
     * @param tempDir directory where queue temporary files will be written; may be null to use
     *                the system default temporary directory. If specified, must exist, be a
     *                directory, and be writable.
     * @throws InvalidParameterException if {@code maxInMemorySize} is less than 1, or if
     *         {@code tempDir} is specified but doesn't exist, is not a directory, or is not writable
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

    /**
     * Finalizer that ensures cleanup of file resources and deletion of the temporary backing store.
     * Logs a warning if cleanup was still needed at finalization time, which may indicate
     * a resource leak where {@link #clear()} was not called.
     *
     * @throws Throwable if an error occurs during finalization
     * @deprecated Finalization is deprecated and subject to removal in future Java versions.
     *             Users should explicitly call {@link #clear()} to ensure timely cleanup.
     */
    @Override
    protected void finalize() throws Throwable {
        if (closeFile()) {
            LOG.log(Level.WARNING, () -> MessageFormat.format("{0} still had open file in finalize", DiskQueue.class.getSimpleName()));
        }
        super.finalize();
    }

    /**
     * Closes all file streams and deletes the temporary backing store file.
     * This method ensures that:
     * <ul>
     *   <li>The input stream is closed and nulled</li>
     *   <li>The output stream is closed and nulled</li>
     *   <li>The cached element is cleared</li>
     *   <li>The file element count is reset to 0</li>
     *   <li>The temporary file is deleted</li>
     * </ul>
     * This method is idempotent; calling it when no file is open is safe.
     *
     * @return {@code true} if a file was open and had to be closed; {@code false} if no file was open
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

    /**
     * Opens the temporary backing store file for reading and writing.
     * This method is idempotent; if a file is already open, it does nothing.
     * <p>
     * The file is created with a unique name in the configured temporary directory,
     * marked for deletion on JVM exit, and opened for both reading and writing.
     * The output stream is flushed immediately to ensure the file exists for the input stream.
     * </p>
     *
     * @throws IOException if an error occurs creating or opening the file streams
     */
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

    /**
     * This operation is not supported by DiskQueue.
     *
     * @return never returns normally
     * @throws UnsupportedOperationException always thrown as iteration is not supported
     */
    @Override
    public Iterator<String> iterator() {
        throw new UnsupportedOperationException(MessageFormat.format("Iterator is not supported for {0}", DiskQueue.class.getSimpleName()));
    }

    /**
     * Returns the total number of elements in this queue, including elements in memory,
     * on disk, and in the single-element cache.
     *
     * @return the total number of elements in the queue
     */
    @Override
    public int size() {
        return memoryQueue.size() + fileElementCount + (cachedElement != null ? 1 : 0);
    }

    /**
     * Inserts the specified element into this queue if it is possible to do so immediately
     * without violating capacity constraints.
     * <p>
     * If the in-memory queue has space, the element is added there. Otherwise, the element
     * is written to the disk backing store. If the backing store doesn't exist yet, it is
     * created automatically.
     * </p>
     *
     * @param element the element to add; must not be null
     * @return {@code true} if the element was added successfully; {@code false} if an I/O error
     *         occurred writing to the backing store
     * @throws NullPointerException if the specified element is null
     */
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

    /**
     * Retrieves, but does not remove, the head of this queue, or returns {@code null}
     * if this queue is empty.
     * <p>
     * This method triggers loading of elements from disk into memory if the memory queue
     * is below its refill threshold.
     * </p>
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    @Override
    public String peek() {
        loadMemoryQueue();
        return memoryQueue.peek();
    }

    /**
     * Retrieves and removes the head of this queue.
     * <p>
     * This method triggers loading of elements from disk into memory if the memory queue
     * is below its refill threshold.
     * </p>
     *
     * @return the head of this queue
     * @throws java.util.NoSuchElementException if this queue is empty
     */
    @Override
    public String remove() {
        loadMemoryQueue();
        return memoryQueue.remove();
    }

    /**
     * Retrieves and removes the head of this queue, or returns {@code null} if this queue is empty.
     * <p>
     * This method triggers loading of elements from disk into memory if the memory queue
     * is below its refill threshold.
     * </p>
     *
     * @return the head of this queue, or {@code null} if this queue is empty
     */
    @Override
    public String poll() {
        loadMemoryQueue();
        return memoryQueue.poll();
    }

    /**
     * Removes all elements from this queue.
     * <p>
     * This implementation provides an optimized clear operation that directly clears
     * the memory queue, discards the cached element, and closes/deletes the backing store file,
     * rather than repeatedly calling {@link #poll()} as the default {@link AbstractQueue}
     * implementation would do.
     * </p>
     * <p>
     * This method should be called when the queue is no longer needed to ensure timely
     * cleanup of file resources.
     * </p>
     */
    @Override
    public void clear() {
        memoryQueue.clear();
        cachedElement = null;
        closeFile();
    }

    /**
     * Loads elements from the disk backing store into the memory queue if space is available.
     * <p>
     * This method is called before read operations (peek, poll, remove) to ensure the memory
     * queue has elements to serve. Loading only occurs when the memory queue usage drops below
     * the configured refill ratio (default 75%).
     * </p>
     * <p>
     * The loading process:
     * </p>
     * <ol>
     *   <li>First attempts to add the cached element (if any) to the memory queue</li>
     *   <li>Reads elements from disk one at a time until the memory queue is full or
     *       all disk elements are consumed</li>
     *   <li>If the memory queue fills before all disk elements are consumed, caches the
     *       next element for the subsequent load operation</li>
     *   <li>Closes and deletes the backing store file when all elements have been loaded</li>
     * </ol>
     * <p>
     * Empty lines from the disk file are skipped during the load process.
     * </p>
     */
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

    /**
     * A fixed-capacity in-memory queue implementation backed by an {@link ArrayList}.
     * <p>
     * This queue has a maximum capacity and rejects offers when full. Elements are
     * stored in insertion order, with the head at index 0.
     * </p>
     * <p>
     * This inner class is used by {@link DiskQueue} to maintain the in-memory portion
     * of the queue.
     * </p>
     *
     * @param <E> the type of elements held in this queue (unused, as the queue stores Strings)
     */
    private static class MemoryQueue<E> extends AbstractQueue<String> {

        private final List<String> queue;
        private final int capacity;

        /**
         * Constructs a new MemoryQueue with the specified capacity.
         *
         * @param capacity the maximum number of elements this queue can hold
         */
        public MemoryQueue(int capacity) {
            super();
            this.capacity = capacity;
            queue = new ArrayList<>(capacity);
        }

        /**
         * Returns an iterator over the elements in this queue in proper sequence.
         * The iterator does not support removal.
         *
         * @return an iterator over the elements in this queue
         */
        @Override
        public Iterator<String> iterator() {
            return queue.iterator();
        }

        /**
         * Returns the maximum capacity of this queue.
         *
         * @return the maximum number of elements this queue can hold
         */
        public int getCapacity() {
            return capacity;
        }

        /**
         * Returns the number of elements currently in this queue.
         *
         * @return the number of elements in this queue
         */
        @Override
        public int size() {
            return queue.size();
        }

        /**
         * Inserts the specified element into this queue if space is available.
         *
         * @param o the element to add; must not be null
         * @return {@code true} if the element was added; {@code false} if the queue is at capacity
         * @throws NullPointerException if the specified element is null
         */
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

        /**
         * Retrieves, but does not remove, the head of this queue, or returns {@code null}
         * if this queue is empty.
         *
         * @return the head of this queue, or {@code null} if this queue is empty
         */
        @Override
        public String peek() {
            if (queue.isEmpty()) {
                return null;
            } else {
                return queue.get(0);
            }
        }

        /**
         * Retrieves and removes the head of this queue, or returns {@code null}
         * if this queue is empty.
         *
         * @return the head of this queue, or {@code null} if this queue is empty
         */
        @Override
        public String poll() {
            if (queue.isEmpty()) {
                return null;
            } else {
                return queue.remove(0);
            }
        }

        /**
         * Retrieves and removes the head of this queue.
         *
         * @return the head of this queue
         * @throws IndexOutOfBoundsException if this queue is empty
         */
        @Override
        public String remove() {
            return queue.remove(0);
        }
    }

}
