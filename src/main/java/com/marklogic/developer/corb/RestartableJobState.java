/*
 * Copyright (c) 2004-2026 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.marklogic.developer.corb;

import com.marklogic.developer.corb.util.FileUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.HashSet;

/**
 * Manages persisted restart state for a single CoRB job.
 * <p>
 * The restart state consists of:
 * </p>
 * <ul>
 * <li>a completion journal containing one processed URI per line</li>
 * <li>a disk-backed bucket index derived from that journal for bounded-memory lookups</li>
 * <li>metadata describing whether the current index matches the journal on disk</li>
 * </ul>
 * <p>
 * Current-run completions are appended to the journal. On the next run, this class
 * rebuilds or reuses the bucket index and answers membership checks without loading
 * the entire journal into memory.
 * </p>
  * @since 2.6.0
 */
public class RestartableJobState implements Closeable {

    static final String COMPLETED_URIS_FILENAME = "completed-uris.log";
    static final String COMPLETED_URIS_INDEX_DIRNAME = "completed-uris-index";
    static final String COMPLETED_URIS_INDEX_METADATA_FILENAME = "completed-uris-index.properties";
    static final String INDEXED_SOURCE_LENGTH = "indexed.source.length";
    static final String INDEXED_SOURCE_LAST_MODIFIED = "indexed.source.lastModified";
    static final String INDEXED_UNIQUE_COUNT = "indexed.uniqueCount";
    private static final Object COMPLETED_URIS_SYNC_OBJ = new Object();
    private static final int BUCKET_ID_LENGTH = 3;
    private static final int CACHE_BUCKET_LIMIT = 8;
    private static final int BUCKET_FLUSH_SIZE = 256;

    private final File stateDir;
    private final File completedUrisFile;
    private final File completedUrisIndexDir;
    private final File completedUrisIndexMetadataFile;
    private final Map<String, Set<String>> cachedBucketEntries = new LinkedHashMap<String, Set<String>>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Set<String>> eldest) {
            return size() > CACHE_BUCKET_LIMIT;
        }
    };
    private long previouslyCompletedUriCount;

    /**
     * Creates or opens restart state rooted at the given job-scoped directory.
     *
     * @param stateDir the directory containing the completion journal and index
     * @throws IOException if the state directory or index cannot be initialized
     */
    public RestartableJobState(File stateDir) throws IOException {
        this.stateDir = stateDir;
        Files.createDirectories(stateDir.toPath());
        this.completedUrisFile = new File(stateDir, COMPLETED_URIS_FILENAME);
        this.completedUrisIndexDir = new File(stateDir, COMPLETED_URIS_INDEX_DIRNAME);
        this.completedUrisIndexMetadataFile = new File(stateDir, COMPLETED_URIS_INDEX_METADATA_FILENAME);
        initializePreviousRunIndex();
    }

    private void initializePreviousRunIndex() throws IOException {
        Files.createDirectories(completedUrisIndexDir.toPath());
        if (!completedUrisFile.exists()) {
            previouslyCompletedUriCount = 0;
            return;
        }
        if (isCurrentIndexUsable()) {
            previouslyCompletedUriCount = loadIndexedUniqueCount();
            return;
        }
        rebuildPreviousRunIndex();
    }

    private boolean isCurrentIndexUsable() throws IOException {
        if (!completedUrisIndexMetadataFile.exists()) {
            return false;
        }
        Properties metadata = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(completedUrisIndexMetadataFile.toPath(), StandardCharsets.UTF_8)) {
            metadata.load(reader);
        }
        return Long.toString(completedUrisFile.length()).equals(metadata.getProperty(INDEXED_SOURCE_LENGTH))
            && Long.toString(completedUrisFile.lastModified()).equals(metadata.getProperty(INDEXED_SOURCE_LAST_MODIFIED));
    }

    private long loadIndexedUniqueCount() throws IOException {
        Properties metadata = new Properties();
        try (BufferedReader reader = Files.newBufferedReader(completedUrisIndexMetadataFile.toPath(), StandardCharsets.UTF_8)) {
            metadata.load(reader);
        }
        String uniqueCount = metadata.getProperty(INDEXED_UNIQUE_COUNT);
        return uniqueCount == null ? countUniqueEntriesInIndex() : Long.parseLong(uniqueCount);
    }

    private void rebuildPreviousRunIndex() throws IOException {
        if (completedUrisIndexDir.exists()) {
            FileUtils.deleteFile(completedUrisIndexDir.getAbsolutePath());
        }
        Files.createDirectories(completedUrisIndexDir.toPath());
        cachedBucketEntries.clear();

        Map<String, List<String>> pendingBucketWrites = new HashMap<>();
        try (BufferedReader reader = Files.newBufferedReader(completedUrisFile.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                String bucketId = getBucketId(trimmed);
                List<String> bucketLines = pendingBucketWrites.computeIfAbsent(bucketId, key -> new ArrayList<>());
                bucketLines.add(trimmed);
                if (bucketLines.size() >= BUCKET_FLUSH_SIZE) {
                    flushBucketEntries(bucketId, bucketLines);
                    bucketLines.clear();
                }
            }
        }
        for (Map.Entry<String, List<String>> entry : pendingBucketWrites.entrySet()) {
            flushBucketEntries(entry.getKey(), entry.getValue());
        }

        previouslyCompletedUriCount = countUniqueEntriesInIndex();
        Properties metadata = new Properties();
        metadata.setProperty(INDEXED_SOURCE_LENGTH, Long.toString(completedUrisFile.length()));
        metadata.setProperty(INDEXED_SOURCE_LAST_MODIFIED, Long.toString(completedUrisFile.lastModified()));
        metadata.setProperty(INDEXED_UNIQUE_COUNT, Long.toString(previouslyCompletedUriCount));
        try (BufferedWriter writer = Files.newBufferedWriter(
            completedUrisIndexMetadataFile.toPath(),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING
        )) {
            metadata.store(writer, "Restart state index metadata");
        }
    }

    /**
     * Returns whether the given URI appears in the restart state loaded from a prior run.
     *
     * @param uri the URI to check
     * @return true if the URI was previously recorded as processed
     */
    public boolean wasCompletedInPreviousRun(String uri) {
        if (uri == null) {
            return false;
        }
        try {
            return getBucketEntries(getBucketId(uri.trim())).contains(uri.trim());
        } catch (IOException ex) {
            throw new IllegalStateException("Unable to read restart state bucket", ex);
        }
    }

    /**
     * Returns the number of unique URIs recorded in the prior-run restart state.
     *
     * @return number of distinct previously processed URIs
     */
    public long getPreviouslyCompletedUriCount() {
        return previouslyCompletedUriCount;
    }

    /**
     * Returns the completion journal file used by this restart state.
     *
     * @return the completion journal file
     */
    File getCompletedUrisFile() {
        return completedUrisFile;
    }

    /**
     * Returns the directory containing the bucket index used for restart lookups.
     *
     * @return the bucket index directory
     */
    File getCompletedUrisIndexDir() {
        return completedUrisIndexDir;
    }

    /**
     * Returns the root directory for this job-scoped restart state.
     *
     * @return the restart state directory
     */
    public File getStateDir() {
        return stateDir;
    }

    /**
     * Appends processed URIs to this instance's completion journal.
     *
     * @param uris URIs to append
     * @throws IOException if the journal cannot be updated
     */
    public void appendCompletedUris(String[] uris) throws IOException {
        appendCompletedUris(stateDir, uris);
    }

    /**
     * Appends processed URIs to the completion journal rooted at the given state directory.
     *
     * @param stateDir the job-scoped restart state directory
     * @param uris URIs to append
     * @throws IOException if the journal cannot be updated
     */
    public static void appendCompletedUris(File stateDir, String[] uris) throws IOException {
        if (uris == null || uris.length == 0) {
            return;
        }
        Files.createDirectories(stateDir.toPath());
        File completedUrisFile = new File(stateDir, COMPLETED_URIS_FILENAME);
        synchronized (COMPLETED_URIS_SYNC_OBJ) {
            try (BufferedWriter completedUrisWriter = Files.newBufferedWriter(
                completedUrisFile.toPath(),
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
            )) {
                for (String uri : uris) {
                    if (uri == null) {
                        continue;
                    }
                    String trimmed = uri.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    completedUrisWriter.write(trimmed);
                    completedUrisWriter.newLine();
                }
                completedUrisWriter.flush();
            }
        }
    }

    /**
     * Deletes all restart state files for this instance, including the completion journal and bucket index.
     * This should be called when restart state is no longer needed to free up drive space.
     *
     * @throws IOException if any state files cannot be deleted
     */
    public void deleteStateFiles() throws IOException {
        cachedBucketEntries.clear();
        deleteIfExists(completedUrisFile);
        deleteIfExists(completedUrisIndexDir);
        deleteIfExists(completedUrisIndexMetadataFile);
        if (stateDir.exists()) {
            File[] remainingFiles = stateDir.listFiles();
            if (remainingFiles != null && remainingFiles.length == 0) {
                FileUtils.deleteFile(stateDir);
            }
        }
        synchronized (this) {
            previouslyCompletedUriCount = 0;
        }
    }

    @Override
    public void close() throws IOException {
        // No-op. Restart artifacts are cleaned up explicitly by the caller when appropriate.
    }

    @Override
    protected final void finalize() throws Throwable {
        super.finalize();
    }

    private void flushBucketEntries(String bucketId, List<String> bucketEntries) throws IOException {
        if (bucketEntries == null || bucketEntries.isEmpty()) {
            return;
        }
        File bucketFile = new File(completedUrisIndexDir, bucketId + ".log");
        try (BufferedWriter writer = Files.newBufferedWriter(
            bucketFile.toPath(),
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND
        )) {
            for (String bucketEntry : bucketEntries) {
                writer.write(bucketEntry);
                writer.newLine();
            }
        }
    }

    private Set<String> getBucketEntries(String bucketId) throws IOException {
        synchronized (cachedBucketEntries) {
            Set<String> bucketEntries = cachedBucketEntries.get(bucketId);
            if (bucketEntries != null) {
                return bucketEntries;
            }
            Set<String> loadedBucketEntries = loadBucketEntries(bucketId);
            cachedBucketEntries.put(bucketId, loadedBucketEntries);
            return loadedBucketEntries;
        }
    }

    private Set<String> loadBucketEntries(String bucketId) throws IOException {
        Set<String> bucketEntries = new HashSet<>();
        File bucketFile = new File(completedUrisIndexDir, bucketId + ".log");
        if (!bucketFile.exists()) {
            return bucketEntries;
        }
        try (BufferedReader reader = Files.newBufferedReader(bucketFile.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty()) {
                    bucketEntries.add(trimmed);
                }
            }
        }
        return bucketEntries;
    }

    private long countUniqueEntriesInIndex() throws IOException {
        long uniqueCount = 0;
        File[] bucketFiles = completedUrisIndexDir.listFiles((dir, name) -> name.endsWith(".log"));
        if (bucketFiles == null) {
            return uniqueCount;
        }
        for (File bucketFile : bucketFiles) {
            Set<String> bucketEntries = new HashSet<>();
            try (BufferedReader reader = Files.newBufferedReader(bucketFile.toPath(), StandardCharsets.UTF_8)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty()) {
                        bucketEntries.add(trimmed);
                    }
                }
            }
            uniqueCount += bucketEntries.size();
        }
        return uniqueCount;
    }

    private static String getBucketId(String uri) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(uri.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(BUCKET_ID_LENGTH * 2);
            for (byte currentByte : bytes) {
                hex.append(String.format("%02x", currentByte));
                if (hex.length() >= BUCKET_ID_LENGTH) {
                    return hex.substring(0, BUCKET_ID_LENGTH);
                }
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("Unable to hash restart state URI", ex);
        }
    }

    private static void deleteIfExists(File file) throws IOException {
        if (file != null && file.exists()) {
            FileUtils.deleteFile(file);
        }
    }
}
