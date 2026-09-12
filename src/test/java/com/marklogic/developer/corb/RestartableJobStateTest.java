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
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RestartableJobStateTest {

    private static File newStateDir() throws Exception {
        return TestUtils.createTempDirectory();
    }

    private static void writeCompletedUris(File stateDir, String... uris) throws Exception {
        Files.write(new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME).toPath(), Arrays.asList(uris), StandardCharsets.UTF_8);
    }

    private static RestartableJobState newState(File stateDir) throws Exception {
        return new RestartableJobState(stateDir);
    }

    @Test
    void testLoadAndAppendCompletedUris() throws Exception {
        File stateDir = newStateDir();
        try {
            writeCompletedUris(stateDir, "uri-1", "", "uri-2", "uri-1");

            try (RestartableJobState state = newState(stateDir)) {
                assertTrue(state.wasCompletedInPreviousRun("uri-1"));
                assertTrue(state.wasCompletedInPreviousRun("uri-2"));
                assertFalse(state.wasCompletedInPreviousRun("uri-3"));
                assertEquals(2L, state.getPreviouslyCompletedUriCount());
                assertTrue(state.getCompletedUrisIndexDir().exists());

                state.appendCompletedUris(new String[]{"uri-3", "uri-4"});
                assertFalse(state.wasCompletedInPreviousRun("uri-3"));
            }

            try (RestartableJobState reloadedState = newState(stateDir)) {
                assertTrue(reloadedState.wasCompletedInPreviousRun("uri-3"));
                assertTrue(reloadedState.wasCompletedInPreviousRun("uri-4"));
                assertEquals(4L, reloadedState.getPreviouslyCompletedUriCount());
            }
        } finally {
            FileUtils.deleteFile(stateDir.getAbsolutePath());
        }
    }

    @Test
    void testDeleteStateFilesDeletesRestartArtifactsAndEmptyStateDirectory() throws Exception {
        File stateDir = newStateDir();
        try {
            writeCompletedUris(stateDir, "uri-1", "uri-2");
            try (RestartableJobState state = newState(stateDir)) {
                File bucketFile = new File(state.getCompletedUrisIndexDir(), "abc.log");
                Files.write(bucketFile.toPath(), Collections.singletonList("uri-1"), StandardCharsets.UTF_8);
                File metadataFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_INDEX_METADATA_FILENAME);
                assertTrue(new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME).exists());
                assertTrue(state.getCompletedUrisIndexDir().exists());
                assertTrue(metadataFile.exists());

                state.deleteStateFiles();
                assertFalse(stateDir.exists());
            }
        } finally {
            if (stateDir.exists()) {
                FileUtils.deleteFile(stateDir.getAbsolutePath());
            }
        }
    }

    @Test
    void testDeleteStateFilesLeavesUnrelatedFilesInStateDirectory() throws Exception {
        File stateDir = newStateDir();
        try {
            File completedUrisFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME);
            writeCompletedUris(stateDir, "uri-1", "uri-2");
            File unrelatedFile = new File(stateDir, "keep-me.txt");
            Files.write(unrelatedFile.toPath(), Collections.singletonList("leave this alone"), StandardCharsets.UTF_8);
            try (RestartableJobState state = newState(stateDir)) {
                File metadataFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_INDEX_METADATA_FILENAME);
                assertTrue(completedUrisFile.exists());
                assertTrue(state.getCompletedUrisIndexDir().exists());
                assertTrue(metadataFile.exists());
                assertTrue(unrelatedFile.exists());

                state.deleteStateFiles();

                assertTrue(stateDir.exists());
                assertFalse(completedUrisFile.exists());
                assertFalse(state.getCompletedUrisIndexDir().exists());
                assertFalse(metadataFile.exists());
                assertTrue(unrelatedFile.exists());
            }
        } finally {
            if (stateDir.exists()) {
                FileUtils.deleteFile(stateDir.getAbsolutePath());
            }
        }
    }

    @Test
    void testAppendCompletedUrisSkipsNullAndBlankValues() throws Exception {
        File stateDir = newStateDir();
        try {
            RestartableJobState.appendCompletedUris(stateDir, new String[]{null, " ", "uri-1"});
            List<String> lines = Files.readAllLines(new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME).toPath(), StandardCharsets.UTF_8);
            assertEquals(Collections.singletonList("uri-1"), lines);
        } finally {
            FileUtils.deleteFile(stateDir.getAbsolutePath());
        }
    }

    @Test
    void testDeleteStateFilesOnEmptyDirectoryRemovesStateDir() throws Exception {
        File stateDir = newStateDir();
        try (RestartableJobState state = newState(stateDir)) {
            state.deleteStateFiles();
            assertFalse(stateDir.exists());
        }
    }
}
