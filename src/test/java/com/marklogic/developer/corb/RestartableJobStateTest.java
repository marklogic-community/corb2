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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class RestartableJobStateTest {

    @Test
    void testLoadAndAppendCompletedUris() throws Exception {
        File stateDir = TestUtils.createTempDirectory();
        try {
            File completedUrisFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME);
            Files.write(
                completedUrisFile.toPath(),
                Arrays.asList("uri-1", "", "uri-2", "uri-1"),
                StandardCharsets.UTF_8
            );

            try (RestartableJobState state = new RestartableJobState(stateDir)) {
                assertTrue(state.wasCompletedInPreviousRun("uri-1"));
                assertTrue(state.wasCompletedInPreviousRun("uri-2"));
                assertFalse(state.wasCompletedInPreviousRun("uri-3"));
                assertEquals(2L, state.getPreviouslyCompletedUriCount());
                assertTrue(state.getCompletedUrisIndexDir().exists());

                state.appendCompletedUris(new String[]{"uri-3", "uri-4"});
                assertFalse(state.wasCompletedInPreviousRun("uri-3"));
            }

            try (RestartableJobState reloadedState = new RestartableJobState(stateDir)) {
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
        File stateDir = TestUtils.createTempDirectory();
        try {
            File completedUrisFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME);
            Files.write(completedUrisFile.toPath(), Arrays.asList("uri-1", "uri-2"), StandardCharsets.UTF_8);
            try (RestartableJobState state = new RestartableJobState(stateDir)) {
                File bucketFile = new File(state.getCompletedUrisIndexDir(), "abc.log");
                Files.write(bucketFile.toPath(), Arrays.asList("uri-1"), StandardCharsets.UTF_8);
                File metadataFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_INDEX_METADATA_FILENAME);
                assertTrue(completedUrisFile.exists());
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
        File stateDir = TestUtils.createTempDirectory();
        try {
            File completedUrisFile = new File(stateDir, RestartableJobState.COMPLETED_URIS_FILENAME);
            Files.write(completedUrisFile.toPath(), Arrays.asList("uri-1", "uri-2"), StandardCharsets.UTF_8);
            File unrelatedFile = new File(stateDir, "keep-me.txt");
            Files.write(unrelatedFile.toPath(), Arrays.asList("leave this alone"), StandardCharsets.UTF_8);
            try (RestartableJobState state = new RestartableJobState(stateDir)) {
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
}
