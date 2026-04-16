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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Properties;

import static com.marklogic.developer.corb.Options.*;
import static com.marklogic.developer.corb.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.marklogic.xcc.Request;
import com.marklogic.xcc.exceptions.RequestPermissionException;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipFile;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
class PostBatchUpdateFileTaskTest {

    private static final Logger LOG = Logger.getLogger(PostBatchUpdateFileTaskTest.class.getName());
    private static final String BOTTOM_CONTENT = "col1,col2,col3,col4";
    private static final String BAK_EXT = ".bak";
    private static final String PART_EXT = ".part";
    private static final String EXAMPLE_CONTENT = "The quick brown fox jumped over the lazy dog.";
    private static final String TEMP_FILE_PREFIX = "moveFile";
    private static final String TEMP_FILE_SUFFIX = "txt";
    private static final String PART_FILE_EXT = ".zpart";
    private static final String ZIP_EXT = ".zip";
    private static final String A = "a\n";
    private static final String B = "b\n";
    private static final String D = "d\n";
    private static final String Z = "z\n";
    private static final String ZDDAB = "z...,d....,d....,a.....,b";
    private static final String DISTINCT = "distinct";

    @BeforeEach
    void setUp() {
        clearSystemProperties();
    }

    @Test
    void testGetBottomContent() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
        String result = instance.getBottomContent();
        assertEquals(BOTTOM_CONTENT, result);
    }

    @Test
    void testGetBottomContentNullBottom() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        String result = instance.getBottomContent();
        assertNull(result);
    }

    @Test
    void testWriteBottomContent() {
        try {
            String expectedResult = BOTTOM_CONTENT.concat("\n");
            String filename = "export.csv";
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = new Properties();
            instance.properties.setProperty(EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
            instance.properties.setProperty(EXPORT_FILE_NAME, filename);
            instance.properties.setProperty(EXPORT_FILE_PART_EXT, ".temp");
            File tempDir = createTempDirectory();
            instance.exportDir = tempDir.toString();

            instance.writeBottomContent();

            File outputFile = new File(tempDir, "export.csv.temp");
            String outputText = readFile(outputFile);
            assertEqualsNormalizeNewline(expectedResult, outputText);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMoveFileStringString() {
        try {
            File source = createSampleFile();
            String destFilePath = source + BAK_EXT;
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.moveFile(source.toString(), destFilePath);

            File dest = new File(destFilePath);
            assertFalse(source.exists());
            assertTrue(dest.exists());
            assertEquals(EXAMPLE_CONTENT, readFile(dest));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMoveFileStringStringDestExists() {
        try {
            File source = createSampleFile();
            String destFilePath = source + BAK_EXT;
            File dest = new File(destFilePath);
            if (dest.createNewFile()) {
                dest.deleteOnExit();
                PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
                instance.moveFile(source.toString(), destFilePath);

                assertFalse(source.exists());
                assertTrue(dest.exists());
                assertEquals(EXAMPLE_CONTENT, readFile(dest));
            } else {
                fail();
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testMoveFileZeroargs() {
        try {
            File partFile = createSamplePartFile();
            String partFilePath = partFile.toString();
            String exportFilePath = partFilePath.substring(0, partFilePath.lastIndexOf('.'));
            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_PART_EXT, PART_EXT);
            props.setProperty(EXPORT_FILE_NAME, exportFilePath);
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.moveFile();

            File dest = new File(exportFilePath);
            assertTrue(dest.exists());
            assertFalse(partFile.exists());
            assertEquals(EXAMPLE_CONTENT, readFile(dest));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCompressFile() {
        try {
            File sampleFile = createSamplePartFile();
            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_AS_ZIP, "true");
            props.setProperty(EXPORT_FILE_NAME, sampleFile.toString());
            props.setProperty(EXPORT_FILE_PART_EXT, PART_FILE_EXT);

            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.compressFile();

            File output = new File(sampleFile.toString().concat(ZIP_EXT));
            output.deleteOnExit();
            assertTrue(output.exists());
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCompressFileOutputFileDoesNotExist() {
        try {
            File sampleFile = createSamplePartFile();
            if (sampleFile.delete()) {
                Properties props = new Properties();
                props.setProperty(EXPORT_FILE_AS_ZIP, Boolean.TRUE.toString());
                props.setProperty(EXPORT_FILE_NAME, sampleFile.toString());
                props.setProperty(EXPORT_FILE_PART_EXT, PART_FILE_EXT);

                PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
                instance.properties = props;
                instance.compressFile();

                File output = new File(sampleFile.toString().concat(ZIP_EXT));
                output.deleteOnExit();
                assertFalse(output.exists());
            } else {
                fail();
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCompressFileZipPartExists() {
        try {
            File sampleFile = createSamplePartFile();
            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_AS_ZIP, Boolean.toString(true));
            props.setProperty(EXPORT_FILE_NAME, sampleFile.toString());
            props.setProperty(EXPORT_FILE_PART_EXT, "zpart");
            String zipFilePart = sampleFile.toString().concat(ZIP_EXT).concat(PART_EXT);
            File existingZipFilePart = new File(zipFilePart);
            if (existingZipFilePart.createNewFile()) {

                PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
                instance.properties = props;
                instance.compressFile();

                File output = new File(sampleFile.toString().concat(ZIP_EXT));
                output.deleteOnExit();
                assertTrue(output.exists());
            } else {
                fail();
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortAscUniq() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_SORT, "asc|uniq");
        try {
            String result = testRemoveDuplicatesAndSort(props);
            List<String> tokens = Arrays.asList(result.split("\\R"));
            assertEquals(4, tokens.size());
            for (String next : new String[]{"a", "b", "d", "z"}) {
                assertTrue(tokens.contains(next));
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortASCENDING() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_SORT, "ASCENDING|distinct");
        try {
            String result = testRemoveDuplicatesAndSort(props);
            assertEqualsNormalizeNewline(splitAndAppendNewline("a,b,d,z"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortTrueSortWithHeaderAndFooter() {
        String header = "BEGIN\nletter\n";
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            try (Writer writer = new OutputStreamWriter(new FileOutputStream(file, true), StandardCharsets.UTF_8)) {
                writer.append(header);
                writer.append(Z);
                writer.append(D);
                writer.append(D);
                writer.append(A);
                writer.append(B);
                writer.flush();
            }

            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_TOP_CONTENT, header);
            props.setProperty(EXPORT_FILE_HEADER_LINE_COUNT, "2");
            props.setProperty(EXPORT_FILE_SORT, "ascending|distinct");
            props.setProperty(EXPORT_FILE_BOTTOM_CONTENT, "END");
            String result = testRemoveDuplicatesAndSort(file, props);
            assertEqualsNormalizeNewline(splitAndAppendNewline("BEGIN,letter,a,b,d,z,END"), result);

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortCustomComparator() throws Exception {
        testCustomComparator(null, "b,z...,d....,d....,a.....");
    }

    @Test
    void testCallRemoveDuplicatesAndSortCustomComparatorDistinct() throws Exception {
        testCustomComparator(DISTINCT, "b,z...,d....,a.....");
    }

    @Test
    void testCallRemoveDuplicatesAndSortCustomComparatorBadClass() throws Exception {
        testCustomComparator(DISTINCT, ZDDAB, "java.lang.String");
    }

    @Test
    void testCallRemoveDuplicatesAndSortNoSortOrDedupDistinctOnly() throws Exception {
        testCustomComparator(DISTINCT, ZDDAB, null);
    }

    @Test
    void testCallRemoveDuplicatesAndSortNoSortOrDedupBlankSort() throws Exception {
        testCustomComparator(" ", ZDDAB, null);
    }

    void testCustomComparator(String sortProperty, String expected) throws Exception {
        testCustomComparator(sortProperty, expected, "com.marklogic.developer.corb.PostBatchUpdateFileTaskTest$StringLengthComparator");
    }

    static void testCustomComparator(String sortProperty, String expected, String comparator) throws Exception {

        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8)) {
            writer.append("z...\n");
            String dDots = "d....\n";
            writer.append(dDots);
            writer.append(dDots);
            writer.append("a.....\n");
            writer.append(B);
            writer.flush();
        }

        Properties props = new Properties();
        if (comparator != null) {
            props.setProperty(EXPORT_FILE_SORT_COMPARATOR, comparator);
        }
        if (sortProperty != null) {
            props.setProperty(EXPORT_FILE_SORT, sortProperty);
        }
        String result = testRemoveDuplicatesAndSort(file, props);
        assertEqualsNormalizeNewline(splitAndAppendNewline(expected), result);
    }

    @Test
    void testCallRemoveDuplicatesAndSortDescendingDistinct() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_SORT, "desc|distinct");
        String result;
        try {
            result = testRemoveDuplicatesAndSort(props);
            assertEqualsNormalizeNewline(splitAndAppendNewline("z,d,b,a"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortInvalidValue() {
        Properties props = new Properties();
        props.setProperty(EXPORT_FILE_SORT, Boolean.toString(false));
        String result;
        try {
            result = testRemoveDuplicatesAndSort(props);
            assertEquals(splitAndAppendNewline("z,d,d,a,b"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private static String splitAndAppendNewline(String values) {
        StringBuilder sb = new StringBuilder();

        for (String value : values.split(",")) {
            sb.append(value)
                    .append('\n');
        }
        return sb.toString();
    }

    @Test
    void testCallRemoveDuplicatesAndSortEmptyFileExtensionWithoutDot() {
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_SORT, Boolean.toString(true));
            props.setProperty(EXPORT_FILE_NAME, file.toString());
            props.setProperty(EXPORT_FILE_PART_EXT, "pt");
            props.setProperty(EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.call();
            assertEquals(0, file.length());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testCallRemoveDuplicatesAndSortEmptyFileEmptyExtension() {
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            Properties props = new Properties();
            props.setProperty(EXPORT_FILE_SORT, Boolean.TRUE.toString());
            props.setProperty(EXPORT_FILE_NAME, file.toString());
            props.setProperty(EXPORT_FILE_PART_EXT, "");
            props.setProperty(EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.call();
            assertEquals(0, file.length());
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private static String testRemoveDuplicatesAndSort(Properties props) throws Exception {

        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8)) {
            writer.append(Z);
            writer.append(D);
            writer.append(D);
            writer.append(A);
            writer.append(B);
        }
        return testRemoveDuplicatesAndSort(file, props);
    }

    private static String testRemoveDuplicatesAndSort(File fileToSort, Properties props)
            throws Exception {

        props.setProperty(EXPORT_FILE_AS_ZIP, Boolean.FALSE.toString());
        props.setProperty(EXPORT_FILE_NAME, fileToSort.toString());
        props.setProperty(EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = props;
        instance.call();
        File output = new File(fileToSort.toString());
        output.deleteOnExit();
        return readFile(output);
    }

    @Test
    void testCall() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        try {
            assertThrows(NullPointerException.class, instance::call);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    void testGetMaxLines() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "100");
        instance.setProperties(props);
        assertEquals(100, instance.getMaxLines());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());

        // Test with invalid value
        props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "not-a-number");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());
    }
    @Test
    void testInsertIndexIntoFileName() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();

        // Test with extension
        assertEquals("001_output.txt", instance.insertIndexIntoFileName("output.txt", 1));
        assertEquals("002_output.csv", instance.insertIndexIntoFileName("output.csv", 2));

        // Test without extension
        assertEquals("001_output", instance.insertIndexIntoFileName("output", 1));

        // Test with multiple dots
        assertEquals("001_my.output.txt", instance.insertIndexIntoFileName("my.output.txt", 1));
    }

    @Test
    void testGetMaxSize() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "1024");
        instance.setProperties(props);
        assertEquals(1024, instance.getMaxSize());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());

        // Test with invalid value
        props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "invalid");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());
    }

    @Test
    void testShouldSplitFilesUsesValidThresholdsOnly() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        Properties props = new Properties();

        props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "invalid");
        instance.setProperties(props);
        assertFalse(instance.shouldSplitFiles());

        props.clear();
        props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "invalid");
        instance.setProperties(props);
        assertFalse(instance.shouldSplitFiles());

        props.clear();
        props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "2");
        instance.setProperties(props);
        assertTrue(instance.shouldSplitFiles());
    }

    @Test
    void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask instance = new PostBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException(AbstractTaskTest.REJECTED_MSG, req, AbstractTaskTest.USER_NAME, false);
        assertFalse(instance.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, AbstractTaskTest.USER_NAME, false);
        assertTrue(instance.hasRetryableMessage(exception));
    }

    @Test
    void testWriteBottomContentWithSplitByLinesAndZip() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-split.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "2");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_AS_ZIP, "true");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            // Create a mock ResultSequence with 5 items
            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            //Need one extra hasNext() because it's tested first in writeToFile(seq)
            when(seq.hasNext()).thenReturn(true, true, true, true, true, true,  false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();
            // Verify that split files were created
            File file1 = new File(tempDir, "001_test-split.txt.tmp");
            File file2 = new File(tempDir, "002_test-split.txt.tmp");
            File file3 = new File(tempDir, "003_test-split.txt.tmp");

            assertTrue(file1.exists());
            assertTrue(file2.exists());
            assertTrue(file3.exists());

            // Verify line counts
            assertEquals(2, FileUtils.getLineCount(file1));
            assertEquals(2, FileUtils.getLineCount(file2));
            assertEquals(1, FileUtils.getLineCount(file3));

            //Now that the files are written, rename temp filenames to final
            instance.moveFile();
            // Verify that split files were created
            file1 = new File(tempDir, "001_test-split.txt");
            file2 = new File(tempDir, "002_test-split.txt");
            file3 = new File(tempDir, "003_test-split.txt");

            assertTrue(file1.exists());
            assertTrue(file2.exists());
            assertTrue(file3.exists());

            // Verify line counts
            assertEquals(2, FileUtils.getLineCount(file1));
            assertEquals(2, FileUtils.getLineCount(file2));
            assertEquals(1, FileUtils.getLineCount(file3));

            instance.compressFile();
            File outputFile = new File(tempDir, "test-split.txt.zip");
            assertTrue(outputFile.exists());
            try (ZipFile zipFile = new ZipFile(outputFile)) {
                assertEquals( 3, zipFile.size());
            }
            assertFalse(file1.exists());
            assertFalse(file2.exists());
            assertFalse(file3.exists());

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithSplitByLinesAddsFooterToEachSplitFile() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-split-footer.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "2");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_BOTTOM_CONTENT, "FOOTER");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();

            File file1 = new File(tempDir, "001_test-split-footer.txt.tmp");
            File file2 = new File(tempDir, "002_test-split-footer.txt.tmp");
            File file3 = new File(tempDir, "003_test-split-footer.txt.tmp");

            assertEqualsNormalizeNewline("line1\nline2\nFOOTER\n", readFile(file1));
            assertEqualsNormalizeNewline("line3\nline4\nFOOTER\n", readFile(file2));
            assertEqualsNormalizeNewline("line5\nFOOTER\n", readFile(file3));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithSplitBySizeNotCountingHeaderBytes() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-size-split.txt.tmp");
            props.setProperty(EXPORT_FILE_NAME, new File(tempDir, "test-size-split.txt").getAbsolutePath());
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_HEADER_LINE_COUNT, "1");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            try (Writer writer = new OutputStreamWriter(Files.newOutputStream(batchFile.toPath()), StandardCharsets.UTF_8)) {
                writer.write("HEADER\n");
                writer.write("A\n");
                writer.write("B\n");
            }

            instance.writeBottomContentWithSplitting(batchFile, null, -1, 4);

            File file1 = new File(tempDir, "001_test-size-split.txt.tmp");
            File file2 = new File(tempDir, "002_test-size-split.txt.tmp");

            assertTrue(file1.exists());
            assertFalse(file2.exists());
            assertEqualsNormalizeNewline("HEADER\nA\nB\n", readFile(file1));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithBothSplitOptionsPrefersLineCount() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-both-split-options.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "3");
            props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "1");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();

            File file1 = new File(tempDir, "001_test-both-split-options.txt.tmp");
            File file2 = new File(tempDir, "002_test-both-split-options.txt.tmp");
            File file3 = new File(tempDir, "003_test-both-split-options.txt.tmp");

            assertTrue(file1.exists());
            assertTrue(file2.exists());
            assertFalse(file3.exists());
            assertEqualsNormalizeNewline("line1\nline2\nline3\n", readFile(file1));
            assertEqualsNormalizeNewline("line4\nline5\n", readFile(file2));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithInvalidLineThresholdFallsBackToSizeSplit() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-invalid-line-valid-size.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "invalid");
            props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "8");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();

            File file1 = new File(tempDir, "001_test-invalid-line-valid-size.txt.tmp");
            File file2 = new File(tempDir, "002_test-invalid-line-valid-size.txt.tmp");
            File file3 = new File(tempDir, "003_test-invalid-line-valid-size.txt.tmp");

            assertTrue(file1.exists());
            assertTrue(file2.exists());
            assertFalse(file3.exists());
            assertEqualsNormalizeNewline("line1\nline2\n", readFile(file1));
            assertEqualsNormalizeNewline("line3\n", readFile(file2));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testInvalidSplitThresholdFallsBackToSingleOutputFile() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-invalid-split.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "invalid");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_BOTTOM_CONTENT, "FOOTER");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();
            instance.moveFile();

            File finalFile = new File(tempDir, "test-invalid-split.txt");
            File splitFile = new File(tempDir, "001_test-invalid-split.txt.tmp");

            assertTrue(finalFile.exists());
            assertFalse(splitFile.exists());
            assertEqualsNormalizeNewline("line1\nline2\nFOOTER\n", readFile(finalFile));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithBothSplitOptionsAndZipPrefersLineCount() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-both-split-zip.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "3");
            props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "1");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_AS_ZIP, "true");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();
            instance.moveFile();
            instance.compressFile();

            File outputZipFile = new File(tempDir, "test-both-split-zip.txt.zip");
            assertTrue(outputZipFile.exists());
            try (ZipFile zipFile = new ZipFile(outputZipFile)) {
                assertEquals(2, zipFile.size());
                assertNotNull(zipFile.getEntry("001_test-both-split-zip.txt"));
                assertNotNull(zipFile.getEntry("002_test-both-split-zip.txt"));
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentWithInvalidLineThresholdAndZipFallsBackToSizeSplit() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-invalid-line-size-zip.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_SPLIT_MAX_LINES, "invalid");
            props.setProperty(EXPORT_FILE_SPLIT_MAX_SIZE, "8");
            props.setProperty(EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(EXPORT_FILE_AS_ZIP, "true");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            when(seq.hasNext()).thenReturn(true, true, true, true, false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();
            instance.moveFile();
            instance.compressFile();

            File outputZipFile = new File(tempDir, "test-invalid-line-size-zip.txt.zip");
            assertTrue(outputZipFile.exists());
            try (ZipFile zipFile = new ZipFile(outputZipFile)) {
                assertEquals(2, zipFile.size());
                assertNotNull(zipFile.getEntry("001_test-invalid-line-size-zip.txt"));
                assertNotNull(zipFile.getEntry("002_test-invalid-line-size-zip.txt"));
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    void testWriteBottomContentForBatchAndZip() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-bottom.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(EXPORT_FILE_AS_ZIP, "true");
            props.setProperty(EXPORT_FILE_BOTTOM_CONTENT, "FOOTER");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.setProperties(props);

            // Create a mock ResultSequence with 5 items
            ResultSequence seq = mock(ResultSequence.class);
            ResultItem item = mock(ResultItem.class);
            XdmItem xdmItem = mock(XdmItem.class);

            //Need one extra hasNext() because it's tested first in writeToFile(seq)
            when(seq.hasNext()).thenReturn(true, true, true, true, true, true,  false);
            when(seq.next()).thenReturn(item);
            when(item.getItem()).thenReturn(xdmItem);
            when(xdmItem.asString()).thenReturn("line1", "line2", "line3", "line4", "line5");
            instance.writeToFile(seq, instance.getExportFile());

            instance.writeBottomContent();
            // Verify that split files were created
            File outputFile = new File(tempDir, "test-bottom.txt");

            assertTrue(outputFile.exists());

            // Verify line counts
            assertEquals(6, FileUtils.getLineCount(outputFile));

            instance.moveFile();
            //move is no-op because no partExt
            assertTrue(outputFile.exists());

            instance.compressFile();
            File outputZipFile = new File(tempDir, "test-bottom.txt.zip");
            assertTrue(outputZipFile.exists());
            try (ZipFile zipFile = new ZipFile(outputZipFile)) {
                assertEquals(1, zipFile.size());
            }

        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    private File createSampleFile() throws IOException {
        return createSampleFile(".tmp");
    }

    private File createSampleFile(String extension) throws IOException {
        File file = File.createTempFile(TEMP_FILE_PREFIX, extension);
        file.deleteOnExit();
        try (Writer writer = new OutputStreamWriter(Files.newOutputStream(file.toPath()), StandardCharsets.UTF_8)) {
            writer.append(EXAMPLE_CONTENT);
        }
        return file;
    }

    private File createSamplePartFile() throws IOException {
        return createSampleFile(".part");
    }

    @Test
    public void insertIndexIntoFileName() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        assertEquals("C:\\test\\foo\\003_bar.baz", instance.insertIndexIntoFileName("C:\\test\\foo\\bar.baz", 3));
        assertEquals("/test/foo/003_bar.baz", instance.insertIndexIntoFileName("/test/foo/bar.baz", 3));
        assertEquals("/003_bar.baz", instance.insertIndexIntoFileName("/bar.baz", 3));
        assertEquals( "003_bar.baz", instance.insertIndexIntoFileName("bar.baz", 3));
    }

    public static class StringLengthComparator implements Comparator<String> , Serializable{
        @Override
        public int compare(String o1, String o2) {
            return Integer.compare(o1.length(), o2.length());
        }
    }
}
