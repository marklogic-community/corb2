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

import java.nio.file.Files;
import java.util.Properties;

import com.marklogic.developer.corb.util.FileUtils;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.XdmItem;
import org.junit.Before;
import org.junit.Test;

import static com.marklogic.developer.corb.Options.EXPORT_FILE_NAME;
import static com.marklogic.developer.corb.TestUtils.*;
import static org.junit.Assert.*;

import com.marklogic.xcc.Request;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
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
public class PostBatchUpdateFileTaskTest {

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

    @Before
    public void setUp() {
        clearSystemProperties();
    }

    @Test
    public void testGetBottomContent() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
        String result = instance.getBottomContent();
        assertEquals(BOTTOM_CONTENT, result);
    }

    @Test
    public void testGetBottomContentNullBottom() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        String result = instance.getBottomContent();
        assertNull(result);
    }

    @Test
    public void testWriteBottomContent() {
        try {
            String expectedResult = BOTTOM_CONTENT.concat("\n");
            String filename = "export.csv";
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = new Properties();
            instance.properties.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
            instance.properties.setProperty(Options.EXPORT_FILE_NAME, filename);
            instance.properties.setProperty(Options.EXPORT_FILE_PART_EXT, ".temp");
            File tempDir = createTempDirectory();
            instance.exportDir = tempDir.toString();

            instance.writeBottomContent();

            File outputFile = new File(tempDir, "export.csv.temp");
            String outputText = TestUtils.readFile(outputFile);
            assertEqualsNormalizeNewline(expectedResult, outputText);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testMoveFileStringString() {
        try {
            File source = createSampleFile();
            String destFilePath = source.toString() + BAK_EXT;
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.moveFile(source.toString(), destFilePath);

            File dest = new File(destFilePath);
            assertFalse(source.exists());
            assertTrue(dest.exists());
            assertEquals(EXAMPLE_CONTENT, TestUtils.readFile(dest));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testMoveFileStringStringDestExists() {
        try {
            File source = createSampleFile();
            String destFilePath = source.toString() + BAK_EXT;
            File dest = new File(destFilePath);
            if (dest.createNewFile()) {
                dest.deleteOnExit();
                PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
                instance.moveFile(source.toString(), destFilePath);

                assertFalse(source.exists());
                assertTrue(dest.exists());
                assertEquals(EXAMPLE_CONTENT, TestUtils.readFile(dest));
            } else {
                fail();
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testMoveFileZeroargs() {
        try {
            File partFile = createSamplePartFile();
            String partFilePath = partFile.toString();
            String exportFilePath = partFilePath.substring(0, partFilePath.lastIndexOf('.'));
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_PART_EXT, PART_EXT);
            props.setProperty(Options.EXPORT_FILE_NAME, exportFilePath);
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.moveFile();

            File dest = new File(exportFilePath);
            assertTrue(dest.exists());
            assertFalse(partFile.exists());
            assertEquals(EXAMPLE_CONTENT, TestUtils.readFile(dest));
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCompressFile() {
        try {
            File sampleFile = createSamplePartFile();
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
            props.setProperty(Options.EXPORT_FILE_NAME, sampleFile.toString());
            props.setProperty(Options.EXPORT_FILE_PART_EXT, PART_FILE_EXT);

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
    public void testCompressFileOutputFileDoesNotExist() {
        try {
            File sampleFile = createSamplePartFile();
            if (sampleFile.delete()) {
                Properties props = new Properties();
                props.setProperty(Options.EXPORT_FILE_AS_ZIP, Boolean.TRUE.toString());
                props.setProperty(Options.EXPORT_FILE_NAME, sampleFile.toString());
                props.setProperty(Options.EXPORT_FILE_PART_EXT, PART_FILE_EXT);

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
    public void testCompressFileZipPartExists() {
        try {
            File sampleFile = createSamplePartFile();
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_AS_ZIP, Boolean.toString(true));
            props.setProperty(Options.EXPORT_FILE_NAME, sampleFile.toString());
            props.setProperty(Options.EXPORT_FILE_PART_EXT, "zpart");
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
    public void testCallRemoveDuplicatesAndSortAscUniq() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "asc|uniq");
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
    public void testCallRemoveDuplicatesAndSortASCENDING() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "ASCENDING|distinct");
        try {
            String result = testRemoveDuplicatesAndSort(props);
            assertEqualsNormalizeNewline(splitAndAppendNewline("a,b,d,z"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCallRemoveDuplicatesAndSortTrueSortWithHeaderAndFooter() {
        String header = "BEGIN\nletter\n";
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            try (FileWriter writer = new FileWriter(file, true)) {
                writer.append(header);
                writer.append(Z);
                writer.append(D);
                writer.append(D);
                writer.append(A);
                writer.append(B);
                writer.flush();
            }
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, header);
            props.setProperty(Options.EXPORT_FILE_HEADER_LINE_COUNT, "2");
            props.setProperty(Options.EXPORT_FILE_SORT, "ascending|distinct");
            props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "END");
            String result = testRemoveDuplicatesAndSort(file, props);
            assertEqualsNormalizeNewline(splitAndAppendNewline("BEGIN,letter,a,b,d,z,END"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparator() throws Exception {
        testCustomComparator(null, "b,z...,d....,d....,a.....");
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparatorDistinct() throws Exception {
        testCustomComparator(DISTINCT, "b,z...,d....,a.....");
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparatorBadClass() throws Exception {
        testCustomComparator(DISTINCT, ZDDAB, "java.lang.String");
    }

    @Test
    public void testCallRemoveDuplicatesAndSortNoSortOrDedupDistinctOnly() throws Exception {
        testCustomComparator(DISTINCT, ZDDAB, null);
    }

    @Test
    public void testCallRemoveDuplicatesAndSortNoSortOrDedupBlankSort() throws Exception {
        testCustomComparator(" ", ZDDAB, null);
    }

    public void testCustomComparator(String sortProperty, String expected) throws Exception {
        testCustomComparator(sortProperty, expected, "com.marklogic.developer.corb.PostBatchUpdateFileTaskTest$StringLengthComparator");
    }

    public void testCustomComparator(String sortProperty, String expected, String comparator) throws Exception {

        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file, true)) {
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
            props.setProperty(Options.EXPORT_FILE_SORT_COMPARATOR, comparator);
        }
        if (sortProperty != null) {
            props.setProperty(Options.EXPORT_FILE_SORT, sortProperty);
        }
        String result = testRemoveDuplicatesAndSort(file, props);
        assertEqualsNormalizeNewline(splitAndAppendNewline(expected), result);
    }

    @Test
    public void testCallRemoveDuplicatesAndSortDescendingDistinct() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "desc|distinct");
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
    public void testCallRemoveDuplicatesAndSortInvalidValue() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, Boolean.toString(false));
        String result;
        try {
            result = testRemoveDuplicatesAndSort(props);
            assertEquals(splitAndAppendNewline("z,d,d,a,b"), result);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private String splitAndAppendNewline(String values) {
        StringBuilder sb = new StringBuilder();

        for (String value : values.split(",")) {
            sb.append(value)
                    .append('\n');
        }
        return sb.toString();
    }

    @Test
    public void testCallRemoveDuplicatesAndSortEmptyFileExtensionWithoutDot() {
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_SORT, Boolean.toString(true));
            props.setProperty(Options.EXPORT_FILE_NAME, file.toString());
            props.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
            props.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.call();
            assertTrue(file.length() == 0);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCallRemoveDuplicatesAndSortEmptyFileEmptyExtension() {
        try {
            File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            file.deleteOnExit();
            Properties props = new Properties();
            props.setProperty(Options.EXPORT_FILE_SORT, Boolean.TRUE.toString());
            props.setProperty(Options.EXPORT_FILE_NAME, file.toString());
            props.setProperty(Options.EXPORT_FILE_PART_EXT, "");
            props.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = props;
            instance.call();
            assertTrue(file.length() == 0);
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    private String testRemoveDuplicatesAndSort(Properties props) throws Exception {

        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.append(Z);
            writer.append(D);
            writer.append(D);
            writer.append(A);
            writer.append(B);
        }
        return testRemoveDuplicatesAndSort(file, props);
    }

    private String testRemoveDuplicatesAndSort(File fileToSort, Properties props)
            throws Exception {

        props.setProperty(Options.EXPORT_FILE_AS_ZIP, Boolean.FALSE.toString());
        props.setProperty(Options.EXPORT_FILE_NAME, fileToSort.toString());
        props.setProperty(Options.EXPORT_FILE_REQUIRE_PROCESS_MODULE, "false");
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = props;
        instance.call();
        File output = new File(fileToSort.toString());
        output.deleteOnExit();
        return TestUtils.readFile(output);
    }

    @Test(expected = NullPointerException.class)
    public void testCall() {

        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        try {
            instance.call();
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            if (ex instanceof NullPointerException) {
                throw (NullPointerException) ex;
            }
        }
        fail();
    }
    @Test
    public void testGetMaxLines() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "100");
        instance.setProperties(props);
        assertEquals(100, instance.getMaxLines());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());

        // Test with invalid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "not-a-number");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxLines());
    }
    @Test
    public void testInsertIndexIntoFileName() {
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
    public void testGetMaxSize() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        Properties props = new Properties();

        // Test with valid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "1024");
        instance.setProperties(props);
        assertEquals(1024, instance.getMaxSize());

        // Test with no value
        props.clear();
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());

        // Test with invalid value
        props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_SIZE, "invalid");
        instance.setProperties(props);
        assertEquals(-1, instance.getMaxSize());
    }

    @Test
    public void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask instance = new PostBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException(AbstractTaskTest.REJECTED_MSG, req, AbstractTaskTest.USER_NAME, false);
        assertFalse(instance.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, AbstractTaskTest.USER_NAME, false);
        assertTrue(instance.hasRetryableMessage(exception));
    }

    @Test
    public void testWriteBottomContentWithSplitByLinesAndZip() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-split.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(Options.EXPORT_FILE_SPLIT_MAX_LINES, "2");
            props.setProperty(Options.EXPORT_FILE_PART_EXT, ".tmp");
            props.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
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

            assertTrue("First file should exist", file1.exists());
            assertTrue("Second file should exist", file2.exists());
            assertTrue("Third file should exist", file3.exists());

            // Verify line counts
            assertEquals("First file should have 2 lines", 2, FileUtils.getLineCount(file1));
            assertEquals("Second file should have 2 lines", 2, FileUtils.getLineCount(file2));
            assertEquals("Third file should have 1 line", 1, FileUtils.getLineCount(file3));

            //Now that the files are written, rename temp filenames to final
            instance.moveFile();
            // Verify that split files were created
            file1 = new File(tempDir, "001_test-split.txt");
            file2 = new File(tempDir, "002_test-split.txt");
            file3 = new File(tempDir, "003_test-split.txt");

            assertTrue("First file should exist", file1.exists());
            assertTrue("Second file should exist", file2.exists());
            assertTrue("Third file should exist", file3.exists());

            // Verify line counts
            assertEquals("First file should have 2 lines", 2, FileUtils.getLineCount(file1));
            assertEquals("Second file should have 2 lines", 2, FileUtils.getLineCount(file2));
            assertEquals("Third file should have 1 line", 1, FileUtils.getLineCount(file3));

            instance.compressFile();
            File outputFile = new File(tempDir, "test-split.txt.zip");
            assertTrue("files have been zipped", outputFile.exists());
            try (ZipFile zipFile = new ZipFile(outputFile)) {
                assertEquals("there are 3 entries in the zip", 3, zipFile.size());
            }
            assertFalse("First file should not exist", file1.exists());
            assertFalse("Second file should not exist", file2.exists());
            assertFalse("Third file should not exist", file3.exists());

        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Test failed", ex);
            fail("Exception occurred: " + ex.getMessage());
        } finally {
            if (tempDir != null) {
                FileUtils.deleteQuietly(tempDir.toPath());
            }
        }
    }

    @Test
    public void testWriteBottomContentForBatchAndZip() {
        Properties props = new Properties();
        File tempDir = null;
        try {
            tempDir = Files.createTempDirectory("corb-test").toFile();
            File batchFile = new File(tempDir, "test-bottom.txt");
            props.setProperty(EXPORT_FILE_NAME, batchFile.getAbsolutePath());
            props.setProperty(Options.EXPORT_FILE_AS_ZIP, "true");
            props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "FOOTER");
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

            assertTrue("Output file should exist, and no partExt", outputFile.exists());

            // Verify line counts
            assertEquals("Output file should have 6 lines", 6, FileUtils.getLineCount(outputFile));

            instance.moveFile();
            //move is no-op because no partExt
            assertTrue("Output file still exists", outputFile.exists());

            instance.compressFile();
            File outputZipFile = new File(tempDir, "test-bottom.txt.zip");
            assertTrue("files have been zipped", outputZipFile.exists());
            try (ZipFile zipFile = new ZipFile(outputZipFile)) {
                assertEquals("there is 1 entry in the zip", 1, zipFile.size());
            }

        } catch (Exception ex) {
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
        try (FileWriter writer = new FileWriter(file, true)) {
            writer.append(EXAMPLE_CONTENT);
        }
        return file;
    }

    private File createSamplePartFile() throws IOException {
        return createSampleFile(".part");
    }

    public static class StringLengthComparator implements Comparator<String> , Serializable{
        @Override
        public int compare(String o1, String o2) {
            return Integer.compare(o1.length(), o2.length());
        }
    }
}
