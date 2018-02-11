/*
 * Copyright (c) 2004-2018 MarkLogic Corporation
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

import java.util.Properties;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;
import static com.marklogic.developer.corb.TestUtils.clearSystemProperties;
import static com.marklogic.developer.corb.TestUtils.createTempDirectory;
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
import static org.mockito.Mockito.mock;

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
            String expextedResult = BOTTOM_CONTENT.concat("\n");
            String filename = "export.csv";
            PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
            instance.properties = new Properties();
            instance.properties.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
            instance.properties.setProperty(Options.EXPORT_FILE_NAME, filename);
            File tempDir = createTempDirectory();
            instance.exportDir = tempDir.toString();

            instance.writeBottomContent();

            File outputFile = new File(tempDir, instance.getPartFileName());
            String outputText = TestUtils.readFile(outputFile);
            assertEquals(expextedResult, outputText);
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
            List<String> tokens = Arrays.asList(result.split("\n"));
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
            assertEquals(splitAndAppendNewline("a,b,d,z"), result);
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

                Properties props = new Properties();
                props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, header);
                props.setProperty(Options.EXPORT_FILE_HEADER_LINE_COUNT, "2");
                props.setProperty(Options.EXPORT_FILE_SORT, "ascending|distinct");
                props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "END");
                String result = testRemoveDuplicatesAndSort(file, props);
                assertEquals(splitAndAppendNewline("BEGIN,letter,a,b,d,z,END"), result);
            }
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, null, ex);
            fail();
        }
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparator() throws Exception {
        assertTrue(testCustomComparator(null, "b,z...,d....,d....,a....."));
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparatorDistinct() throws Exception {
        assertTrue(testCustomComparator(DISTINCT, "b,z...,d....,a....."));
    }

    @Test
    public void testCallRemoveDuplicatesAndSortCustomComparatorBadClass() throws Exception {
        assertTrue(testCustomComparator(DISTINCT, ZDDAB, "java.lang.String"));
    }

    @Test
    public void testCallRemoveDuplicatesAndSortNoSortOrDedupDistinctOnly() throws Exception {
        assertTrue(testCustomComparator(DISTINCT, ZDDAB, null));
    }

    @Test
    public void testCallRemoveDuplicatesAndSortNoSortOrDedupBlankSort() throws Exception {
        assertTrue(testCustomComparator(" ", ZDDAB, null));
    }

    public boolean testCustomComparator(String sortProperty, String expected) throws Exception {
        return testCustomComparator(sortProperty, expected, "com.marklogic.developer.corb.PostBatchUpdateFileTaskTest$StringLengthComparator");
    }

    public boolean testCustomComparator(String sortProperty, String expected, String comparator) throws Exception {

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
        return result.equals(splitAndAppendNewline(expected));
    }

    @Test
    public void testCallRemoveDuplicatesAndSortDescendingDistinct() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "desc|distinct");
        String result;
        try {
            result = testRemoveDuplicatesAndSort(props);
            assertEquals(splitAndAppendNewline("z,d,b,a"), result);
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
