/*
 * Copyright (c) 2004-2016 MarkLogic Corporation
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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PostBatchUpdateFileTaskTest {

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
    
    @Before
    public void setUp() {
        clearSystemProperties();
    }

    /**
     * Test of getBottomContent method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testGetBottomContent() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, BOTTOM_CONTENT);
        String result = instance.getBottomContent();
        assertEquals(BOTTOM_CONTENT, result);
    }

    @Test
    public void testGetBottomContent_nullBottom() {
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        String result = instance.getBottomContent();
        assertNull(result);
    }

    /**
     * Test of writeBottomContent method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testWriteBottomContent() throws Exception {
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
    }

    /**
     * Test of moveFile method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testMoveFile_String_String() throws Exception {
        File source = createSampleFile();
        String destFilePath = source.toString() + BAK_EXT;
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.moveFile(source.toString(), destFilePath);

        File dest = new File(destFilePath);
        assertFalse(source.exists());
        assertTrue(dest.exists());
        assertEquals(EXAMPLE_CONTENT, TestUtils.readFile(dest));
    }

    @Test
    public void testMoveFile_String_String_destExists() throws Exception {
        File source = createSampleFile();
        String destFilePath = source.toString() + BAK_EXT;
        File dest = new File(destFilePath);
        dest.deleteOnExit();
        dest.createNewFile();
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.moveFile(source.toString(), destFilePath);

        assertFalse(source.exists());
        assertTrue(dest.exists());
        assertEquals(EXAMPLE_CONTENT, TestUtils.readFile(dest));
    }

    /**
     * Test of moveFile method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testMoveFile_0args() throws Exception {
        File partFile = createSamplePartFile();
        String partFilePath = partFile.toString();
        String exportFilePath = partFilePath.substring(0, partFilePath.lastIndexOf("."));
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
    }

    /**
     * Test of compressFile method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testCompressFile() throws Exception {
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
    }

    @Test
    public void testCompressFile_outputFileDoesNotExist() throws Exception {
        File sampleFile = createSamplePartFile();
        sampleFile.delete();
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
    }

    @Test
    public void testCompressFile_zipPartExists() throws Exception {
        File sampleFile = createSamplePartFile();
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_AS_ZIP, Boolean.toString(true));
        props.setProperty(Options.EXPORT_FILE_NAME, sampleFile.toString());
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "zpart");
        String zipFilePart = sampleFile.toString().concat(ZIP_EXT).concat(PART_EXT);
        File existingZipFilePart = new File(zipFilePart);
        existingZipFilePart.createNewFile();

        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = props;
        instance.compressFile();

        File output = new File(sampleFile.toString().concat(ZIP_EXT));
        output.deleteOnExit();
        assertTrue(output.exists());
    }

    /**
     * Test of call method, of class PostBatchUpdateFileTask.
     */
    @Test
    public void testCall_removeDuplicatesAndSort_ascUniq() throws Exception {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "asc|uniq");
        String result = testRemoveDuplicatesAndSort(props);
        List<String> tokens = Arrays.asList(result.split("\n"));
        assertEquals(4, tokens.size());
        for (String next : new String[]{"a", "b", "d", "z"}) {
            assertTrue(tokens.contains(next));
        }
    }

    @Test
    public void testCall_removeDuplicatesAndSort_ASCENDING() throws Exception {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "ASCENDING|distinct");
        String result = testRemoveDuplicatesAndSort(props);
        assertEquals(splitAndAppendNewline("a,b,d,z"), result);
    }

    @Test
    public void testCall_removeDuplicatesAndSort_trueSort_withHeaderAndFooter()
            throws Exception {
        String header = "BEGIN\nletter\n";
        
        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file, true);
        writer.append(header);
        writer.append(Z);
        writer.append(D);
        writer.append(D);
        writer.append(A);
        writer.append(B);
        writer.flush();
        writer.close();

        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, header);
        props.setProperty(Options.EXPORT_FILE_HEADER_LINE_COUNT, "2");
        props.setProperty(Options.EXPORT_FILE_SORT, "ascending|distinct");
        props.setProperty(Options.EXPORT_FILE_BOTTOM_CONTENT, "END");
        String result = testRemoveDuplicatesAndSort(file, props);
        assertEquals(splitAndAppendNewline("BEGIN,letter,a,b,d,z,END"), result);
    }

    @Test
    public void testCall_removeDuplicatesAndSort_customComparator() throws Exception {
        assertTrue(testCustomComparator(null, "b,z...,d....,d....,a....."));
    }

    @Test
    public void testCall_removeDuplicatesAndSort_customComparator_distinct() throws Exception {
        assertTrue(testCustomComparator("distinct", "b,z...,d....,a....."));
    }

    @Test
    public void testCall_removeDuplicatesAndSort_customComparator_badClass() throws Exception {
        assertTrue(testCustomComparator("distinct", "z...,d....,d....,a.....,b", "java.lang.String"));
    }

    @Test
    public void testCall_removeDuplicatesAndSort_noSortOrDedup_distinctOnly() throws Exception {
        assertTrue(testCustomComparator("distinct", "z...,d....,d....,a.....,b", null));
    }

    @Test
    public void testCall_removeDuplicatesAndSort_noSortOrDedup_blankSort() throws Exception {
        assertTrue(testCustomComparator(" ", "z...,d....,d....,a.....,b", null));
    }

    public boolean testCustomComparator(String sortProperty, String expected) throws Exception {
        return testCustomComparator(sortProperty, expected, "com.marklogic.developer.corb.PostBatchUpdateFileTaskTest$StringLengthComparator");
    }

    public boolean testCustomComparator(String sortProperty, String expected, String comparator) throws Exception {
        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file, true);
        writer.append("z...\n");
        writer.append("d....\n");
        writer.append("d....\n");
        writer.append("a.....\n");
        writer.append(B);
        writer.flush();
        writer.close();

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
    public void testCall_removeDuplicatesAndSort_descendingDistinct() throws Exception {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, "desc|distinct");
        String result = testRemoveDuplicatesAndSort(props);
        assertEquals(splitAndAppendNewline("z,d,b,a"), result);
    }

    @Test
    public void testCall_removeDuplicatesAndSort_invalidValue() throws Exception {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, Boolean.toString(false));
        String result = testRemoveDuplicatesAndSort(props);
        assertEquals(splitAndAppendNewline("z,d,d,a,b"), result);
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
    public void testCall_removeDuplicatesAndSort_emptyFile_extensionWithoutDot() throws Exception {
        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        file.createNewFile();
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, Boolean.toString(true));
        props.setProperty(Options.EXPORT_FILE_NAME, file.toString());
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "pt");
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = props;
        instance.call();
        assertTrue(file.length() == 0);
    }

    @Test
    public void testCall_removeDuplicatesAndSort_emptyFile_emptyExtension() throws Exception {
        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        file.createNewFile();
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_SORT, Boolean.TRUE.toString());
        props.setProperty(Options.EXPORT_FILE_NAME, file.toString());
        props.setProperty(Options.EXPORT_FILE_PART_EXT, "");
        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.properties = props;
        instance.call();
        assertTrue(file.length() == 0);
    }

    private String testRemoveDuplicatesAndSort(Properties props) throws IOException, Exception {

        File file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        file.deleteOnExit();
        FileWriter writer = new FileWriter(file, true);
        writer.append(Z);
        writer.append(D);
        writer.append(D);
        writer.append(A);
        writer.append(B);
        writer.close();
        return testRemoveDuplicatesAndSort(file, props);
    }

    private String testRemoveDuplicatesAndSort(File fileToSort, Properties props)
            throws IOException, Exception {

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
    public void testCall() throws Exception {

        PostBatchUpdateFileTask instance = new PostBatchUpdateFileTask();
        instance.call();
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
        FileWriter writer = new FileWriter(file, true);
        writer.append(EXAMPLE_CONTENT);
        writer.close();
        return file;
    }

    private File createSamplePartFile() throws IOException {
        return createSampleFile(".part");
    }

    public static class StringLengthComparator implements Comparator<String> {

        @Override
        public int compare(String o1, String o2) {
            if (o1.length() > o2.length()) {
                return 1;
            } else if (o1.length() < o2.length()) {
                return -1;
            } else {
                return 0;
            }
        }
    }
}
