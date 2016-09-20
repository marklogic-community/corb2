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

import com.marklogic.xcc.Request;
import com.marklogic.xcc.exceptions.RequestPermissionException;
import java.io.File;
import java.util.Properties;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Mads Hansen, MarkLogic Corporation
 */
public class PreBatchUpdateFileTaskTest {

    /**
     * Test of getTopContent method, of class PreBatchUpdateFileTask.
     */
    @Test
    public void testGetTopContent() {
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, "foo@" + Manager.URIS_BATCH_REF + "baz");
        props.setProperty(Manager.URIS_BATCH_REF, "bar");
        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.properties = props;
        String result = instance.getTopContent();
        assertEquals("foobarbaz", result);
    }

    @Test
    public void testGetTopContentIsNull() {
        Properties props = new Properties();
        props.setProperty(Manager.URIS_BATCH_REF, "bar");
        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.properties = props;
        String result = instance.getTopContent();
        assertNull(result);
    }

    @Test
    public void testGetTopContentUrisBatchRefIsNull() {
        Properties props = new Properties();
        String val = "foo@" + Manager.URIS_BATCH_REF + "baz";
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, val);
        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.properties = props;
        String result = instance.getTopContent();
        assertEquals(val, result);
    }

    /**
     * Test of writeTopContent method, of class PreBatchUpdateFileTask.
     */
    @Test
    public void testWriteTopContent() throws Exception {
        String content = "foo,bar,baz";
        File tempDir = TestUtils.createTempDirectory();
        File tempFile = new File(tempDir, "topContent");
        tempFile.createNewFile();
        tempFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, content);
        props.setProperty(Options.EXPORT_FILE_NAME, tempFile.getName());

        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.properties = props;
        instance.exportDir = tempDir.toString();
        instance.writeTopContent();
        File partFile = new File(tempDir, instance.getPartFileName());
        assertEquals(content.concat(new String(PreBatchUpdateFileTask.NEWLINE)), TestUtils.readFile(partFile));
    }

    /**
     * Test of call method, of class PreBatchUpdateFileTask.
     */
    @Test(expected = NullPointerException.class)
    public void testCallNpe() throws Exception {
        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.call();
        fail();
    }

    @Test
    public void testCall() throws Exception {
        String content = "foo,bar,baz";
        File tempDir = TestUtils.createTempDirectory();
        File tempFile = new File(tempDir, "topContent");
        tempFile.createNewFile();
        tempFile.deleteOnExit();
        Properties props = new Properties();
        props.setProperty(Options.EXPORT_FILE_TOP_CONTENT, content);
        props.setProperty(Options.EXPORT_FILE_NAME, tempFile.getName());

        PreBatchUpdateFileTask instance = new PreBatchUpdateFileTask();
        instance.properties = props;
        instance.exportDir = tempDir.toString();
        File partFile = new File(tempDir, instance.getPartFileName());
        instance.call();

        assertEquals(content.concat(new String(PreBatchUpdateFileTask.NEWLINE)), TestUtils.readFile(partFile));
    }
    
    @Test
    public void testHasRetryableMessage() {
        Request req = mock(Request.class);
        AbstractTask instance = new PreBatchUpdateFileTask();
        instance.properties = new Properties();
        instance.properties.setProperty(Options.QUERY_RETRY_ERROR_MESSAGE, "FOO,Authentication failure for user,BAR");
        RequestPermissionException exception = new RequestPermissionException(AbstractTaskTest.REJECTED_MSG, req, AbstractTaskTest.USER_NAME, false);
        assertFalse(instance.hasRetryableMessage(exception));

        exception = new RequestPermissionException("Authentication failure for user 'user-name'", req, AbstractTaskTest.USER_NAME, false);
        assertTrue(instance.hasRetryableMessage(exception));
    }
}
